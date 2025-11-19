"""
Shortest Path Computation using Pure PySpark

This module implements shortest path computation using only Spark SQL operations.
This pure Spark approach is optimal for very large partitions and GPU acceleration.

Advantages:
- Works well for large partitions (> 100k rows)
- Better GPU acceleration support
- Native Spark ecosystem compatibility

Author: [Your Name]
Date: 2025
"""

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from utilities import (
    initialize_spark,
    read_edges,
    initial_shortcuts_table,
    update_dummy_costs_for_edges,
    add_info_for_shortcuts,
    filter_shortcuts_by_resolution,
    add_parent_cell_at_resolution,
    merge_shortcuts_to_main_table
)


# ============================================================================
# PURE SPARK SHORTEST PATH COMPUTATION
# ============================================================================
def has_converged(current_paths, next_paths):
    """
    Check if shortest path computation has converged.
    Returns: True if NO CHANGES found (converged)
    """
    join_keys = ["incoming_edge", "outgoing_edge", "cost", "current_cell"]
    
    # Find rows in next_paths that DON'T exist in current_paths
    # If empty, then next_paths == current_paths (converged!)
    changes = next_paths.join(
        current_paths.select(join_keys),
        on=join_keys,
        how="left_anti"
    )
    
    # True if NO changes (converged)
    return changes.limit(1).count() == 0

def update_convergence_status(
    shortcuts_df_last: DataFrame,
    shortcuts_df_new: DataFrame
) -> DataFrame:
    """
    Mark spatial partitions as converged if their contents haven't changed.
    
    A partition is marked as converged (True) if none of its associated rows
    (based on join keys) have changed between the last and new iterations.
    Non-converged partitions are marked as False, indicating they need
    further processing.
    
    Args:
        shortcuts_df_last: Paths from previous iteration
        shortcuts_df_new: Paths from current iteration
    
    Returns:
        DataFrame with updated 'is_converged' column
    """
    
    # Join keys define when a shortcut is considered "changed"
    join_keys = ["incoming_edge", "outgoing_edge", "cost"]
    
    # --- Find all ACTIVE (Non-Converged) current_cell IDs ---
    # Use LEFT ANTI-JOIN to find rows in 'new' that are new or changed
    non_converged_rows = shortcuts_df_new.join(
        shortcuts_df_last,
        on=join_keys,
        how="left_anti"
    )
    
    # Extract unique cell IDs that have changes
    active_cell_ids = non_converged_rows.select("current_cell").distinct()
    
    # --- Flag convergence status ---
    result_df = shortcuts_df_new.alias("new")
    
    # Join with active cell IDs to mark which cells still need processing
    result_df = result_df.join(
        active_cell_ids.alias("active"),
        on="current_cell",
        how="left_outer"
    )
    
    # Convergence: cells NOT in active list are converged
    final_df = result_df.withColumn(
        "is_converged",
        F.when(
            F.col("active.current_cell").isNull(),
            F.lit(True)
        ).otherwise(
            F.lit(False)
        )
    ).drop("active.current_cell")
    
    return final_df


def run_grouped_shortest_path_with_convergence(
    shortcuts_df: DataFrame,
    max_iterations: int = 10
) -> DataFrame:
    """
    Compute all-pairs shortest paths using pure Spark SQL operations.
    
    Uses self-joins and window functions to iteratively extend paths until
    convergence. All operations are performed in Spark, with no data leaving
    the cluster.
    
    Algorithm:
    1. Start with direct edges as paths, mark all as non-converged
    2. For each iteration:
       a. Join paths: if A->B and B->C exist (same cell), create A->C
       b. Filter to keep only minimum cost paths per (source, destination, cell)
       c. Check convergence: mark cells with no changes as converged
       d. Continue iteration with only non-converged cells
    3. Stop when all cells are converged or max iterations reached
    
    Args:
        shortcuts_df: Input shortcuts DataFrame with columns:
                      incoming_edge, outgoing_edge, cost, via_edge, current_cell
        max_iterations: Maximum iterations before stopping
    
    Returns:
        DataFrame with computed shortest paths (without convergence flags)
    """
    
    # Initialize: all paths start as non-converged
    current_paths = shortcuts_df.select(
        "incoming_edge", "outgoing_edge", "cost", "via_edge", "current_cell"
    ).withColumn(
        "is_converged", F.lit(False)
    ).cache()
    
    #for iteration in range(max_iterations):
    iteration = 0
    while(True):
        print(f"\n--- Iteration {iteration} ---")
        iteration+=1
        # --- PATH EXTENSION ---
        # Find new two-hop paths by joining on connection points
        new_paths = current_paths.alias("L").join(
            current_paths.alias("R"),
            # Join conditions:
            # 1. Connection: L's outgoing edge == R's incoming edge
            # 2. Same partition: both in same H3 cell
            [
                F.col("L.outgoing_edge") == F.col("R.incoming_edge"),
                F.col("L.current_cell") == F.col("R.current_cell")
            ],
            "inner"
        ).filter(
            # Filters:
            # 1. No self-loops for entire path
            # 2. Only extend from non-converged paths
            (F.col("L.incoming_edge") != F.col("R.outgoing_edge")) &
            (F.col("L.is_converged") == F.lit(False)) &
            (F.col("R.is_converged") == F.lit(False))
        ).select(
            F.col("L.incoming_edge").alias("incoming_edge"),
            F.col("R.outgoing_edge").alias("outgoing_edge"),
            (F.col("L.cost") + F.col("R.cost")).alias("cost"),
            F.col("L.outgoing_edge").alias("via_edge"),
            F.col("L.current_cell").alias("current_cell"),
            F.lit(False).alias("is_converged")
        ).cache()
        
        # Check if any new paths were found
        if new_paths.limit(1).count() == 0:
            print(f"✓ No new paths found. Converged.")
            break
        
        # --- COST MINIMIZATION ---
        # Combine existing and new paths
        all_paths = current_paths.unionByName(new_paths)
        
        # Use window function to rank paths by cost
        window_spec = Window.partitionBy(
            "incoming_edge",
            "outgoing_edge",
            "current_cell"
        ).orderBy(
            F.col("cost").asc(),
            F.col("via_edge").asc()  # Tie-breaker
        )
        
        # Keep only the minimum cost path for each (source, destination, cell)
        next_paths = all_paths.withColumn(
            "rnk",
            F.row_number().over(window_spec)
        ).filter(
            F.col("rnk") == 1
        ).drop("rnk").cache()
        
        # Clean up temporary DataFrames
        current_paths.unpersist()
        new_paths.unpersist()
        
        # --- CONVERGENCE CHECK ---
        # Mark cells as converged if their contents haven't changed
        #next_paths = update_convergence_status(current_paths, next_paths)
        
        # Check if all rows are converged
        #all_converged = next_paths.select(
        #    F.max(F.col("is_converged").cast("int")).alias("max_converged")
        #).collect()[0]["max_converged"] == 1
        
        #if all_converged:
        #    print(f"✓ Iteration {iteration}: All paths fully converged!")
        #    break
        # ✅ Simple, correct, efficient
        if has_converged(current_paths, next_paths):
            print(f"✓ Iteration {iteration}: Converged!")
            break
        current_paths = next_paths.checkpoint()
        print(f"✓ Iteration {iteration}: Processed and updated shortest paths")
            
    # Remove temporary columns and return result
    return current_paths.drop("is_converged", "current_cell")


# ============================================================================
# MAIN EXECUTION FOR PURE SPARK VERSION
# ============================================================================

def main(
    edges_file: str,
    graph_file: str,
    resolution_range: range = range(14, 8, -1),
    max_iterations: int = 10
):
    """
    Main execution function for pure Spark shortest path computation.
    
    Args:
        edges_file: Path to edges CSV file
        graph_file: Path to edge graph CSV file
        resolution_range: Range of H3 resolutions to process
        max_iterations: Maximum iterations per partition
    """
    # Initialize
    spark = initialize_spark(app_name="ShortestPathSpark")
    
    try:
        # Load and prepare data
        print("Loading edge data...")
        edges_df = read_edges(spark, edges_file).cache()
        
        print("Computing edge costs...")
        edges_cost_df = update_dummy_costs_for_edges(spark, edges_file, edges_df)
        
        print("Creating initial shortcuts table...")
        shortcuts_df = initial_shortcuts_table(spark, graph_file, edges_cost_df)
        
        # Process each resolution level
        for current_resolution in resolution_range:
            print(f"\n{'='*60}")
            print(f"Processing resolution {current_resolution}")
            print(f"{'='*60}")
            
            # Enrich with spatial info
            shortcuts_df = add_info_for_shortcuts(spark, shortcuts_df, edges_df)
            shortcuts_df = shortcuts_df.checkpoint().cache()
            
            # Filter by resolution
            shortcuts_df_filtered = filter_shortcuts_by_resolution(
                shortcuts_df,
                current_resolution
            )
            
            # Add spatial partition key
            shortcuts_df_with_cell = add_parent_cell_at_resolution(
                shortcuts_df_filtered,
                current_resolution
            )
            
            # Compute shortest paths using pure Spark
            print(f"Computing shortest paths using pure Spark SQL...")
            shortcuts_df_new = run_grouped_shortest_path_with_convergence(
                shortcuts_df_with_cell,
                max_iterations=max_iterations
            )
            
            count = shortcuts_df_new.count()
            print(f"✓ Generated {count} shortcuts at resolution {current_resolution}")
            
            # Merge back to main table or just save for each resolution
            #shortcuts_df = merge_shortcuts_to_main_table(shortcuts_df, shortcuts_df_new)
            shortcuts_df = shortcuts_df_new
        
        print("\n" + "="*60)
        print("Shortest path computation (Pure Spark version) completed!")
        print("="*60)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main(
        edges_file="data/burnaby_driving_simplified_edges_with_h3.csv",
        graph_file="data/burnaby_driving_edge_graph.csv",
        resolution_range=range(15, 8, -1),
        max_iterations=10
    )