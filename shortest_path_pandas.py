"""
Shortest Path Computation using PySpark (with Pandas acceleration)

This module implements shortest path computation using Spark for data distribution
and Pandas for efficient inner-partition computation. This hybrid approach combines
the scalability of Spark with the performance of vectorized Pandas operations.

Advantages:
- Faster for small-to-medium sized partitions (< 100k rows)
- Better for complex path computations
- Leverages Pandas C-optimized operations

Author: [Your Name]
Date: 2025
"""

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
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
# PANDAS-BASED SHORTEST PATH COMPUTATION
# ============================================================================

def converging_check(old_paths: pd.DataFrame, new_paths: pd.DataFrame) -> bool:
    """
    Check if path set has converged by comparing old and new paths.
    
    Uses pandas merge to identify new or changed paths. Convergence occurs
    when no new paths are found.
    
    Args:
        old_paths: Previous iteration's paths
        new_paths: Current iteration's paths
    
    Returns:
        True if converged (no changes), False otherwise
    """
    # Perform left merge to find rows in new_paths that don't exist in old_paths
    merged_df = pd.merge(
        new_paths,
        old_paths,
        on=["incoming_edge", "outgoing_edge", "cost"],
        how='left',
        indicator=True
    )
    
    # Check if any new rows exist
    new_rows = merged_df[merged_df['_merge'] == 'left_only']
    return len(new_rows) == 0


def compute_shortest_paths_per_partition(
    df_shortcuts: DataFrame,
    partition_columns: list,
) -> DataFrame:
    """
    Compute all-pairs shortest paths using vectorized pandas operations.
    
    Uses Spark's applyInPandas to process each partition with optimized pandas code.
    This approach is significantly faster than pure Spark SQL for small groups
    because it avoids shuffle operations and uses pandas' C-optimized operations.
    
    Algorithm:
    1. Start with direct edges as initial paths
    2. Iteratively extend paths: if A->B and B->C exist, create A->C
    3. Keep minimum cost for each (source, destination) pair
    4. Stop when no new paths are found or cost improvements made
    
    Args:
        df_shortcuts: DataFrame with schema (incoming_edge, outgoing_edge, cost, via_edge)
        partition_columns: List of column names to group by (e.g., ["current_cell"])
    
    Returns:
        DataFrame with computed shortest paths
    """
    
    # Define output schema
    output_schema = StructType([
        StructField("incoming_edge", StringType(), False),
        StructField("outgoing_edge", StringType(), False),
        StructField("via_edge", StringType(), False),
        StructField("cost", DoubleType(), False),
    ])
    
    def process_partition_pandas(pdf: pd.DataFrame) -> pd.DataFrame:
        """
        Process a single partition using vectorized pandas operations.
        
        This function executes on each Spark executor for its partition's data.
        All operations are vectorized for maximum performance.
        
        Args:
            pdf: Pandas DataFrame for a single partition
        
        Returns:
            Pandas DataFrame with computed shortest paths
        """
        
        if len(pdf) == 0:
            return pd.DataFrame(columns=['incoming_edge', 'outgoing_edge', 'via_edge', 'cost'])
        
        # Initialize paths from input edges (direct connections)
        paths = pdf[['incoming_edge', 'outgoing_edge', 'via_edge', 'cost']].copy()
        
        # Remove duplicates, keeping minimum cost for each (source, destination) pair
        paths = paths.loc[
            paths.groupby(['incoming_edge', 'outgoing_edge'])['cost'].idxmin()
        ].reset_index(drop=True)
        
        iteration = 0
        max_iterations = 100  # Safety limit
        
        # Iteratively extend paths until convergence
        while iteration < max_iterations:
            # Vectorized merge: if A->B and B->C exist, create A->C
            # This is the core of the Floyd-Warshall algorithm
            new_paths = paths.merge(
                paths,
                left_on='outgoing_edge',
                right_on='incoming_edge',
                suffixes=('_L', '_R'),
                how='inner'
            )
            
            if len(new_paths) == 0:
                break  # No new paths to add
            
            # Extract and rename columns for the new paths
            new_paths = new_paths[[
                'incoming_edge_L',
                'outgoing_edge_R',
                'cost_L',
                'cost_R',
                'outgoing_edge_L'
            ]].copy()
            
            # Calculate total cost
            new_paths['cost'] = new_paths['cost_L'] + new_paths['cost_R']
            
            # Rename columns to standard format
            new_paths = new_paths.rename(columns={
                'incoming_edge_L': 'incoming_edge',
                'outgoing_edge_R': 'outgoing_edge',
                'outgoing_edge_L': 'via_edge'
            })[['incoming_edge', 'outgoing_edge', 'via_edge', 'cost']]
            
            # Remove self-loops (paths where incoming_edge == outgoing_edge)
            new_paths = new_paths[new_paths['incoming_edge'] != new_paths['outgoing_edge']]
            
            if len(new_paths) == 0:
                break  # No valid new paths
            
            # Combine existing and new paths
            combined = pd.concat([paths, new_paths], ignore_index=True)
            
            # Keep only minimum cost for each (source, destination) pair
            updated_paths = combined.loc[
                combined.groupby(['incoming_edge', 'outgoing_edge'])['cost'].idxmin()
            ].reset_index(drop=True)
            
            # Check convergence: if no change in paths, we're done
            if converging_check(paths, updated_paths):
                break
            
            paths = updated_paths
            iteration += 1
        
        return paths
    
    # Apply the pandas function to each partition group
    result = df_shortcuts.groupBy(partition_columns).applyInPandas(
        process_partition_pandas,
        schema=output_schema
    )
    
    return result


# ============================================================================
# MAIN EXECUTION FOR PANDAS VERSION
# ============================================================================

def main(
    edges_file: str,
    graph_file: str,
    resolution_range: range = range(14, 8, -1)
):
    """
    Main execution function for pandas-based shortest path computation.
    
    Args:
        edges_file: Path to edges CSV file
        graph_file: Path to edge graph CSV file
        resolution_range: Range of H3 resolutions to process
    """
    # Initialize
    spark = initialize_spark(app_name="ShortestPathPandas")
    
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
            
            # Compute shortest paths per partition using Pandas
            print(f"Computing shortest paths using Pandas (partition-wise)...")
            shortcuts_df_new = compute_shortest_paths_per_partition(
                shortcuts_df_with_cell,
                partition_columns=["current_cell"]
            )
            
            count = shortcuts_df_new.count()
            print(f"✓ Generated {count} shortcuts at resolution {current_resolution}")
            
            # Optional: merge back to main table
            # shortcuts_df = merge_shortcuts_to_main_table(shortcuts_df, shortcuts_df_new)
        
        print("\n" + "="*60)
        print("Shortest path computation (Pandas version) completed!")
        print("="*60)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main(
        edges_file="data/burnaby_driving_simplified_edges_with_h3.csv",
        graph_file="data/burnaby_driving_edge_graph.csv",
        resolution_range=range(14, 8, -1)
    )