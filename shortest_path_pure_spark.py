"""
Shortest Path Computation using Pure PySpark (With Logging)

This module implements shortest path computation using only Spark SQL operations
with comprehensive logging to both terminal and file.

Advantages:
- Works well for extremely large partitions (> 100k rows) where Scipy might OOM
- Better GPU acceleration support (via RAPIDS/Spark SQL)
- All operations logged to file for debugging

Author: [Your Name]
Date: 2025
"""

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from logging_config import get_logger, log_section, log_dict
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

# Initialize logger
logger = get_logger(__name__)


# ============================================================================
# PURE SPARK SHORTEST PATH COMPUTATION
# ============================================================================

def has_converged(current_paths: DataFrame, next_paths: DataFrame) -> bool:
    """
    Check if the shortest path computation has converged.
    
    Convergence occurs when no new paths or path improvements are found.
    
    Args:
        current_paths: Paths from previous iteration
        next_paths: Paths computed in current iteration
    
    Returns:
        True if converged (no changes), False if changes exist
    """
    join_keys = ["incoming_edge", "outgoing_edge", "cost", "current_cell"]
    
    # Find rows in next_paths that don't exist in current_paths
    changes = next_paths.join(
        current_paths.select(join_keys),
        on=join_keys,
        how="left_anti"
    )
    
    # True if no changes found (converged)
    return changes.limit(1).count() == 0


def run_grouped_shortest_path_with_convergence(
    shortcuts_df: DataFrame,
    max_iterations: int = 10
) -> DataFrame:
    """
    Compute all-pairs shortest paths using pure Spark SQL operations.
    
    Uses self-joins and window functions to iteratively extend paths until
    convergence. All operations logged to file.
    
    Args:
        shortcuts_df: Input shortcuts DataFrame
        max_iterations: Maximum iterations before stopping
    
    Returns:
        DataFrame with computed shortest paths
    """
    
    logger.info(f"Starting shortest path computation with max_iterations={max_iterations}")
    
    # Initialize
    current_paths = shortcuts_df.select(
        "incoming_edge", "outgoing_edge", "cost", "via_edge", "current_cell"
    ).cache()
    
    initial_count = current_paths.count()
    logger.info(f"Initial paths count: {initial_count}")
    
    for iteration in range(max_iterations):
        logger.info(f"\n--- Iteration {iteration} ---")
        
        try:
            # --- PATH EXTENSION ---
            new_paths = current_paths.alias("L").join(
                current_paths.alias("R"),
                [
                    F.col("L.outgoing_edge") == F.col("R.incoming_edge"),
                    F.col("L.current_cell") == F.col("R.current_cell")
                ],
                "inner"
            ).filter(
                (F.col("L.incoming_edge") != F.col("R.outgoing_edge"))
            ).select(
                F.col("L.incoming_edge").alias("incoming_edge"),
                F.col("R.outgoing_edge").alias("outgoing_edge"),
                (F.col("L.cost") + F.col("R.cost")).alias("cost"),
                F.col("L.outgoing_edge").alias("via_edge"),
                F.col("L.current_cell").alias("current_cell")
            ).cache()
            
            new_count = new_paths.count()
            logger.info(f"Found {new_count} new paths")
            
            # Check if any new paths were found
            if new_count == 0:
                logger.info("✓ No new paths found. Converged.")
                break
            
            # --- COST MINIMIZATION ---
            all_paths = current_paths.unionByName(new_paths)
            
            window_spec = Window.partitionBy(
                "incoming_edge",
                "outgoing_edge",
                "current_cell"
            ).orderBy(
                F.col("cost").asc(),
                F.col("via_edge").asc()
            )
            
            next_paths = all_paths.withColumn(
                "rnk",
                F.row_number().over(window_spec)
            ).filter(
                F.col("rnk") == 1
            ).drop("rnk").localCheckpoint()
            
            next_count = next_paths.count()
            logger.info(f"After cost minimization: {next_count} paths")
            
            # Clean up
            current_paths.unpersist()
            new_paths.unpersist()
            
            # --- CONVERGENCE CHECK ---
            if has_converged(current_paths, next_paths):
                logger.info(f"✓ Iteration {iteration}: Converged - no improvements found!")
                break
            
            current_paths = next_paths
            logger.info(f"✓ Iteration {iteration}: Completed successfully")
            
        except Exception as e:
            logger.error(f"Error in iteration {iteration}: {str(e)}")
            raise
    
    logger.info(f"\nShortest path computation completed after {iteration + 1} iterations")
    
    # Remove temporary columns and return result
    return current_paths.drop("current_cell")


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
    
    log_section(logger, "SHORTEST PATH COMPUTATION - PURE SPARK VERSION")
    
    # Log configuration
    config = {
        "edges_file": edges_file,
        "graph_file": graph_file,
        "resolution_range": f"{resolution_range.start} to {resolution_range.stop}",
        "max_iterations": max_iterations
    }
    log_dict(logger, config, "Configuration")
    
    spark = None
    
    try:
        # Initialize Spark
        logger.info("Initializing Spark session...")
        spark = initialize_spark(app_name="ShortestPathSpark")
        logger.info("✓ Spark session initialized successfully")
        
        # Load and prepare data
        logger.info("\nLoading edge data...")
        edges_df = read_edges(spark, edges_file).cache()
        edges_count = edges_df.count()
        logger.info(f"✓ Loaded {edges_count} edges")
        
        logger.info("Computing edge costs...")
        edges_cost_df = update_dummy_costs_for_edges(spark, edges_file, edges_df)
        logger.info("✓ Edge costs computed")
        
        logger.info("Creating initial shortcuts table...")
        shortcuts_df = initial_shortcuts_table(spark, graph_file, edges_cost_df)
        shortcuts_count = shortcuts_df.count()
        logger.info(f"✓ Created shortcuts table with {shortcuts_count} entries")

        # Process each resolution level
        resolution_results = []
        
        for current_resolution in resolution_range:
            log_section(logger, f"Processing resolution {current_resolution}")
            
            try:
                # Enrich with spatial info (User requested this inside loop)
                logger.info("Enriching shortcuts with spatial information...")
                # We keep a reference to unpersist the old one if needed, but since we are overwriting
                # the variable 'shortcuts_df' which is used in the next iteration, we must be careful.
                # If the user intends to accumulate info, we let it be.
                # However, to save memory, we should use localCheckpoint here too.
                
                shortcuts_df = add_info_for_shortcuts(spark, shortcuts_df, edges_df)
                shortcuts_df = shortcuts_df.localCheckpoint()
                logger.info("✓ Spatial information added and checkpointed")

                # Filter by resolution
                logger.info(f"Filtering by resolution {current_resolution}...")
                shortcuts_df_filtered = filter_shortcuts_by_resolution(
                    shortcuts_df,
                    current_resolution
                )
                filtered_count = shortcuts_df_filtered.count()
                logger.info(f"✓ Filtered to {filtered_count} shortcuts")
                
                # Add spatial partition key
                logger.info("Adding spatial partition keys...")
                shortcuts_df_with_cell = add_parent_cell_at_resolution(
                    shortcuts_df_filtered,
                    current_resolution
                ).cache() # Cache this as it's the input to the iterative process
                logger.info("✓ Spatial partition keys added")
                
                # Compute shortest paths
                logger.info("Computing shortest paths using pure Spark SQL...")
                shortcuts_df_new = run_grouped_shortest_path_with_convergence(
                    shortcuts_df_with_cell,
                    max_iterations=max_iterations
                )
                
                count = shortcuts_df_new.count()
                logger.info(f"✓ Generated {count} shortcuts at resolution {current_resolution}")
                
                resolution_results.append({
                    "resolution": current_resolution,
                    "count": count
                })

                # Update main shortcuts table with new paths
                logger.info("Updating main shortcuts table...")
                shortcuts_df = merge_shortcuts_to_main_table(shortcuts_df, shortcuts_df_new)
                shortcuts_df = shortcuts_df.localCheckpoint()
                logger.info("✓ Main shortcuts table updated and checkpointed")

                # FORCE CLEANUP
                shortcuts_df_new.unpersist()
                shortcuts_df_with_cell.unpersist()
                
            except Exception as e:
                logger.error(f"Error processing resolution {current_resolution}: {str(e)}")
                raise
        
        # Log summary
        log_section(logger, "SUMMARY")
        logger.info(f"Processed {len(resolution_results)} resolutions")
        for result in resolution_results:
            logger.info(f"  Resolution {result['resolution']}: {result['count']} shortcuts")
        
        logger.info("✓ Shortest path computation completed successfully!")
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        raise
        
    finally:
        if spark:
            logger.info("\nShutting down Spark session...")
            spark.stop()
            logger.info("✓ Spark session closed")
    
    log_section(logger, "JOB COMPLETED")


if __name__ == "__main__":
    main(
        edges_file="data/burnaby_driving_simplified_edges_with_h3.csv",
        graph_file="data/burnaby_driving_edge_graph.csv",
        resolution_range=range(15, -1, -1),
        max_iterations=20
    )