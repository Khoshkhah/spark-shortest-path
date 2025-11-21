"""
Hybrid Shortest Path Computation using PySpark (Scipy + Pure Spark)

This module implements a hybrid approach that allows selecting the optimal
algorithm (Scipy or Pure Spark) for different H3 resolution ranges.

Strategy:
- Use Pure Spark for fine resolutions (10-16): Many small partitions
- Use Scipy for coarse resolutions (0-10): Fewer but larger, denser partitions

Author: [Your Name]
Date: 2025
"""

from pyspark.sql import SparkSession, DataFrame
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
    merge_shortcuts_to_main_table,
    add_final_info_for_shortcuts
)

# Import computation functions from both implementations
from shortest_path_scipy_spark import compute_shortest_paths_per_partition
from shortest_path_pure_spark import run_grouped_shortest_path_with_convergence

# Initialize logger
logger = get_logger(__name__)


# ============================================================================
# HYBRID MAIN EXECUTION
# ============================================================================

def main(
    edges_file: str,
    graph_file: str,
    resolution_range: range = range(15, -1, -1),
    scipy_resolutions: list = None,
    pure_spark_resolutions: list = None,
    max_iterations: int = 10
):
    """
    Main execution function for hybrid shortest path computation.
    
    Allows selecting which algorithm to use for each resolution level.
    
    Args:
        edges_file: Path to edges CSV file
        graph_file: Path to edge graph CSV file
        resolution_range: Range of H3 resolutions to process
        scipy_resolutions: List of resolutions to use Scipy algorithm (default: 0-10)
        pure_spark_resolutions: List of resolutions to use Pure Spark (default: 11-15)
        max_iterations: Maximum iterations for Pure Spark algorithm
    """
    
    log_section(logger, "SHORTEST PATH COMPUTATION - HYBRID VERSION")
    
    # Default algorithm selection if not specified
    if scipy_resolutions is None and pure_spark_resolutions is None:
        # Default strategy: Scipy for coarse (0-10), Pure Spark for fine (11-15)
        scipy_resolutions = list(range(0, 11))
        pure_spark_resolutions = list(range(11, 16))
    elif scipy_resolutions is None:
        scipy_resolutions = []
    elif pure_spark_resolutions is None:
        pure_spark_resolutions = []
    
    # Log configuration
    config = {
        "edges_file": edges_file,
        "graph_file": graph_file,
        "resolution_range": f"{resolution_range.start} to {resolution_range.stop}",
        "scipy_resolutions": scipy_resolutions,
        "pure_spark_resolutions": pure_spark_resolutions,
        "max_iterations": max_iterations,
        "approach": "Hybrid (Scipy + Pure Spark)"
    }
    log_dict(logger, config, "Configuration")
    
    spark = None
    
    try:
        # Initialize Spark
        logger.info("Initializing Spark session...")
        spark = initialize_spark(app_name="ShortestPathHybrid")
        logger.info("✓ Spark session initialized successfully")
        
        # Load and prepare data
        logger.info("Loading edge data...")
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
            # Determine which algorithm to use
            if current_resolution in scipy_resolutions:
                algorithm = "Scipy"
            elif current_resolution in pure_spark_resolutions:
                algorithm = "Pure Spark"
            else:
                logger.warning(f"Resolution {current_resolution} not assigned to any algorithm, skipping")
                continue
            
            log_section(logger, f"Processing resolution {current_resolution} using {algorithm}")
            
            try:
                # Enrich with spatial info
                logger.info("Enriching shortcuts with spatial information...")
                shortcuts_df = add_info_for_shortcuts(spark, shortcuts_df, edges_df)
                shortcuts_df = shortcuts_df.checkpoint().cache()
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
                )
                
                # Compute shortest paths using selected algorithm
                if algorithm == "Scipy":
                    logger.info("Computing shortest paths using Scipy (partition-wise)...")
                    shortcuts_df_new = compute_shortest_paths_per_partition(
                        shortcuts_df_with_cell,
                        partition_columns=["current_cell"]
                    )
                else:  # Pure Spark
                    logger.info("Computing shortest paths using Pure Spark SQL...")
                    shortcuts_df_with_cell = shortcuts_df_with_cell.cache()
                    shortcuts_df_new = run_grouped_shortest_path_with_convergence(
                        shortcuts_df_with_cell,
                        max_iterations=max_iterations
                    )
                
                count = shortcuts_df_new.count()
                logger.info(f"✓ Generated {count} shortcuts at resolution {current_resolution} using {algorithm}")
                
                resolution_results.append({
                    "resolution": current_resolution,
                    "algorithm": algorithm,
                    "count": count
                })
                
                # Merge back to main table
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

        # ============================================================================
        # STEP 2: GLOBAL OPTIMIZATION (Resolution -1)
        # ============================================================================
        
        log_section(logger, "GLOBAL OPTIMIZATION (Resolution -1)")
        
        try:
            current_resolution = -1
            algorithm = "Scipy" # Always use Scipy for global step (single partition)
            
            logger.info("Enriching shortcuts with spatial information...")
            shortcuts_df = add_info_for_shortcuts(spark, shortcuts_df, edges_df)
            shortcuts_df = shortcuts_df.checkpoint().cache()
            
            # Filter for LCA <= 0 (covers -1 and 0)
            logger.info(f"Filtering for global shortcuts (LCA <= 0)...")
            shortcuts_df_filtered = shortcuts_df.filter(F.col("lca_res") <= 0)
            filtered_count = shortcuts_df_filtered.count()
            logger.info(f"✓ Filtered to {filtered_count} global shortcuts")
            
            # Add spatial partition key (returns "global")
            logger.info("Adding global partition key...")
            shortcuts_df_with_cell = add_parent_cell_at_resolution(
                shortcuts_df_filtered,
                current_resolution
            )
            
            logger.info("Computing global shortest paths using Scipy...")
            shortcuts_df_new = compute_shortest_paths_per_partition(
                shortcuts_df_with_cell,
                partition_columns=["current_cell"]
            )
            
            count = shortcuts_df_new.count()
            logger.info(f"✓ Generated {count} global shortcuts")
            
            resolution_results.append({
                "resolution": -1,
                "algorithm": "Global (Scipy)",
                "count": count
            })
            
            logger.info("Updating main shortcuts table...")
            shortcuts_df = merge_shortcuts_to_main_table(shortcuts_df, shortcuts_df_new)
            shortcuts_df = shortcuts_df.localCheckpoint()
            
            shortcuts_df_new.unpersist()
            shortcuts_df_with_cell.unpersist()
            
        except Exception as e:
            logger.error(f"Error processing global resolution: {str(e)}")
            raise

        # ============================================================================
        # STEP 3: UPWARD PASS (Resolution 0 -> 15)
        # ============================================================================
        
        log_section(logger, "UPWARD PASS (Resolution 0 -> 15)")
        
        # Process from coarse to fine
        upward_range = range(0, 16)
        
        for current_resolution in upward_range:
            # Determine algorithm (same logic as downward pass)
            if current_resolution in scipy_resolutions:
                algorithm = "Scipy"
            elif current_resolution in pure_spark_resolutions:
                algorithm = "Pure Spark"
            else:
                # Default to Scipy for coarse, Spark for fine if not specified
                algorithm = "Scipy" if current_resolution <= 10 else "Pure Spark"
            
            log_section(logger, f"Upward Pass: Resolution {current_resolution} using {algorithm}")
            
            try:
                logger.info("Enriching shortcuts with spatial information...")
                shortcuts_df = add_info_for_shortcuts(spark, shortcuts_df, edges_df)
                shortcuts_df = shortcuts_df.checkpoint().cache()
                
                logger.info(f"Filtering by resolution {current_resolution}...")
                shortcuts_df_filtered = filter_shortcuts_by_resolution(
                    shortcuts_df,
                    current_resolution
                )
                
                logger.info("Adding spatial partition keys...")
                shortcuts_df_with_cell = add_parent_cell_at_resolution(
                    shortcuts_df_filtered,
                    current_resolution
                )
                
                if algorithm == "Scipy":
                    logger.info("Computing shortest paths using Scipy...")
                    shortcuts_df_new = compute_shortest_paths_per_partition(
                        shortcuts_df_with_cell,
                        partition_columns=["current_cell"]
                    )
                else:
                    logger.info("Computing shortest paths using Pure Spark...")
                    shortcuts_df_with_cell = shortcuts_df_with_cell.cache()
                    shortcuts_df_new = run_grouped_shortest_path_with_convergence(
                        shortcuts_df_with_cell,
                        max_iterations=max_iterations
                    )
                
                count = shortcuts_df_new.count()
                logger.info(f"✓ Generated {count} shortcuts at resolution {current_resolution}")
                
                resolution_results.append({
                    "resolution": current_resolution,
                    "algorithm": f"Upward {algorithm}",
                    "count": count
                })
                
                logger.info("Updating main shortcuts table...")
                shortcuts_df = merge_shortcuts_to_main_table(shortcuts_df, shortcuts_df_new)
                shortcuts_df = shortcuts_df.localCheckpoint()
                
                shortcuts_df_new.unpersist()
                shortcuts_df_with_cell.unpersist()
                
            except Exception as e:
                logger.error(f"Error processing upward resolution {current_resolution}: {str(e)}")
                raise
        
        # Log summary
        log_section(logger, "SUMMARY")
        logger.info(f"Processed {len(resolution_results)} resolutions")
        for result in resolution_results:
            algo_str = f" ({result['algorithm']})" if 'algorithm' in result else ""
            logger.info(f"  Resolution {result['resolution']}{algo_str}: {result['count']} shortcuts")
        
        logger.info("✓ Shortest path computation completed successfully!")
        
        # Save final shortcuts table to output
        log_section(logger, "SAVING OUTPUT")
        final_count = shortcuts_df.count()
        logger.info(f"Final shortcuts table contains {final_count} rows")
        
        import config
        logger.info("Adding final info to shortcuts...")
        shortcuts_df = add_final_info_for_shortcuts(spark, shortcuts_df, edges_df)
        output_path = str(config.SHORTCUTS_OUTPUT_FILE)
        logger.info(f"Saving shortcuts table to: {output_path}")
        
        shortcuts_df.write.mode("overwrite").parquet(output_path)
        logger.info(f"✓ Shortcuts table saved successfully!")
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        raise
        
    finally:
        if spark:
            logger.info("\nShutting down Spark session...")
            spark.stop()
            logger.info("✓ Spark session closed")
    
    log_section(logger, "JOB COMPLETED")


import config

if __name__ == "__main__":
    # Example 1: Use default strategy (Scipy for 0-10, Pure Spark for 11-15)
    main(
        edges_file=str(config.EDGES_FILE),
        graph_file=str(config.GRAPH_FILE),
        resolution_range=range(15, -1, -1)
    )
    
    # Example 2: Custom algorithm selection
    # main(
    #     edges_file=str(config.EDGES_FILE),
    #     graph_file=str(config.GRAPH_FILE),
    #     resolution_range=range(15, 7, -1),
    #     scipy_resolutions=[8, 9, 10],
    #     pure_spark_resolutions=[11, 12, 13, 14, 15],
    #     max_iterations=20
    # )
