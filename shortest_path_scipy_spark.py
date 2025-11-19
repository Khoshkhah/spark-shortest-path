"""
Shortest Path Computation using PySpark (with Scipy acceleration and Logging)

This module implements shortest path computation using Spark for data distribution
and Scipy (sparse matrix algorithms) for efficient inner-partition computation. 
This hybrid approach combines the scalability of Spark with the performance of 
C-optimized graph algorithms.

All operations are logged to both terminal and log file.

Advantages:
- Significantly faster than iterative Pandas merges for dense graphs
- Memory efficient (uses sparse matrices)
- Leverages Scipy's C-optimized Dijkstra/Johnson algorithms
- Full logging for debugging

Author: [Your Name]
Date: 2025
"""

import pandas as pd
import gc
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
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
# SCIPY-BASED SHORTEST PATH COMPUTATION
# ============================================================================

def compute_shortest_paths_per_partition(
    df_shortcuts: DataFrame,
    partition_columns: list,
) -> DataFrame:
    """
    Compute all-pairs shortest paths using Scipy (Dijkstra/Floyd-Warshall).
    
    Uses Spark's applyInPandas to process each partition.
    This approach uses scipy.sparse.csgraph.shortest_path which is significantly
    faster and more memory-efficient than iterative pandas merges for dense graphs.
    
    Args:
        df_shortcuts: DataFrame with schema (incoming_edge, outgoing_edge, cost, via_edge)
        partition_columns: List of column names to group by (e.g., ["current_cell"])
    
    Returns:
        DataFrame with computed shortest paths
    """
    
    logger.info(f"Starting shortest path computation per partition (Scipy-optimized)")
    
    # Define output schema
    output_schema = StructType([
        StructField("incoming_edge", StringType(), False),
        StructField("outgoing_edge", StringType(), False),
        StructField("via_edge", StringType(), False),
        StructField("cost", DoubleType(), False),
    ])
    
    def process_partition_scipy(pdf: pd.DataFrame) -> pd.DataFrame:
        """
        Process a single partition using Scipy's shortest_path.
        
        This function executes on each Spark executor for its partition's data.
        It constructs a sparse matrix and uses Dijkstra/Johnson algorithm.
        """
        import pandas as pd
        import numpy as np
        from scipy.sparse import csr_matrix
        from scipy.sparse.csgraph import shortest_path
        
        if len(pdf) == 0:
            return pd.DataFrame(columns=['incoming_edge', 'outgoing_edge', 'via_edge', 'cost'])
        
        logger.debug(f"Processing partition with {len(pdf)} input edges")
        
        # 1. Map string IDs to integers
        # Get all unique nodes (edges in the road network)
        nodes = pd.concat([pdf['incoming_edge'], pdf['outgoing_edge']]).unique()
        n_nodes = len(nodes)
        
        # Create mapping from string to index
        node_to_idx = pd.Series(data=np.arange(n_nodes), index=nodes)
        
        # 2. Deduplicate and prepare matrix data
        # Keep minimum cost for each (source, destination) pair
        pdf_dedup = pdf.loc[
            pdf.groupby(['incoming_edge', 'outgoing_edge'])['cost'].idxmin()
        ]
        
        src_indices = pdf_dedup['incoming_edge'].map(node_to_idx).values
        dst_indices = pdf_dedup['outgoing_edge'].map(node_to_idx).values
        costs = pdf_dedup['cost'].values
        
        # Create CSR matrix
        # Note: duplicate entries are summed by csr_matrix, but we deduped above.
        graph = csr_matrix((costs, (src_indices, dst_indices)), shape=(n_nodes, n_nodes))
        
        # 3. Compute shortest paths
        # method='auto' usually selects Dijkstra for sparse graphs with positive weights
        dist_matrix, predecessors = shortest_path(
            csgraph=graph,
            method='auto',
            directed=True,
            return_predecessors=True
        )
        
        # 4. Convert back to DataFrame
        # Process in chunks to avoid OOM when creating dense arrays from the result
        results = []
        chunk_size = 2000  # Rows per chunk
        
        for start_row in range(0, n_nodes, chunk_size):
            end_row = min(start_row + chunk_size, n_nodes)
            
            # Slice the result matrices
            sub_dist = dist_matrix[start_row:end_row, :]
            sub_pred = predecessors[start_row:end_row, :]
            
            # Find valid entries (reachable and not self-loop)
            # Note: shortest_path returns 0 for self-loops by default
            valid_mask = (sub_dist != np.inf)
            
            rows, cols = np.where(valid_mask)
            
            # Adjust rows to global indices
            global_rows = rows + start_row
            
            # Filter self-loops
            non_loop_mask = (global_rows != cols)
            
            rows = rows[non_loop_mask]
            cols = cols[non_loop_mask]
            global_rows = global_rows[non_loop_mask]
            
            if len(rows) == 0:
                continue
            
            # Extract values
            chunk_costs = sub_dist[rows, cols]
            chunk_preds = sub_pred[rows, cols]
            
            # Map indices back to strings
            chunk_src = nodes[global_rows]
            chunk_dst = nodes[cols]
            chunk_via = nodes[chunk_preds]
            
            chunk_df = pd.DataFrame({
                'incoming_edge': chunk_src,
                'outgoing_edge': chunk_dst,
                'via_edge': chunk_via,
                'cost': chunk_costs
            })
            
            results.append(chunk_df)
            
        if not results:
            return pd.DataFrame(columns=['incoming_edge', 'outgoing_edge', 'via_edge', 'cost'])
            
        final_df = pd.concat(results, ignore_index=True)
        logger.debug(f"Partition complete: {len(pdf)} input -> {len(final_df)} output paths")
        
        return final_df
    
    # Apply the pandas function to each partition group
    logger.info(f"Applying Scipy computation to partitions grouped by: {partition_columns}")
    result = df_shortcuts.groupBy(partition_columns).applyInPandas(
        process_partition_scipy,
        schema=output_schema
    )
    
    logger.info("✓ Scipy computation completed for all partitions")
    
    return result


# ============================================================================
# MAIN EXECUTION FOR SCIPY VERSION
# ============================================================================

def main(
    edges_file: str,
    graph_file: str,
    resolution_range: range = range(14, 8, -1)
):
    """
    Main execution function for Scipy-based shortest path computation.
    
    Args:
        edges_file: Path to edges CSV file
        graph_file: Path to edge graph CSV file
        resolution_range: Range of H3 resolutions to process
    """
    
    log_section(logger, "SHORTEST PATH COMPUTATION - SCIPY-OPTIMIZED VERSION")
    
    # Log configuration
    config = {
        "edges_file": edges_file,
        "graph_file": graph_file,
        "resolution_range": f"{resolution_range.start} to {resolution_range.stop}",
        "approach": "Scipy-optimized (applyInPandas)"
    }
    log_dict(logger, config, "Configuration")
    
    spark = None
    
    try:
        # Initialize Spark
        logger.info("Initializing Spark session...")
        spark = initialize_spark(app_name="ShortestPathPandas")
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
            log_section(logger, f"Processing resolution {current_resolution}")
            
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
                logger.info("✓ Spatial partition keys added")
                
                # Compute shortest paths per partition using Pandas
                logger.info("Computing shortest paths using Pandas (partition-wise)...")
                shortcuts_df_new = compute_shortest_paths_per_partition(
                    shortcuts_df_with_cell,
                    partition_columns=["current_cell"]
                )
                
                count = shortcuts_df_new.count()
                logger.info(f"✓ Generated {count} shortcuts at resolution {current_resolution}")
                
                resolution_results.append({
                    "resolution": current_resolution,
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
        resolution_range=range(15, -1, -1)
    )