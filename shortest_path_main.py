"""
Shared utilities for Shortest Path Computation using PySpark and H3 Geospatial Indexing

This module contains common functions used by both pandas and pure Spark implementations.

Author: [Your Name]
Date: 2025
"""

import os
from typing import List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import h3


# ============================================================================
# 1. SPARK SESSION INITIALIZATION
# ============================================================================

def initialize_spark(app_name: str = "AllPairsShortestPath", driver_memory: str = "8g") -> SparkSession:
    """
    Initialize and configure a PySpark session.
    
    Args:
        app_name: Name of the Spark application
        driver_memory: Memory allocation for the driver (e.g., "8g", "16g")
    
    Returns:
        Configured SparkSession instance
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.memory", driver_memory)
        .getOrCreate()
    )
    spark.sparkContext.setCheckpointDir("checkpoints")
    return spark


# ============================================================================
# 2. DATA LOADING AND INITIALIZATION
# ============================================================================

def read_edges(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Load edge data from CSV file.
    
    Expected columns: id, incoming_cell, outgoing_cell, lca_res
    
    Args:
        spark: SparkSession instance
        file_path: Path to edges CSV file
    
    Returns:
        DataFrame with edge information
    """
    edges_df = spark.read.csv(file_path, header=True, inferSchema=True)
    return edges_df.select("id", "incoming_cell", "outgoing_cell", "lca_res")


def initial_shortcuts_table(spark: SparkSession, file_path: str, edges_cost_df: DataFrame) -> DataFrame:
    """
    Create initial shortcuts table from edge graph.
    
    Represents direct connections between edges. The via_edge column initially
    points to the outgoing edge (direct connection).
    
    Args:
        spark: SparkSession instance
        file_path: Path to edge graph CSV file
        edges_cost_df: DataFrame containing edge costs
    
    Returns:
        Initial shortcuts DataFrame with columns: incoming_edge, outgoing_edge, via_edge, cost
    """
    shortcuts_df = spark.read.csv(file_path, header=True, inferSchema=True)
    shortcuts_df = shortcuts_df.select("incoming_edge", "outgoing_edge")
    
    # Initialize via_edge as direct connection
    shortcuts_df = shortcuts_df.withColumn("via_edge", F.col("outgoing_edge"))
    
    # Join with edge costs
    shortcuts_df = shortcuts_df.join(
        edges_cost_df.select("id", "cost"),
        shortcuts_df.incoming_edge == edges_cost_df.id,
        "left"
    ).drop(edges_cost_df.id)
    
    return shortcuts_df


# ============================================================================
# 3. COST CALCULATION
# ============================================================================

@F.udf(returnType=DoubleType())
def dummy_cost(length: float, maxspeed: float) -> float:
    """
    Calculate edge cost based on length and maximum speed.
    
    Formula: cost = length / maxspeed
    
    Args:
        length: Road segment length
        maxspeed: Maximum speed limit
    
    Returns:
        Calculated cost (float), or infinity for invalid speeds
    """
    if maxspeed <= 0:
        return float('inf')
    return float(length) / float(maxspeed)


def update_dummy_costs_for_edges(spark: SparkSession, file_path: str, edges_df: DataFrame) -> DataFrame:
    """
    Add cost column to edges DataFrame using the dummy_cost function.
    
    Args:
        spark: SparkSession instance
        file_path: Path to edges CSV file (must contain: id, length, maxspeed)
        edges_df: Original edges DataFrame
    
    Returns:
        DataFrame with added cost column
    """
    edges_df_cost = spark.read.csv(file_path, header=True, inferSchema=True).select("id", "length", "maxspeed")
    edges_df_cost = edges_df_cost.withColumn("cost", dummy_cost(F.col("length"), F.col("maxspeed")))
    
    edges_result = edges_df.drop("cost") if "cost" in edges_df.columns else edges_df
    edges_result = edges_result.join(edges_df_cost.select("id", "cost"), on="id", how="left")
    
    return edges_result


# ============================================================================
# 4. H3 GEOSPATIAL UTILITIES
# ============================================================================

@F.udf(StringType())
def find_lca(cell1: str, cell2: str) -> str:
    """
    Find the Lowest Common Ancestor (LCA) cell between two H3 cells.
    
    The LCA is the coarsest resolution cell that contains both input cells.
    
    Args:
        cell1: First H3 cell ID
        cell2: Second H3 cell ID
    
    Returns:
        LCA cell ID, or None if no common ancestor exists
    """
    if cell1 is None or cell2 is None:
        return None
    
    cell1_res = h3.get_resolution(cell1)
    cell2_res = h3.get_resolution(cell2)
    lca_res = min(cell1_res, cell2_res)
    
    while lca_res > 0:
        if h3.cell_to_parent(cell1, lca_res) == h3.cell_to_parent(cell2, lca_res):
            return h3.cell_to_parent(cell1, lca_res)
        lca_res -= 1
    
    return None


@F.udf(IntegerType())
def find_resolution(cell: str) -> int:
    """
    Extract H3 resolution from a cell ID.
    
    Args:
        cell: H3 cell ID
    
    Returns:
        Resolution level (0-15), or -1 if cell is None
    """
    if cell is None:
        return -1
    return h3.get_resolution(cell)


def add_info_for_shortcuts(spark: SparkSession, shortcuts_df: DataFrame, edges_df: DataFrame) -> DataFrame:
    """
    Enrich shortcuts DataFrame with spatial information from edges.
    
    Adds:
    - incoming_cell, outgoing_cell: The cells from respective edges
    - lca_res: Lowest common ancestor resolution
    - via_cell: LCA between start and end cells
    - via_res: Resolution of the via_cell
    
    Args:
        spark: SparkSession instance
        shortcuts_df: Current shortcuts DataFrame
        edges_df: Edge information DataFrame
    
    Returns:
        Enriched shortcuts DataFrame
    """
    # Clean up existing spatial columns
    for col in ["lca_res", "via_cell", "via_res"]:
        if col in shortcuts_df.columns:
            shortcuts_df = shortcuts_df.drop(col)
    
    # Join incoming edge information
    shortcuts_df = shortcuts_df.join(
        edges_df.select(
            F.col("id").alias("incoming_edge_id"),
            F.col("incoming_cell").alias("incoming_cell_in"),
            F.col("lca_res").alias("lca_res_in")
        ),
        shortcuts_df.incoming_edge == F.col("incoming_edge_id"),
        "left"
    ).drop("incoming_edge_id")
    
    # Join outgoing edge information
    shortcuts_df = shortcuts_df.join(
        edges_df.select(
            F.col("id").alias("outgoing_edge_id"),
            F.col("outgoing_cell").alias("outgoing_cell_out"),
            F.col("lca_res").alias("lca_res_out")
        ),
        shortcuts_df.outgoing_edge == F.col("outgoing_edge_id"),
        "left"
    ).drop("outgoing_edge_id")
    
    # Calculate aggregate lca_res
    shortcuts_df = shortcuts_df.withColumn(
        "lca_res",
        F.greatest(F.col("lca_res_in"), F.col("lca_res_out"))
    ).drop("lca_res_in", "lca_res_out")
    
    # Calculate via_cell and via_res
    shortcuts_df = shortcuts_df.withColumn(
        "via_cell",
        find_lca(F.col("incoming_cell_in"), F.col("outgoing_cell_out"))
    ).withColumn(
        "via_res",
        find_resolution(F.col("via_cell"))
    ).drop("incoming_cell_in", "outgoing_cell_out")
    
    return shortcuts_df


# ============================================================================
# 5. FILTERING BY RESOLUTION
# ============================================================================

def filter_shortcuts_by_resolution(shortcuts_df: DataFrame, current_res: int) -> DataFrame:
    """
    Filter shortcuts based on H3 resolution level.
    
    Keeps only shortcuts where:
    - lca_res <= current_res (LCA is at or below current resolution)
    - via_res >= current_res (via_cell is at or above current resolution)
    
    Args:
        shortcuts_df: Shortcuts DataFrame
        current_res: Target H3 resolution level
    
    Returns:
        Filtered DataFrame
    """
    return shortcuts_df.filter(
        (F.col("lca_res") <= current_res) & (F.col("via_res") >= current_res)
    )


# ============================================================================
# 6. SPATIAL PARTITIONING
# ============================================================================

def add_parent_cell_at_resolution(shortcuts_df: DataFrame, current_resolution: int) -> DataFrame:
    """
    Add spatial partition key (current_cell) to shortcuts.
    
    Groups shortcuts by their parent cell at the specified resolution.
    This enables distributed shortest path computation per spatial region.
    
    Args:
        shortcuts_df: Shortcuts DataFrame with via_cell column
        current_resolution: Target H3 resolution for partitioning
    
    Returns:
        DataFrame with added current_cell column
    """
    @F.udf(StringType())
    def get_parent_cell(cell: str, target_resolution: int) -> str:
        """Get parent cell at target resolution, handling edge cases."""
        if cell is None:
            return None
        try:
            cell_res = h3.get_resolution(cell)
            if target_resolution >= cell_res:
                return cell
            return h3.cell_to_parent(cell, target_resolution)
        except Exception:
            return None
    
    shortcuts_df = shortcuts_df.withColumn(
        "current_cell",
        get_parent_cell(F.col("via_cell"), F.lit(current_resolution))
    )
    
    # Clean up intermediate columns
    shortcuts_df = shortcuts_df.drop("lca_res", "via_cell", "via_res")
    
    return shortcuts_df


# ============================================================================
# 7. MERGING RESULTS
# ============================================================================

def merge_shortcuts_to_main_table(main_df: DataFrame, new_shortcuts: DataFrame) -> DataFrame:
    """
    Merge newly computed shortcuts back into the main table.
    
    Strategy:
    1. Remove old paths for (incoming_edge, outgoing_edge) pairs that have been updated
    2. Add the new computed paths
    
    Args:
        main_df: Main shortcuts DataFrame
        new_shortcuts: Newly computed shortcuts
    
    Returns:
        Updated DataFrame with new shortcuts
    """
    # Standardize columns
    main_df = main_df.select("incoming_edge", "outgoing_edge", "cost", "via_edge")
    new_shortcuts = new_shortcuts.select("incoming_edge", "outgoing_edge", "cost", "via_edge")
    
    # Remove old entries that are being replaced
    remaining_df = main_df.alias("main").join(
        new_shortcuts.alias("update"),
        on=(
            (F.col("main.incoming_edge") == F.col("update.incoming_edge")) &
            (F.col("main.outgoing_edge") == F.col("update.outgoing_edge"))
        ),
        how="left_anti"
    )
    
    # Combine old and new
    updated_df = remaining_df.unionByName(new_shortcuts)
    
    return updated_df