"""
Configuration parameters for Shortest Path Computation

This module centralizes all configurable parameters for the application,
making it easy to adjust settings without modifying the main code.
"""

import os
from pathlib import Path

# ============================================================================
# SPARK CONFIGURATION
# ============================================================================

# Driver memory allocation
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "8g")

# Executor memory allocation (per executor)
SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "4g")

# Number of executor cores
SPARK_EXECUTOR_CORES = int(os.getenv("SPARK_EXECUTOR_CORES", "4"))

# Application name
SPARK_APP_NAME = "AllPairsShortestPath"

# Checkpoint directory for Spark lineage management
CHECKPOINT_DIR = "checkpoints"

# ============================================================================
# FILE PATHS
# ============================================================================

# Base data directory
DATA_DIR = Path("data")

# Edge data file - contains OSM-derived road network data with H3 indices
EDGES_FILE = DATA_DIR / "burnaby_driving_simplified_edges_with_h3.csv"

# Edge graph file - defines which edges are connected
GRAPH_FILE = DATA_DIR / "burnaby_driving_edge_graph.csv"

# Output directory for results
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

# ============================================================================
# COMPUTATION PARAMETERS
# ============================================================================

# H3 Resolution Range
# Valid range: 0 (world) to 15 (highest detail, ~10m)
# Typical road network: 13-15
MIN_H3_RESOLUTION = 8
MAX_H3_RESOLUTION = 14

# Process from high to low resolution (coarse to fine)
RESOLUTION_RANGE = range(MAX_H3_RESOLUTION, MIN_H3_RESOLUTION - 1, -1)

# Maximum iterations per spatial partition
# Floyd-Warshall typically converges in O(V) iterations where V = num nodes
MAX_ITERATIONS_PER_PARTITION = 10

# ============================================================================
# COST CALCULATION
# ============================================================================

# Cost function formula: cost = length / maxspeed
# Time units: seconds (length in meters, speed in m/s)
# To use minutes instead: uncomment the formula below
# cost = length / maxspeed / 60

# Invalid speed handling
INVALID_SPEED_COST = float('inf')

# ============================================================================
# PERFORMANCE TUNING
# ============================================================================

# Cache frequently used DataFrames to avoid recomputation
USE_CACHING = True

# Use checkpoint() for iterative operations to break lineage
# Prevents stack overflow for deep DAGs
USE_CHECKPOINTING = True

# Repartition data before intensive operations
# Set to None to skip repartitioning
REPARTITION_SIZE = None  # Auto-adjust based on cluster size

# ============================================================================
# LOGGING AND OUTPUT
# ============================================================================

# Log level: DEBUG, INFO, WARNING, ERROR
LOG_LEVEL = "INFO"

# Print progress information
VERBOSE = True

# Save intermediate results
SAVE_INTERMEDIATE_RESULTS = False

# Output format for results: csv, parquet
OUTPUT_FORMAT = "parquet"

# ============================================================================
# VALIDATION SETTINGS
# ============================================================================

# Validate input data before processing
VALIDATE_INPUT = True

# Maximum missing values (%) allowed in required columns
MAX_MISSING_PERCENTAGE = 5.0

# Check for duplicate edges
CHECK_DUPLICATES = True

# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def get_config_summary() -> dict:
    """
    Return a dictionary of all configuration parameters.
    
    Returns:
        Dictionary with all configuration values
    """
    return {
        "spark_driver_memory": SPARK_DRIVER_MEMORY,
        "spark_executor_memory": SPARK_EXECUTOR_MEMORY,
        "h3_resolution_range": list(RESOLUTION_RANGE),
        "edges_file": str(EDGES_FILE),
        "graph_file": str(GRAPH_FILE),
        "output_dir": str(OUTPUT_DIR),
        "max_iterations": MAX_ITERATIONS_PER_PARTITION,
        "use_caching": USE_CACHING,
        "use_checkpointing": USE_CHECKPOINTING,
    }


def print_config():
    """Print all configuration parameters to console."""
    config = get_config_summary()
    print("\n" + "="*60)
    print("CONFIGURATION SUMMARY")
    print("="*60)
    for key, value in config.items():
        print(f"{key:.<40} {value}")
    print("="*60 + "\n")