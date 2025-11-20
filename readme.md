# Hierarchical Shortest Path Computation using PySpark and H3 Geospatial Indexing

A distributed computing solution for computing all-pairs shortest paths on large-scale road networks using Apache Spark and hierarchical H3 geospatial indexing.

## 🎯 Overview

This project implements a scalable, all-pairs shortest path algorithm for large geospatial graphs using **PySpark** and **H3 Geospatial Indexing**. It employs a hybrid approach, dynamically switching between **Scipy** (for dense, in-memory computation) and **Pure Spark SQL** (for distributed, sparse computation) to optimize performance.

For a detailed explanation of the algorithm, including the bi-directional resolution pass, see [ALGORITHM.md](ALGORITHM.md).
- **PySpark**: Distributed computation across multiple nodes
- **H3 Geospatial Indexing**: Hierarchical spatial partitioning for efficient processing
- **Scipy (Sparse Graphs)**: Efficient in-memory shortest path computation (Dijkstra/Johnson)
- **Multi-resolution Processing**: Processes road networks at different H3 resolution levels

### Key Features

- **Hierarchical Processing**: Processes roads at different spatial granularities (H3 resolutions 8-15)
- **Hybrid Computation**: Combines Spark for distribution with Scipy for fast local execution
- **Memory Efficient**: Uses sparse matrices to handle dense graph connections
- **Scalable**: Capable of handling large road networks by partitioning space

## 📋 Prerequisites

- Python 3.12+
- Apache Spark 3.x
- Java 21+
- PySpark
- h3
- pandas
- scipy
- scipy
- numpy

## 🔌 Data Source

This project requires processed road network data. Use the **[osm-to-road-network](https://github.com/Khoshkhah/osm-to-road-network)** project to generate the required input files:
1.  `*_simplified_edges_with_h3.csv`
2.  `*_edge_graph.csv`

## 🚀 Installation

### 1. Clone the Repository

```bash
git clone https://github.com/khoshkhah/spark-shortest-path.git
cd spark-shortest-path
```

### 2. Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Set JAVA_HOME

```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
```

## 📁 Project Structure

```
shortest-path-spark/
├── src/                             # Source code directory
│   ├── utilities.py                 # Shared utility functions
│   ├── shortest_path_scipy_spark.py # Scipy-optimized implementation
│   ├── shortest_path_pure_spark.py  # Pure Spark SQL implementation
│   ├── shortest_path_hybrid.py      # Hybrid implementation (Recommended)
│   ├── config.py                    # Configuration parameters
│   └── logging_config.py            # Logging configuration
├── requirements.txt                 # Python dependencies
├── data/
│   ├── burnaby_driving_simplified_edges_with_h3.csv    # Edge data
│   └── burnaby_driving_edge_graph.csv                   # Edge graph
├── logs/                            # Execution logs
├── output/                          # Output directory
│   └── shortcuts_final/             # Final shortcuts table (Parquet)
├── notebooks/
│   ├── analyze_shortcuts.ipynb      # Analysis notebook for shortcuts output
│   ├── first_pandas_ver.ipynb       # Original development notebook (Pandas)
│   └── first_pure_spark_ver.ipynb   # Original development notebook (Pure Spark)
└── README.md                        # This file
```

## 🔧 Configuration

Edit `src/config.py` to customize parameters:

```python
# Spark Configuration
SPARK_DRIVER_MEMORY = "8g"
SPARK_EXECUTOR_MEMORY = "4g"

# File Paths
EDGES_FILE = "data/burnaby_driving_simplified_edges_with_h3.csv"
GRAPH_FILE = "data/burnaby_driving_edge_graph.csv"

# Output
SHORTCUTS_OUTPUT_FILE = "output/shortcuts_final"

# H3 Resolution Range
MIN_RESOLUTION = 8
MAX_RESOLUTION = 14

# Computation
MAX_ITERATIONS_PER_PARTITION = 10

# Hybrid Algorithm Selection
SCIPY_RESOLUTIONS = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]  # Coarse resolutions
PURE_SPARK_RESOLUTIONS = [11, 12, 13, 14, 15]           # Fine resolutions
```

## ▶️ Usage

### Option 1: Hybrid Version (Recommended)

```bash
python src/shortest_path_hybrid.py
```

**Best for:**
- **All resolution ranges** (automatically selects optimal algorithm)
- Default: Scipy for resolutions 0-10, Pure Spark for 11-15
- Customizable algorithm selection per resolution

```python
from src.shortest_path_hybrid import main

# Use default strategy
main(
    edges_file="data/burnaby_driving_simplified_edges_with_h3.csv",
    graph_file="data/burnaby_driving_edge_graph.csv",
    resolution_range=range(15, -1, -1)
)

# Custom algorithm selection
main(
    edges_file="data/burnaby_driving_simplified_edges_with_h3.csv",
    graph_file="data/burnaby_driving_edge_graph.csv",
    resolution_range=range(15, 7, -1),
    scipy_resolutions=[8, 9, 10],
    pure_spark_resolutions=[11, 12, 13, 14, 15],
    max_iterations=20
)
```

### Option 2: Scipy-Optimized Version (Recommended for Coarse Resolutions)

```bash
python src/shortest_path_scipy_spark.py
```

**Best for:**
- **Resolutions 0-10** (Coarser resolutions, larger spatial areas)
- Complex, dense local graphs
- When memory efficiency is critical (uses sparse matrices)

```python
from src.shortest_path_scipy_spark import main

main(
    edges_file="data/burnaby_driving_simplified_edges_with_h3.csv",
    graph_file="data/burnaby_driving_edge_graph.csv",
    resolution_range=range(10, -1, -1)
)
```

### Option 3: Pure Spark Version (Recommended for Fine Resolutions)

```bash
python src/shortest_path_pure_spark.py
```

**Best for:**
- **Resolutions 10-16** (Finer resolutions, smaller spatial areas)
- Extremely large partitions (> 100k rows)
- Environments with GPU acceleration (RAPIDS)

```python
from src.shortest_path_pure_spark import main

main(
    edges_file="data/burnaby_driving_simplified_edges_with_h3.csv",
    graph_file="data/burnaby_driving_edge_graph.csv",
    resolution_range=range(15, 10, -1),
    max_iterations=10
)
```

## 🧮 Algorithm Explanation

### Overview
Both implementations follow a hierarchical approach, but differ in how they compute shortest paths within each spatial partition:

- **Scipy Version**: Converts the partition's edge list into a **Sparse Matrix (CSR)** and uses **Dijkstra's Algorithm** (via `scipy.sparse.csgraph`). This is $O(E + V \log V)$ and highly efficient for road networks.
- **Pure Spark Version**: Uses **Iterative Self-Joins** (Floyd-Warshall style) in Spark SQL. This is better for massive datasets that don't fit in driver/executor memory but requires more shuffle operations.

### Implementation Steps
1.  **Load Data**: Read edges and compute costs (`length / maxspeed`).
2.  **Enrich**: Add spatial info (LCA resolution, via cell) to shortcuts.
3.  **Filter**: Process one H3 resolution at a time.
4.  **Partition**: Group shortcuts by their parent cell at the current resolution.
5.  **Compute**:
    -   *Scipy*: `applyInPandas` -> Build Graph -> Dijkstra -> Return Paths
    -   *Spark*: `join` -> `window` -> `filter` loop until convergence
6.  **Merge**: Update the main shortcuts table with newly found paths.

## � Output

After processing all resolutions, both implementations save the final shortcuts table to:

```
output/shortcuts_final/
```

The output is saved in **Parquet format** for efficient storage and fast querying. The shortcuts table contains:

- `incoming_edge`: Source edge ID
- `outgoing_edge`: Destination edge ID  
- `via_edge`: Intermediate edge used in the shortest path
- `cost`: Total cost (travel time) of the path

### Reading the Output

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadShortcuts").getOrCreate()
shortcuts = spark.read.parquet("output/shortcuts_final")
shortcuts.show()
```

### Analysis

Use the provided Jupyter notebook to analyze the shortcuts data:

```bash
jupyter notebook notebooks/analyze_shortcuts.ipynb
```

The notebook includes:
- Basic statistics (min, max, avg, median, percentiles)
- Cost distribution visualizations
- Edge connectivity analysis
- Via edge usage (critical junctions)
- Direct vs multi-hop path comparison
- Summary export to CSV

## 📊 Performance Guide

Based on empirical testing:

| Resolution Range | Recommended Approach | Reason |
|------------------|----------------------|--------|
| **10 - 16** (Fine) | **Pure Spark** | Partitions are numerous but small. Spark's overhead is managed, and it handles the sheer volume of partitions well. |
| **0 - 10** (Coarse) | **Scipy-Optimized** | Partitions become large and dense. Scipy's sparse matrix approach prevents OOM errors and is orders of magnitude faster than iterative joins. |

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/improvement`)
3. Commit changes (`git commit -am 'Add improvement'`)
4. Push to branch (`git push origin feature/improvement`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see LICENSE file for details.