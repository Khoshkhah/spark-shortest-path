# Hierarchical Shortest Path Computation using PySpark and H3 Geospatial Indexing

A distributed computing solution for computing all-pairs shortest paths on large-scale road networks using Apache Spark and hierarchical H3 geospatial indexing.

## 🎯 Overview

This project implements a scalable shortest path algorithm that leverages:

- **PySpark**: Distributed computation across multiple nodes
- **H3 Geospatial Indexing**: Hierarchical spatial partitioning for efficient processing
- **Floyd-Warshall Algorithm**: Iterative path extension until convergence
- **Multi-resolution Processing**: Processes road networks at different H3 resolution levels

### Key Features

✅ **Hierarchical Processing**: Processes roads at different spatial granularities (H3 resolutions 8-15)
✅ **Distributed Computation**: Scales across Spark clusters
✅ **Memory Efficient**: Checkpointing and caching strategies to manage large datasets
✅ **Convergence Detection**: Stops iterations when no improvements are found
✅ **Spatial Partitioning**: Groups shortcuts by H3 cells for independent processing

## 📋 Prerequisites

- Python 3.12+
- Apache Spark 3.x
- Java 21+
- PySpark
- h3-py
- pandas (for data analysis)

## 🚀 Installation

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/shortest-path-spark.git
cd shortest-path-spark
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
├── utilities.py                     # Shared utility functions (data loading, H3 operations)
├── shortest_path_pandas.py          # Pandas-optimized implementation
├── shortest_path_spark.py           # Pure Spark SQL implementation
├── config.py                        # Configuration parameters
├── requirements.txt                 # Python dependencies
├── data/
│   ├── burnaby_driving_simplified_edges_with_h3.csv    # Edge data
│   └── burnaby_driving_edge_graph.csv                   # Edge graph
├── notebooks/
│   ├── first_pandas_ver.ipynb       # Original development notebook (Pandas)
│   └── first_pure_spark_ver.ipynb   # Original development notebook (Pure Spark)
└── README.md                        # This file
```

## 📊 Input Data Format

### Edges CSV (`burnaby_driving_simplified_edges_with_h3.csv`)

Required columns:
- `id`: Unique edge identifier
- `incoming_cell`: H3 cell ID for incoming node
- `outgoing_cell`: H3 cell ID for outgoing node
- `lca_res`: Lowest common ancestor resolution
- `length`: Road segment length
- `maxspeed`: Maximum speed limit

### Edge Graph CSV (`burnaby_driving_edge_graph.csv`)

Required columns:
- `incoming_edge`: Source edge ID
- `outgoing_edge`: Destination edge ID

## 🔧 Configuration

Edit `config.py` to customize parameters:

```python
# Spark Configuration
SPARK_DRIVER_MEMORY = "8g"
SPARK_EXECUTOR_MEMORY = "4g"

# File Paths
EDGES_FILE = "data/burnaby_driving_simplified_edges_with_h3.csv"
GRAPH_FILE = "data/burnaby_driving_edge_graph.csv"

# H3 Resolution Range
MIN_RESOLUTION = 8
MAX_RESOLUTION = 14

# Computation
MAX_ITERATIONS_PER_PARTITION = 10
```

## ▶️ Usage

### Option 1: Pandas-Optimized Version (Recommended for small-medium partitions)

```bash
python shortest_path_pandas.py
```

**When to use:**
- Partition size: < 100k rows
- Faster for complex path computations
- Better performance with vectorized operations
- Development and testing

```python
from shortest_path_pandas import main

main(
    edges_file="data/burnaby_driving_simplified_edges_with_h3.csv",
    graph_file="data/burnaby_driving_edge_graph.csv",
    resolution_range=range(14, 8, -1)
)
```

### Option 2: Pure Spark Version (Recommended for large partitions)

```bash
python shortest_path_spark.py
```

**When to use:**
- Partition size: > 100k rows
- GPU acceleration needed
- Native Spark ecosystem required
- Large-scale production deployments

```python
from shortest_path_spark import main

main(
    edges_file="data/burnaby_driving_simplified_edges_with_h3.csv",
    graph_file="data/burnaby_driving_edge_graph.csv",
    resolution_range=range(14, 8, -1),
    max_iterations=10
)
```

### Using Jupyter Notebooks (Development)

For interactive exploration with original notebooks:

```bash
# Pandas-based development notebook
jupyter notebook notebooks/first_pandas_ver.ipynb

# Pure Spark development notebook
jupyter notebook notebooks/first_pure_spark_ver.ipynb
```

## 🧮 Algorithm Explanation

### Overview
Both implementations follow the same hierarchical Floyd-Warshall algorithm, with different execution strategies:

- **Pandas Version**: Optimized for small-medium partitions using vectorized operations
- **Pure Spark Version**: Optimized for large partitions using native Spark SQL

### Implementation Steps (Same for both versions)
- Load edge data and compute costs: `cost = length / maxspeed`
- Create initial shortcuts table with direct edge connections

### 2. **Spatial Enrichment**
- For each shortcut, compute:
  - `via_cell`: Lowest Common Ancestor of incoming and outgoing cells
  - `lca_res`: Resolution of the LCA
  - `via_res`: Resolution of the via_cell

### 3. **Resolution-based Filtering**
- At each H3 resolution level, filter shortcuts where:
  - `lca_res <= current_resolution`
  - `via_res >= current_resolution`

### 4. **Spatial Partitioning**
- Group shortcuts by their parent cell at current resolution
- This enables independent shortest path computation per spatial region

### 5. **Shortest Path Computation (per partition)**
- **Iteration**: For each iteration
  - **Join**: If A→B and B→C exist, create path A→C with cost = cost(A→B) + cost(B→C)
  - **Minimize**: Keep only the minimum cost path for each (source, destination) pair
  - **Filter**: Remove self-loops
  - **Check**: If no new paths found or no improvements made, converge

### 6. **Merging Results**
- Combine computed shortcuts back into the main table
- Replace old paths with newly computed ones

## 📊 Performance Comparison

### Pandas Version
| Metric | Value |
|--------|-------|
| Partition size | < 100k rows |
| Execution speed | Faster (vectorized) |
| Memory usage | Lower |
| Scalability | Good for small-medium |
| GPU support | Limited |
| Code complexity | Lower |

### Pure Spark Version
| Metric | Value |
|--------|-------|
| Partition size | > 100k rows |
| Execution speed | Good (optimized joins) |
| Memory usage | Higher (more shuffles) |
| Scalability | Excellent for large |
| GPU support | Native |
| Code complexity | Higher |

### Choosing the Right Version

**Use Pandas Version if:**
- Your road network has < 500k edges
- Partitions typically have < 50k rows each
- You want faster development iteration
- Single-machine or small cluster

**Use Pure Spark Version if:**
- Your road network has > 1M edges
- Partitions typically have > 100k rows
- You need GPU acceleration
- Large multi-node production cluster

### Benchmark Results (Burnaby, BC)

```
Dataset: ~2000 edges, ~3000 shortcuts
Resolution 14 (Pandas):   ~5-10 seconds
Resolution 14 (Spark):    ~8-15 seconds
```

## 🔍 Example Output

```
============================================================
Processing resolution 14
============================================================
Loading edge data...
Computing edge costs...
Creating initial shortcuts table...

✓ Iteration 0: Processed shortest paths
✓ Iteration 1: Processed shortest paths
✓ Iteration 2: No new paths found. Converged.

✓ Generated 97963 shortcuts at resolution 14

============================================================
Shortest path computation completed successfully!
============================================================
```

## 📝 API Reference

### Shared Utilities (`utilities.py`)

Core functions used by both implementations:

#### `initialize_spark(app_name, driver_memory) → SparkSession`
Initialize and configure PySpark session.

#### `read_edges(spark, file_path) → DataFrame`
Load edges from CSV file.

#### `add_info_for_shortcuts(spark, shortcuts_df, edges_df) → DataFrame`
Enrich shortcuts with spatial information (via_cell, lca_res, via_res).

#### `filter_shortcuts_by_resolution(shortcuts_df, current_res) → DataFrame`
Filter shortcuts based on H3 resolution.

#### `add_parent_cell_at_resolution(shortcuts_df, current_resolution) → DataFrame`
Add spatial partition key (current_cell) to shortcuts.

#### `merge_shortcuts_to_main_table(main_df, new_shortcuts) → DataFrame`
Merge newly computed shortcuts back into main table.

### Pandas Version (`shortest_path_pandas.py`)

Partition-level shortest path computation using Pandas:

#### `compute_shortest_paths_per_partition(df_shortcuts, partition_columns) → DataFrame`
Compute shortest paths using vectorized Pandas operations per partition.

#### `converging_check(old_paths, new_paths) → bool`
Check if path set has converged using Pandas merge.

### Pure Spark Version (`shortest_path_spark.py`)

Partition-level shortest path computation using Spark SQL:

#### `run_grouped_shortest_path_with_convergence(shortcuts_df, max_iterations) → DataFrame`
Compute shortest paths using pure Spark SQL self-joins and window functions.

#### `update_convergence_status(shortcuts_df_last, shortcuts_df_new) → DataFrame`
Mark spatial partitions as converged based on content changes.

## 🐛 Troubleshooting

### Issue: "Service 'SparkUI' could not bind on port 4040"
**Solution**: Multiple Spark sessions are running. Ports are auto-incremented (4041, 4042, etc.)

### Issue: "Out of Memory"
**Solution**: 
- Increase driver memory: `initialize_spark(driver_memory="16g")`
- Use Pandas version with smaller partitions
- Increase number of partitions

### Issue: "H3 cell resolution mismatch"
**Solution**: Ensure all H3 cells in input data use consistent resolutions (typically 15).

### Pandas Version Slow or Hanging
**Possible causes and solutions:**
1. Partition too large → Increase number of H3 resolution levels
2. Max iterations too high → Reduce `max_iterations` parameter
3. Memory pressure → Use Pure Spark version instead

### Pure Spark Version High Memory Usage
**Possible causes and solutions:**
1. Large shuffle operations → Use Pandas version for smaller partitions
2. Many iterations → Algorithm may not be converging; check input data
3. Caching too many DataFrames → Explicitly unpersist unused DataFrames

### Version Comparison for Debugging
If you're unsure which version to use:
1. Start with Pandas version (faster iteration)
2. If it runs out of memory or is slow, switch to Pure Spark
3. Check output log files for convergence patterns

## 📊 Development Versions (Jupyter Notebooks)

### `notebooks/first_pandas_ver.ipynb`
Original development notebook implementing the Pandas-optimized approach.
- Uses `applyInPandas` for partition-level computation
- Includes convergence checking logic
- Good for understanding the algorithm step-by-step
- Mix of Spark and Pandas operations

### `notebooks/first_pure_spark_ver.ipynb`
Original development notebook implementing the pure Spark approach.
- Uses Spark SQL joins and window functions
- Includes convergence status tracking
- Demonstrates complex Spark optimization techniques
- All operations in Spark

### Running Development Notebooks
```bash
jupyter notebook notebooks/
```

Key differences between notebooks:
| Aspect | Pandas Notebook | Spark Notebook |
|--------|-----------------|----------------|
| Partition processing | applyInPandas | groupBy().join() |
| Convergence check | Pandas merge | LEFT ANTI-JOIN |
| Optimization focus | Vectorization | SQL planning |
| Debugging | Easier (familiar Pandas) | Advanced (Spark DAG) |

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/improvement`)
3. Commit changes (`git commit -am 'Add improvement'`)
4. Push to branch (`git push origin feature/improvement`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see LICENSE file for details.

## 👨‍💻 Author

[Your Name]  
[Your Email]  
[Your GitHub Profile]

## 🔗 References

- [H3 Documentation](https://h3geo.org/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Floyd-Warshall Algorithm](https://en.wikipedia.org/wiki/Floyd%E2%80%93Warshall_algorithm)
- [Apache Spark Best Practices](https://spark.apache.org/docs/latest/sql-performance-tuning.html)

## 📞 Support

For issues, questions, or suggestions:
- Open an Issue on GitHub
- Check existing documentation
- Review the development notebooks for examples

---

**Last Updated**: November 2025  
**Status**: Production Ready