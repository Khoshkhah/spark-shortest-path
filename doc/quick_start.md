# Quick Start Guide

Get started with the Shortest Path Computation project in 5 minutes!

## ⚡ 30-Second Setup

```bash
# 1. Clone repository
git clone https://github.com/yourusername/shortest-path-spark.git
cd shortest-path-spark

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Set Java home (Linux/Mac)
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# 5. Run (choose one)
python shortest_path_pandas.py    # Pandas version
# OR
python shortest_path_spark.py     # Pure Spark version
```

## 🎯 First Run Expected Output

```
Loading edge data...
Computing edge costs...
Creating initial shortcuts table...

============================================================
Processing resolution 14
============================================================
✓ Iteration 0: Processed shortest paths
✓ Iteration 1: Processed shortest paths
✓ Iteration 2: No new paths found. Converged.

✓ Generated 97963 shortcuts at resolution 14

============================================================
Shortest path computation completed!
============================================================
```

## 📊 Choose Your Version

### ✅ Start with Pandas (Recommended for beginners)
```bash
python shortest_path_pandas.py
```
- Faster startup
- Easier to understand
- Good for testing
- Perfect for small-medium datasets

### 🚀 Use Pure Spark (For production)
```bash
python shortest_path_spark.py
```
- Better for large datasets
- Production-ready
- GPU support
- Better scalability

## 🔧 Custom Configuration

Edit `config.py` to customize:

```python
# Change resolution range
RESOLUTION_RANGE = range(15, 10, -1)  # Process res 15 down to 10

# Increase driver memory
SPARK_DRIVER_MEMORY = "16g"

# Change data files
EDGES_FILE = "data/your_edges.csv"
GRAPH_FILE = "data/your_graph.csv"
```

Then run:
```bash
python shortest_path_pandas.py  # Picks up config changes
```

## 📈 Monitor Progress

### Check Spark UI
While running, open in browser:
```
http://localhost:4040
```

Monitor:
- Number of tasks
- Shuffle size
- Execution time

### Enable Verbose Output
```python
# In shortest_path_pandas.py or shortest_path_spark.py, change:
verbose = True
```

## 🆘 Common Issues & Solutions

### Issue: "Java not found"
```bash
# Fix: Set JAVA_HOME correctly
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
echo $JAVA_HOME  # Verify it's set
```

### Issue: "No module named pyspark"
```bash
# Fix: Install dependencies
pip install -r requirements.txt
```

### Issue: "Out of memory"
```bash
# Fix: Use Pandas version with smaller resolution range
# In config.py:
RESOLUTION_RANGE = range(14, 13, -1)  # Just one resolution
```

### Issue: "Slow execution"
```bash
# Check which version to use:
# - Small dataset (< 100k rows): Use Pandas
# - Large dataset (> 100k rows): Use Spark
```

## 📚 Next Steps

1. **Understand the algorithm**: Read [VERSION_COMPARISON.md](VERSION_COMPARISON.md)
2. **Explore your data**: Check sample edges in `data/` folder
3. **Experiment with resolutions**: Modify `config.py` values
4. **Run development notebooks**: `jupyter notebook notebooks/`
5. **Scale to production**: Prepare your own data and follow [README.md](README.md)

## 📝 Using Your Own Data

### 1. Prepare Edges CSV
Required columns:
```csv
id,incoming_cell,outgoing_cell,lca_res,length,maxspeed
edge_001,8f28de881760406,8f28de881760407,15,150.5,50
edge_002,8f28de881760407,8f28de881760408,15,200.0,60
...
```

### 2. Prepare Graph CSV
Required columns:
```csv
incoming_edge,outgoing_edge
edge_001,edge_002
edge_002,edge_003
...
```

### 3. Update Config
```python
EDGES_FILE = "data/your_edges.csv"
GRAPH_FILE = "data/your_graph.csv"
```

### 4. Run
```bash
python shortest_path_pandas.py
```

## 🎓 Learning Path

**Beginner (0-1 hour):**
- ✓ Run this quick start
- ✓ View generated shortcuts
- ✓ Understand version differences

**Intermediate (1-4 hours):**
- ✓ Read README.md algorithm explanation
- ✓ Study VERSION_COMPARISON.md
- ✓ Run with different resolutions
- ✓ Modify config.py settings

**Advanced (4+ hours):**
- ✓ Study notebooks (development versions)
- ✓ Understand H3 geospatial indexing
- ✓ Modify shortest path algorithm
- ✓ Deploy on Spark cluster
- ✓ Add GPU acceleration

## 💡 Tips & Tricks

### Run Just One Resolution
```bash
# Modify config.py:
RESOLUTION_RANGE = range(14, 14, -1)  # Just resolution 14
```

### Compare Pandas vs Spark
```bash
# Run Pandas version
time python shortest_path_pandas.py

# Run Spark version
time python shortest_path_spark.py

# Compare timing
```

### Debug Convergence
```python
# Add to shortest_path_pandas.py around line 150:
print(f"Iteration {iteration}:")
print(f"  Old paths: {len(paths)}")
print(f"  New paths: {len(new_paths)}")
print(f"  Converged: {converging_check(paths, updated_paths)}")
```

### Save Intermediate Results
```python
# After line 200 in shortest_path_pandas.py:
shortcuts_df_new.write.parquet(
    f"output/resolution_{current_resolution}.parquet",
    mode="overwrite"
)
```

## 🔗 Resources

- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [H3 Geospatial Indexing](https://h3geo.org/)
- [Floyd-Warshall Algorithm](https://en.wikipedia.org/wiki/Floyd%E2%80%93Warshall_algorithm)

## 📞 Need Help?

1. Check [README.md](README.md) - Full documentation
2. Check [VERSION_COMPARISON.md](VERSION_COMPARISON.md) - Version differences
3. Look in `notebooks/` - See original development versions
4. Open GitHub Issue - Report problems

## ✨ Success Checklist

- ✅ Python 3.12+ installed
- ✅ Java 21 configured
- ✅ Dependencies installed (`pip install -r requirements.txt`)
- ✅ Data files present (`data/burnaby_*.csv`)
- ✅ First test run completed
- ✅ Understood version differences

**You're ready to go! 🚀**

---

**Last Updated:** November 2025  
**Status:** Verified working ✓
