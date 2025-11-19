# Version Comparison Guide

## Quick Reference

| Feature | Pandas Version | Pure Spark Version |
|---------|----------------|--------------------|
| **File** | `shortest_path_pandas.py` | `shortest_path_spark.py` |
| **Execution** | `python shortest_path_pandas.py` | `python shortest_path_spark.py` |
| **Best For** | Small-medium partitions | Large partitions |
| **Partition Size** | < 100k rows | > 100k rows |
| **Speed** | ⚡ Faster (vectorized) | 🚀 Good (optimized) |
| **Memory** | 💾 Lower | 💾💾 Higher |
| **Learning Curve** | Easy | Moderate |
| **GPU Support** | Limited | Native |
| **Scalability** | Good | Excellent |

---

## Detailed Comparison

### 1. Architecture

#### Pandas Version
```
Spark Driver
    ↓
Read Edges ─→ Distribute to Workers
    ↓
Executors (each runs applyInPandas)
    ├─ Worker 1: Pandas operations on partition 1
    ├─ Worker 2: Pandas operations on partition 2
    └─ Worker N: Pandas operations on partition N
    ↓
Combine Results → Spark Driver
```

**Characteristics:**
- Data leaves Spark briefly for Pandas processing
- Each partition computed independently
- C-optimized vectorized operations
- Lower memory overhead per executor

#### Pure Spark Version
```
Spark Driver
    ↓
Read Edges ─→ Distribute to Workers
    ↓
Executors (Spark SQL operations)
    ├─ Worker 1: Window functions, joins on partition 1
    ├─ Worker 2: Window functions, joins on partition 2
    └─ Worker N: Window functions, joins on partition N
    ↓
Combine Results → Spark Driver
```

**Characteristics:**
- All operations in Spark ecosystem
- Native Catalyst optimizer
- More shuffle operations between iterations
- Better GPU acceleration support

---

### 2. Algorithm Implementation

#### Pandas: Floyd-Warshall with Pandas Merge
```python
# Per partition (on executor)
for iteration in range(max_iterations):
    # Self-join using Pandas merge
    new_paths = paths.merge(
        paths,
        left_on='outgoing_edge',
        right_on='incoming_edge',
        suffixes=('_L', '_R'),
        how='inner'
    )
    
    # Vectorized filtering and groupby
    updated_paths = combined.loc[
        combined.groupby(['incoming_edge', 'outgoing_edge'])['cost'].idxmin()
    ]
    
    # Convergence check with Pandas merge
    if converging_check(paths, updated_paths):
        break
    
    paths = updated_paths
```

**Advantages:**
- Fast vectorized operations
- Familiar Python syntax
- Easy to debug
- Lower memory for small partitions

#### Spark: Floyd-Warshall with Window Functions
```python
# Per partition and across iterations
for iteration in range(max_iterations):
    # Spark SQL self-join
    new_paths = current_paths.alias("L").join(
        current_paths.alias("R"),
        [L.outgoing == R.incoming, L.cell == R.cell],
        "inner"
    )
    
    # Window function for ranking
    window_spec = Window.partitionBy(
        "incoming", "outgoing", "cell"
    ).orderBy("cost")
    
    next_paths = all_paths.withColumn(
        "rnk", F.row_number().over(window_spec)
    ).filter(F.col("rnk") == 1)
    
    # Convergence tracking with LEFT ANTI-JOIN
    is_converged = update_convergence_status(...)
    
    if all_converged:
        break
```

**Advantages:**
- Catalyst query optimization
- Better for large data
- Native Spark scaling
- GPU acceleration ready

---

### 3. Memory Behavior

#### Pandas Version: Memory Profile
```
Per executor, per partition:
- Base Python + Pandas:           ~500 MB
- DataFrame copy:                 ~(rows × columns × 8 bytes)
- Working memory (merge, groupby): ~2× DataFrame size
- Total: ~500 MB + 3× data size

Example: 10k rows, 4 columns
- Data: 10k × 4 × 8 = 320 KB
- Working: ~1 MB
- Per executor: ~501 MB
```

**Memory-efficient for:**
- Small partitions (< 50k rows)
- Frequent context switches
- Limited executor memory (< 4GB)

#### Spark Version: Memory Profile
```
Per executor, per iteration:
- Spark overhead:                 ~200 MB
- Current paths:                  ~(rows × columns × 16 bytes)
- New paths:                       ~(rows × columns × 16 bytes)
- All paths (combined):            ~(rows × columns × 24 bytes)
- Total: ~200 MB + 4× data size (after broadcast)

Example: 10k rows, 4 columns
- Data: 10k × 4 × 16 = 640 KB
- New: 10k × 4 × 16 = 640 KB
- Combined: ~2.4 MB
- Per executor: ~202 MB
```

**Memory-efficient for:**
- Large partitions (> 100k rows)
- Few iterations needed
- Distributed caching

---

### 4. Performance Characteristics

#### Pandas Version: Vectorized Operations
```
Operation                    Time Complexity
─────────────────────────────────────────────
Read data                    O(n)
Pandas merge (join)          O(n log n) → O(k²)
Groupby + idxmin             O(n log n)
Convergence check            O(n)
Per iteration:               O(k² × log k) where k = new paths

Total for partition:         O(iterations × k² × log k)
```

**Performance optimized for:**
- < 100k rows per partition
- Complex path extensions
- I/O bound operations

#### Spark Version: Distributed Operations
```
Operation                    Network Cost
─────────────────────────────────────────────
Read data                    O(1) on local executor
Spark join                   O(shuffle) - data redistribution
Window function              O(partition sort)
Convergence check            O(shuffle) - anti-join
Per iteration:               O(shuffle + sort)

Total for partition:         O(iterations × shuffle_cost)
```

**Performance optimized for:**
- > 100k rows per partition
- Shallow iteration depths
- Network-fast environments

---

### 5. Scalability

#### Pandas Version Scaling
```
Cluster Size    Per-Partition Speed    Total Time
──────────────────────────────────────────────────
1 node          ████                   ████████████████
2 nodes         ████                   ████████
4 nodes         ████                   ████
8 nodes         ████                   ██ (diminishing)
```

**Scales linearly** with number of partitions (H3 cells)
**Does NOT benefit** much from more executors per partition

#### Spark Version Scaling
```
Cluster Size    Per-Partition Speed    Total Time
──────────────────────────────────────────────────
1 node          ██                     ████████████████
2 nodes         ███                    █████████
4 nodes         ████                   ███
8 nodes         █████                  ██
```

**Scales with:**
- Number of nodes (for distributed processing)
- GPU availability (for native acceleration)
- Network bandwidth (for shuffle operations)

---

### 6. When to Use Each Version

#### Use Pandas Version When:
✅ Road network has < 500k edges  
✅ Partitions typically < 50k rows  
✅ Quick prototyping/development  
✅ Single machine or small cluster (2-4 nodes)  
✅ Limited executor memory (< 4GB)  
✅ You want fastest iteration speed  
✅ Familiar with Python/Pandas  

**Example scenarios:**
- City-level routing (single city)
- Testing algorithm changes
- Development environment
- Performance profiling

#### Use Pure Spark Version When:
✅ Road network has > 1M edges  
✅ Partitions typically > 100k rows  
✅ Production deployment  
✅ Large cluster (8+ nodes)  
✅ GPU acceleration available  
✅ Complex multi-region routing  
✅ Familiar with Spark/SQL  

**Example scenarios:**
- National-scale routing
- Multi-region transportation network
- Real-time continuous updates
- GPU-accelerated infrastructure

---

### 7. Switching Between Versions

#### From Pandas to Spark

If Pandas version is running out of memory:

```python
# Replace this:
from shortest_path_pandas import main
main(edges_file, graph_file)

# With this:
from shortest_path_spark import main
main(edges_file, graph_file)
```

**No input data changes needed!** Both versions use the same utility functions.

#### From Spark to Pandas

If Pure Spark version is too slow on small data:

```python
# Replace this:
from shortest_path_spark import main
main(edges_file, graph_file, max_iterations=20)

# With this:
from shortest_path_pandas import main
main(edges_file, graph_file)
```

---

### 8. Convergence Behavior

#### Pandas Version Convergence
```
Iteration 0: 5000 paths → 5500 new paths
Iteration 1: 5500 paths → 500 new paths
Iteration 2: 5500 paths → 50 new paths
Iteration 3: 5500 paths → 0 new paths ✓ Converged
```

Convergence detection via Pandas merge:
- Compares row-by-row
- Fast for small datasets
- May check ALL rows unnecessarily

#### Spark Version Convergence
```
Iteration 0: 5000 paths (is_converged=False)
Iteration 1: 5500 paths (is_converged=False for changed cells)
Iteration 2: 5500 paths (is_converged=False for 2 cells)
Iteration 3: 5500 paths (is_converged=True for all) ✓ Converged
```

Convergence tracking via LEFT ANTI-JOIN:
- Tracks per H3 cell
- Efficient for multi-region data
- Only processes non-converged cells

---

### 9. Debugging Guide

#### Pandas Version Debugging
```python
# Easy: Print intermediate DataFrames
print(f"Before merge: {len(paths)} rows")
print(f"After merge: {len(new_paths)} rows")
print(f"Converged: {converging_check(paths, updated_paths)}")

# Access DataFrame directly in process_partition_pandas()
pdf.to_csv(f"debug_iteration_{i}.csv")
```

#### Spark Version Debugging
```python
# Using Spark UI (localhost:4040)
# Check:
# - Number of tasks per stage
# - Shuffle size
# - Execution time per stage

# Programmatic debugging:
shortcuts_df_new.explain(mode="extended")  # Show query plan
shortcuts_df_new.show(10)  # Display sample data
```

---

### 10. Migration Path

**For existing projects:**

1. **Start with Pandas** for development (faster iteration)
2. **Test with Pure Spark** as data grows
3. **Monitor metrics** (memory, time per iteration)
4. **Switch when needed:**
   - If Pandas uses > 80% available memory → Switch to Spark
   - If iteration time > 5x Spark time → Reconsider architecture

**No breaking changes** between versions - same input, same output!

---

## Summary Matrix

| Criterion | Winner | Reason |
|-----------|--------|--------|
| Development Speed | Pandas | Faster iteration, easier debugging |
| Scalability | Spark | Better distributed optimization |
| Memory Efficiency (small) | Pandas | Lower overhead |
| Memory Efficiency (large) | Spark | Better caching strategy |
| Learning Curve | Pandas | Familiar Python/Pandas |
| Production Readiness | Spark | Native Spark ecosystem |
| GPU Support | Spark | Native integration |
| Single Machine | Pandas | Simpler deployment |
| Multi-Machine Cluster | Spark | Better scaling |

---

**Recommendation for New Users:**
Start with **Pandas version** on your local machine to understand the algorithm, then switch to **Pure Spark version** for production deployments with larger data.
