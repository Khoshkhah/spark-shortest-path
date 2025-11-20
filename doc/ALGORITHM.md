# Hybrid Bi-directional Shortest Path Algorithm

This document details the algorithm used in the `spark-shortest-path` project. The system computes all-pairs shortest paths on large-scale geospatial graphs using a resolution-based, divide-and-conquer approach.

## Core Concept

The algorithm leverages **H3 Geospatial Indexing** to partition the graph into manageable sub-problems. Instead of computing the entire graph at once, it iteratively processes the graph at different H3 resolution levels (from fine to coarse and back).

### The "Shortcut" Model
The core data structure is a **Shortcut**. A shortcut represents a path between two edges in the graph.
-   **Initial State**: Direct connections between adjacent edges (cost = edge weight).
-   **Iterative Step**: If we have a path $A \to B$ and $B \to C$, we can create a shortcut $A \to C$ with $cost = cost(A \to B) + cost(B \to C)$.

## Algorithm Phases

The computation is performed in three distinct phases:

### 1. Downward Pass (Resolution 15 $\to$ 0)
**Goal**: Compute local shortest paths within increasingly larger spatial regions.

-   **Iterates** from resolution 15 (finest) down to 0 (coarsest).
-   **Partitioning**: At each resolution $R$, the graph is partitioned by H3 cells at resolution $R$.
-   **Computation**: For each partition (cell), we load all shortcuts contained within that cell (where the Lowest Common Ancestor of the start and end points is at or below $R$).
-   **Result**: We compute the transitive closure (all-pairs shortest paths) *restricted to that cell*. This creates "shortcuts" that skip over intermediate nodes within the cell.

### 2. Global Optimization (Resolution -1)
**Goal**: Connect the disparate base cells (Resolution 0) to form a globally connected graph.

-   **Input**: All shortcuts that cross boundaries of Resolution 0 cells (LCA resolution $\le$ 0).
-   **Execution**: These are grouped into a single "global" partition.
-   **Result**: High-level shortcuts connecting distant regions of the map.

### 3. Upward Pass (Resolution 0 $\to$ 15)
**Goal**: Propagate global optimality back down to local levels.

-   **Iterates** from resolution 0 up to 15.
-   **Logic**: Now that we have global shortcuts, we re-examine local regions. A local path that seemed optimal in Phase 1 might now be improved by a "global" shortcut that routes through a faster highway or different region.
-   **Result**: The final set of shortcuts represents the true shortest paths across the entire graph.

## Hybrid Execution Strategy

To optimize performance and scalability, the system dynamically selects the best execution engine for each resolution:

| Resolution Range | Characteristics | Engine | Reason |
| :--- | :--- | :--- | :--- |
| **Fine (11-15)** | Many partitions, small data per partition. | **Pure Spark** | Spark's overhead for starting Python workers (Scipy) is too high. Spark SQL is efficient for many small tasks. |
| **Coarse (0-10)** | Fewer partitions, large dense graphs per partition. | **Scipy** | Scipy's `csgraph` (Dijkstra/Johnson) is orders of magnitude faster than Spark SQL for dense matrix operations. |
| **Global (-1)** | Single massive partition. | **Scipy** | Requires efficient in-memory graph algorithms. |

## Data Flow

1.  **Edges Input**: Raw road network edges.
2.  **Initial Shortcuts**: Convert edges to initial shortcuts.
3.  **Enrichment**: Add H3 spatial info (LCA, Via Cell) to shortcuts.
4.  **Loop (Phases 1, 2, 3)**:
    -   **Filter**: Select shortcuts relevant to current resolution.
    -   **Partition**: Group by parent cell.
    -   **Compute**: Run Scipy or Spark SQL to find new shortcuts.
    -   **Merge**: Update the main shortcuts table with found paths.
5.  **Output**: Final table of all-pairs shortest paths.
