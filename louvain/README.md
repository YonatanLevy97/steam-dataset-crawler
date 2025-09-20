# Louvain Community Detection for Steam Dataset

Complete implementation of Louvain community detection algorithm for analyzing Steam game similarity graphs.

The Louvain algorithm is a fast, modularity-based community detection method that is particularly well-suited for large networks. It provides an excellent alternative to Girvan-Newman with significantly better performance characteristics.

## 🚀 Quick Start

### Mac/Linux
```bash
# Run basic community detection
./scripts/run_louvain.sh --edges ./out/graph_runs/.../edges_top100.csv.gz --giant-only

# Analyze community features in detail  
python3 scripts/detailed_community_feature_analysis.py \
    --communities ./out/louvain_.../community_assignments.csv \
    --metadata ./out/dead_labels_enriched.csv
```

### Windows
```cmd
REM Run basic community detection
scripts\run_louvain.bat --edges .\out\graph_runs\...\edges_top100.csv.gz --giant-only

REM Analyze community features in detail
python scripts\detailed_community_feature_analysis.py ^
    --communities .\out\louvain_...\community_assignments.csv ^
    --metadata .\out\dead_labels_enriched.csv
```

## 📊 Key Differences from Girvan-Newman

| Feature | Girvan-Newman | Louvain |
|---------|---------------|---------|
| **Speed** | Slower (O(n³)) | **Much faster (O(n log n))** |
| **Memory** | High memory usage | **Lower memory usage** |
| **Output** | Hierarchical communities | Single-level communities |
| **Best for** | Small graphs, detailed analysis | **Large graphs, quick results** |
| **Parameters** | max-communities | resolution, random-seed |
| **Determinism** | Deterministic | Randomized (but reproducible with seed) |

## 🔧 Installation & Requirements

No additional requirements beyond the main project dependencies. Louvain is included in NetworkX >= 2.8.

For older NetworkX versions, optionally install:
```bash
pip install python-louvain
```

## 📝 Usage Examples

### Basic Community Detection
```bash
# Simple run with default parameters
./scripts/run_louvain.sh --edges ./out/graph_runs/.../edges_top100.csv.gz

# Focus on giant component only
./scripts/run_louvain.sh --edges ./out/graph_runs/.../edges_top100.csv.gz --giant-only
```

### Advanced Parameters
```bash
# Higher resolution for more detailed communities
./scripts/run_louvain.sh --edges ./out/graph_runs/.../edges_top100.csv.gz \
                         --resolution 1.5 --giant-only --kcore 3

# Quick test on subset of data
./scripts/run_louvain.sh --edges ./out/graph_runs/.../edges_top100.csv.gz \
                         --max-edges 50000 --max-nodes 500
```

### Feature Analysis Pipeline
```bash
# 1. Run community detection
./scripts/run_louvain.sh --edges ./out/graph_runs/.../edges_top100.csv.gz --giant-only

# 2. Detailed feature analysis
python3 scripts/detailed_community_feature_analysis.py \
    --communities ./out/louvain_.../community_assignments.csv \
    --metadata ./out/dead_labels_enriched.csv \
    --out-dir ./out/louvain_feature_analysis/

# 3. Create summary report
python3 scripts/community_feature_summary.py \
    --analysis ./out/louvain_feature_analysis/detailed_feature_analysis.json \
    --out-dir ./out/louvain_summary/
```

## 📁 Output Files

### Core Algorithm Output
- `community_assignments.csv` - Node-to-community mapping
- `community_stats.json` - Summary statistics
- `community_sizes.png` - Size distribution visualization
- `modularity_info.json` - Modularity score and parameters

### Feature Analysis Output
- `detailed_feature_analysis.json` - Complete feature breakdown
- `feature_summary_*.csv` - Summary tables by feature type
- `community_profiles.txt` - Human-readable community descriptions

## ⚙️ Parameters

### Core Parameters
- `--resolution` (default: 1.0) - Controls community size. Higher = smaller communities
- `--random-seed` (default: 42) - For reproducible results
- `--min-community-size` (default: 5) - Filter small communities

### Graph Filtering
- `--min-weight` (default: 0.7) - Minimum edge weight (cosine similarity)  
- `--giant-only` - Analyze only largest connected component
- `--kcore K` - Apply k-core decomposition filter
- `--max-nodes N` - Limit graph size (degree-based sampling)
- `--max-edges N` - Limit edges loaded (for testing)

## 🎯 When to Use Louvain vs Girvan-Newman

**Choose Louvain when:**
- Working with large graphs (>1000 nodes)
- Need quick results
- Want to experiment with different community granularities
- Memory is a concern
- Single-level communities are sufficient

**Choose Girvan-Newman when:**
- Working with smaller graphs (<500 nodes)
- Need hierarchical community structure
- Want deterministic results
- Have plenty of time and computational resources

## 📋 Complete Workflow

1. **Graph Generation** (main project)
   ```bash
   # Generate similarity graph from Steam data
   python3 graph_scripts/build_cosine_similarity_graph.py
   ```

2. **Community Detection** (this module)
   ```bash
   # Run Louvain algorithm
   ./louvain/scripts/run_louvain.sh --edges ./out/graph_runs/.../edges_top100.csv.gz --giant-only
   ```

3. **Feature Analysis**
   ```bash
   # Analyze what each community represents
   python3 louvain/scripts/detailed_community_feature_analysis.py \
       --communities ./out/louvain_.../community_assignments.csv \
       --metadata ./out/dead_labels_enriched.csv
   ```

4. **Results Summary**
   ```bash
   # Generate readable summary
   python3 louvain/scripts/community_feature_summary.py \
       --analysis ./out/louvain_feature_analysis/detailed_feature_analysis.json
   ```

## 🔬 Algorithm Details

The Louvain algorithm works in two phases:
1. **Local optimization**: Each node is moved to the community that gives the maximum increase in modularity
2. **Community aggregation**: Communities are treated as single nodes and the process repeats

**Resolution Parameter**: Controls the balance between internal community connections and external connections. Higher values favor smaller, more tightly-knit communities.

## 📚 Directory Structure

```
louvain/
├── README.md                 # This file
├── scripts/
│   ├── louvain_analysis.py   # Core algorithm implementation
│   ├── run_louvain.sh        # Main wrapper script (Unix)
│   ├── run_louvain.bat       # Main wrapper script (Windows)
│   ├── analyze_community_results.py          # Basic analysis
│   ├── detailed_community_feature_analysis.py # Comprehensive analysis  
│   └── community_feature_summary.py          # Summary reports
├── docs/
│   ├── README_louvain.md     # Detailed documentation
│   ├── LOUVAIN_SUMMARY.md    # Technical summary
│   └── DETAILED_FEATURE_ANALYSIS_SUMMARY.md
└── examples/
    ├── basic_usage.sh        # Simple examples
    ├── basic_usage.bat
    ├── quick_test.sh         # Testing examples
    └── quick_test.bat
```

## 🆘 Troubleshooting

### Common Issues

**Error: "No module named 'community'"**
```bash
pip install python-louvain
```

**Empty communities detected**
- Try lowering `--min-weight` (e.g., 0.5 instead of 0.7)  
- Use `--giant-only` to focus on main component
- Check that input graph has sufficient connectivity

**Communities too large/small**
- Adjust `--resolution` parameter (higher = smaller communities)
- Try different `--kcore` values to filter low-degree nodes
- Use `--max-nodes` to limit graph size for experimentation

**Performance issues**
- Use `--max-edges` and `--max-nodes` for testing
- Apply `--kcore` filter to reduce graph complexity
- Consider using `--giant-only` to focus analysis

## 🔗 Related Files

- `../LOUVAIN_VS_GIRVAN_NEWMAN.md` - Detailed algorithm comparison
- `../girvan_newman/` - Alternative Girvan-Newman implementation
- `../graph_scripts/` - Graph generation pipeline
- `../out/` - Generated datasets and results

## 📈 Performance Benchmarks

Approximate performance on typical Steam dataset:
- **1,000 nodes**: ~1-2 seconds
- **10,000 nodes**: ~10-30 seconds  
- **100,000 nodes**: ~2-10 minutes

Compare with Girvan-Newman: ~100x faster on large graphs.

---

For detailed documentation and advanced usage, see `docs/README_louvain.md`.