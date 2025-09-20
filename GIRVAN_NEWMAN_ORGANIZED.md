# ✅ Girvan-Newman Files Organized

All Girvan-Newman community detection files have been moved to the `girvan_newman/` folder for better organization.

## 📁 New Directory Structure

```
girvan_newman/
├── README.md                              # Main overview and usage
├── scripts/                               # All executable scripts
│   ├── run_girvan_newman.sh                   # 🚀 Main wrapper script  
│   ├── girvan_newman_analysis.py              # Core algorithm implementation
│   ├── detailed_community_feature_analysis.py # 📊 Comprehensive feature analysis
│   ├── community_feature_summary.py           # Readable summaries
│   ├── analyze_community_results.py           # Basic analysis (legacy)
│   ├── run_girvan_newman_binary.sh            # Binary dead/alive classification
│   ├── interpret_binary_communities.py        # Binary result interpretation  
│   └── optimize_binary_separation.sh          # Parameter optimization
├── docs/                                  # Documentation
│   ├── README_girvan_newman.md                # 📖 Complete usage guide
│   ├── GIRVAN_NEWMAN_SUMMARY.md               # Implementation summary
│   ├── DETAILED_FEATURE_ANALYSIS_SUMMARY.md  # Feature analysis guide
│   └── BINARY_DEAD_ALIVE_GUIDE.md             # Binary classification guide
└── examples/                              # Example usage scripts
    ├── basic_usage.sh                         # Complete workflow example
    └── quick_test.sh                          # Fast test with small data
```

## 🚀 How to Use (Updated Paths)

### Main Community Detection
```bash
cd girvan_newman
./scripts/run_girvan_newman.sh --edges ../out/graph_runs/.../edges_top100.csv.gz --giant-only
```

### Detailed Feature Analysis
```bash
cd girvan_newman
python3 scripts/detailed_community_feature_analysis.py \
    --communities ../out/girvan_newman_.../community_assignments_best.csv \
    --metadata ../out/dead_labels_enriched.csv \
    --out-dir ../out/detailed_analysis
```

### Create Readable Summaries
```bash
cd girvan_newman  
python3 scripts/community_feature_summary.py \
    --analysis ../out/detailed_analysis/detailed_feature_analysis.json \
    --out-dir ../out/community_summary
```

### Run Examples
```bash
cd girvan_newman
./examples/quick_test.sh          # Fast test
./examples/basic_usage.sh         # Complete workflow
```

## 📖 Documentation

All documentation is now in `girvan_newman/docs/`:

- **[Main README](girvan_newman/README.md)** - Overview and quick start
- **[Complete Guide](girvan_newman/docs/README_girvan_newman.md)** - Detailed usage with all options
- **[Implementation Summary](girvan_newman/docs/GIRVAN_NEWMAN_SUMMARY.md)** - Technical details and results
- **[Feature Analysis Guide](girvan_newman/docs/DETAILED_FEATURE_ANALYSIS_SUMMARY.md)** - Understanding community characteristics

## 🎯 What You Get

The organized system provides:

1. **Clean community detection** with NetworkX Girvan-Newman algorithm
2. **Detailed feature analysis** showing percentage of games with each feature value in each community
3. **Multiple output formats** (JSON, CSV, visualizations)
4. **Integration ready** with your existing Steam dataset pipeline
5. **Well documented** with examples and guides

## 🔧 Key Scripts You'll Use

### Essential (90% of use cases)
- **`scripts/run_girvan_newman.sh`** - Main community detection
- **`scripts/detailed_community_feature_analysis.py`** - Feature analysis

### Helpful
- **`scripts/community_feature_summary.py`** - Readable summaries
- **`examples/quick_test.sh`** - Test with small data

### Optional/Advanced
- **`scripts/run_girvan_newman_binary.sh`** - Binary dead/alive classification  
- **`scripts/optimize_binary_separation.sh`** - Parameter optimization
- **`scripts/interpret_binary_communities.py`** - Binary interpretation

## 🚀 Ready to Use!

The system is fully organized and ready for production use. All paths are updated and tested to work from the new directory structure.

Start with:
```bash
cd girvan_newman
./examples/quick_test.sh
```

This will run a fast test to make sure everything works correctly! 🎉