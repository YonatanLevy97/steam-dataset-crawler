# Community Profiling System - Quick Summary

## What This System Does

🎯 **Purpose**: Test how well we can predict which community an unseen game belongs to based on its features and community profiles created from training data only.

## Key Innovation

Instead of running Girvan-Newman on all games, this system:
1. **Splits** games into train (80%) and test (20%) sets
2. **Runs Girvan-Newman** only on training games to find communities  
3. **Creates profiles** by averaging feature vectors within each community
4. **Tests assignment** of unseen games using cosine similarity with community profiles
5. **Evaluates** how well the system performed

## Quick Start

```bash
# 1. Install dependencies
pip install numpy pandas scipy networkx matplotlib

# 2. Run experiment (uses existing feature matrices and edges)
./run_community_profiling_experiment.sh --edges out/graph_runs/.../edges_top100.csv.gz

# 3. Analyze results  
python analyze_profiling_results.py --results-dir out/community_profiling_experiment_TIMESTAMP
```

## What You Get

### Output Files
- **Train/test split info**: Which games were used for training vs testing
- **Community assignments**: Communities found in training data only
- **Community profiles**: Average feature vectors for each community
- **Test assignments**: Where unseen games were assigned + similarity scores
- **Evaluation metrics**: Performance analysis and visualizations

### Key Metrics
- **Similarity Scores**: How well test games matched their assigned communities
- **Coverage**: What % of communities received test game assignments  
- **Balance**: Whether assignments were evenly distributed across communities

### Success Indicators
- **High similarity scores** (>0.7): Good community-game matching
- **High coverage** (>80%): Communities are broadly applicable to unseen games
- **Balanced distribution**: Assignment rates similar to training community sizes

## Use Cases

1. **Validate Communities**: Do communities make sense for unseen data?
2. **Test Generalization**: How well do communities generalize beyond training data?
3. **Feature Analysis**: Which features drive community assignment?
4. **Recommendation Systems**: Assign new games to existing communities for recommendations

## Files Created

| File | Description |
|------|-------------|
| `community_profiling_system.py` | Main implementation |
| `run_community_profiling_experiment.sh` | Convenient wrapper script |
| `analyze_profiling_results.py` | Results analysis tool |
| `demo_community_profiling.py` | Quick demonstration script |
| `requirements_community_profiling.txt` | Python dependencies |
| `COMMUNITY_PROFILING_README.md` | Detailed documentation |

## Example Results

```
Community Profiling Experiment Results:
- Train games: 12,879
- Test games: 3,220  
- Communities detected: 8
- Average similarity: 0.7543
- Community coverage: 87.5%
- High-quality assignments (≥0.8): 1,834 (57.0%)
```

## Integration with Existing Pipeline

This seamlessly integrates with your current Steam dataset workflow:

```bash
# Existing pipeline
python build_feature_vectors.py --in data/enriched_games.csv --out-dir data/features/games_matrix
./run_full_cosine_graph_v3.sh --features data/features/games_matrix/X_csr.npz

# New community profiling step  
./run_community_profiling_experiment.sh --edges out/graph_runs/.../edges_top100.csv.gz
```

## Research Value

This system enables you to:
- **Quantify** community quality with unseen data
- **Compare** different community detection parameters objectively
- **Understand** which game features drive community membership
- **Build** recommendation systems based on validated communities

---

**Ready to start?** Run `python demo_community_profiling.py` for a quick test!