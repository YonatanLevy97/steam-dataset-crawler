# Cosine Similarity Analysis System

A comprehensive, reusable system for calculating cosine similarity between Steam games and community profiles using the established `graph_scripts` methodology.

## Overview

This analysis system determines how closely "dead games" (games with consistently low player engagement) resemble successful community archetypes on Steam. Using L2-normalized sparse feature vectors and cosine similarity, it provides quantitative insights into game-community alignment.

## Key Results Summary

### Main Finding
**Only 1 out of 3,220 dead games (0.03%) achieved cosine similarity ≥ 0.8** with any community profile.

### Key Statistics
- **Highest similarity:** 0.8163 (Groggers! → Community 7)
- **Average similarity:** 0.4480  
- **Games with similarity ≥ 0.6:** 268 (8.32%)
- **Games with similarity ≥ 0.7:** 33 (1.02%)

### Business Implications
- Dead games are genuinely different from successful patterns
- Game failure often stems from fundamental market misalignment  
- High similarity is extremely rare, validating "dead game" classification

## System Architecture

### Core Components

1. **`unified_similarity_analysis.py`** - Main orchestration script
2. **`calculate_game_community_similarity.py`** - Core similarity calculation engine  
3. **`create_analysis_report.py`** - Comprehensive report generator
4. **`build_aligned_features.py`** - Alternative feature alignment approach

### Dependencies
- Existing `graph_scripts/build_feature_vectors.py` for consistent feature engineering
- Community profiles from `community_14_profiles_analysis/`
- Dead games dataset from `out/dead_games_only_test.csv`

## Usage

### Quick Start
```bash
# Run complete analysis with default settings
python unified_similarity_analysis.py \
  --games-csv ../out/dead_games_only_test.csv \
  --community-profiles ../community_14_profiles_analysis/detailed_community_profiles.json \
  --out-dir results \
  --threshold 0.8
```

### Custom Analysis
```bash
# Analyze with different threshold
python unified_similarity_analysis.py \
  --games-csv path/to/games.csv \
  --community-profiles path/to/profiles.json \
  --out-dir custom_results \
  --threshold 0.7
```

## Output Structure

```
results/
├── combined_dataset.csv              # Unified games + communities data
├── combined_features/                # Unified feature vectors
├── games_features/                   # Games-only feature vectors
├── communities_features/             # Communities-only feature vectors  
├── results/
│   ├── similarity_matrix.npz         # Full similarity matrix
│   ├── similarity_results.json       # Detailed statistics
│   ├── high_similarity_games.csv     # Games above threshold
│   └── all_games_similarity.csv      # All games with best matches
└── UNIFIED_COSINE_SIMILARITY_REPORT.md # Comprehensive analysis report
```

## Feature Engineering

### Methodology
Uses the established `graph_scripts/build_feature_vectors.py` approach:

- **L2-normalized sparse vectors** for consistent cosine similarity
- **Multi-hot encoding** for categorical features (genres, tags, developers, etc.)
- **Standardized scaling** for numerical features (prices, scores, player counts)  
- **Binary encoding** for boolean features (platform support, DLC status)

### Feature Dimensions
- **Total features:** ~487 dimensions per vector
- **Categorical features:** Multi-hot encoded with top-100 values per field
- **Numerical features:** 9 standardized numeric fields
- **Boolean features:** 7 binary platform/status indicators

## Key Insights

### 1. Dead Game Divergence
The extremely low similarity scores (only 0.03% ≥ 0.8) validate that games classified as "dead" are genuinely different from successful community patterns, not just poorly marketed versions of good concepts.

### 2. Community 7 Attraction  
The single high-similarity game (Groggers!) matched Community 7, which appears to represent indie/casual action-adventure games. This suggests some dead games do align with successful patterns but fail for other reasons (timing, execution, discoverability).

### 3. Threshold Distribution
- **≥ 0.5:** 34.41% (moderate alignment possible)
- **≥ 0.6:** 8.32% (good alignment rare)  
- **≥ 0.7:** 1.02% (strong alignment very rare)
- **≥ 0.8:** 0.03% (exceptional alignment extremely rare)

## Reusability Features

### Configurable Parameters
- **Threshold:** Adjust similarity threshold for analysis
- **Feature selection:** Modify categorical/numerical field inclusion
- **Community profiles:** Works with any community profile JSON structure
- **Block size:** Adjust memory usage for large datasets

### Extensible Design
- **Modular components:** Each script handles specific functionality
- **Consistent methodology:** Uses existing `graph_scripts` pipeline
- **JSON output:** Machine-readable results for further analysis
- **Markdown reports:** Human-readable comprehensive analysis

### Error Handling
- **Feature dimension alignment:** Ensures identical vector spaces
- **Missing data handling:** Conservative approach (empty strings → [], NaN → 0)
- **Memory management:** Blockwise computation for large datasets
- **Progress tracking:** Real-time progress reporting

## Technical Specifications

### Performance
- **Processing time:** ~3-4 seconds for 3,220 games vs 14 communities
- **Memory usage:** Efficient sparse matrix operations  
- **Scalability:** Linear scaling with dataset size

### Validation
- **Similarity range:** All scores within valid [-1, 1] bounds
- **Feature alignment:** Verified identical dimensions across datasets
- **Normalization:** Confirmed L2 normalization preserves similarity properties

## Example Applications

### Game Development
```bash
# Analyze new game concept against community profiles
python unified_similarity_analysis.py \
  --games-csv new_game_concepts.csv \
  --community-profiles community_profiles.json \
  --out-dir concept_analysis \
  --threshold 0.7
```

### Market Research  
```bash
# Compare failed games vs community patterns
python unified_similarity_analysis.py \
  --games-csv failed_games_2024.csv \
  --community-profiles updated_profiles.json \
  --out-dir market_analysis \
  --threshold 0.6
```

### Investment Analysis
```bash
# Evaluate portfolio alignment with successful communities  
python unified_similarity_analysis.py \
  --games-csv portfolio_games.csv \
  --community-profiles investor_profiles.json \
  --out-dir investment_analysis \
  --threshold 0.8
```

## Files Description

| File | Purpose | Reusable |
|------|---------|----------|
| `unified_similarity_analysis.py` | Main orchestration script | ✅ |
| `calculate_game_community_similarity.py` | Core similarity engine | ✅ |
| `create_analysis_report.py` | Report generation | ✅ |
| `build_aligned_features.py` | Alternative feature alignment | ✅ |
| `build_community_vectors.py` | Community vector builder | ✅ |
| `run_full_analysis.py` | Original orchestration (deprecated) | ❌ |

## Future Enhancements

### Potential Improvements
1. **Dynamic thresholding:** Automatically determine optimal thresholds
2. **Similarity explanations:** Identify which features drive high/low similarity
3. **Temporal analysis:** Track community profile evolution over time  
4. **Clustering analysis:** Group similar dead games for pattern identification

### Integration Opportunities  
1. **Steam Discovery:** Integrate with recommendation systems
2. **Developer Tools:** Real-time similarity checking during development
3. **Market Intelligence:** Continuous monitoring of game-community alignment
4. **Academic Research:** Game studies and digital humanities applications

---

*System developed: September 2025*  
*Methodology: Graph-based cosine similarity with L2-normalized sparse feature vectors*  
*Validation: 3,220 dead games vs 14 established community profiles*