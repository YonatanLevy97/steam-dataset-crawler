# Cosine Similarity Analysis Summary

## Overview
This analysis calculated cosine similarity between dominant features vectors from communities and game feature vectors from the dead games database.

## Data Sources
- **Games Database**: `out/dead_games_only_test.csv` (1,610 games total)
- **Dominant Features**: `yoav/specific_features_analysis/dominant_features_specific.json` (14 communities)
- **Sample Size**: 50 games (first 50 from the database)

## Methodology

### Feature Vector Construction
1. **Game Features**: Built comprehensive feature vectors including:
   - Numeric features: age, metacritic score, recommendations, achievements, DLC count, discount, prices
   - Boolean features: free status, platform support, DLC availability
   - Categorical features: game type, categories, genres (one-hot encoded)
   - Multi-value features: tags, developers, publishers

2. **Community Features**: Mapped dominant features from community profiles to corresponding feature vector positions

3. **Dimension Handling**: Both game and community vectors had matching dimensions (39 features), so no PCA was needed

### Similarity Calculation
- Used cosine similarity: `cos(θ) = (A·B) / (||A|| × ||B||)`
- Normalized vectors before calculation
- Handled zero vectors appropriately

## Results

### Output File
- **Location**: `yoav/cosine_similarity_results.csv`
- **Format**: CSV with columns: `appid`, `community_id`, `cosine_similarity`
- **Total Comparisons**: 700 (50 games × 14 communities)

### Statistics
- **Mean Similarity**: 0.0080
- **Standard Deviation**: 0.0232
- **Minimum**: 0.0000
- **Maximum**: 0.2774

### Top Similarities
The highest cosine similarity was found between:
- **Game 529640** and **Community 9**: 0.2774
- **Game 529640** and **Community 1**: 0.1891
- **Game 529640** and **Community 11**: 0.1852

## Key Findings
1. **Low Overall Similarity**: Most similarities are very low (mean ~0.008), indicating that the dominant community features don't strongly align with individual game characteristics
2. **Sparse Similarities**: Many game-community pairs have zero similarity, suggesting limited feature overlap
3. **Outlier Game**: Game 529640 shows consistently higher similarities across multiple communities
4. **Community Variation**: Different communities show varying levels of similarity with games

## Technical Details
- **Script**: `yoav/cosine_similarity_calculator.py`
- **Dependencies**: pandas, numpy, scipy, sklearn
- **Feature Dimensions**: 39 features per vector
- **Processing**: Handled price parsing, boolean conversion, categorical encoding

## Files Generated
1. `yoav/cosine_similarity_results.csv` - Main results file
2. `yoav/cosine_similarity_calculator.py` - Analysis script
3. `yoav/COSINE_SIMILARITY_ANALYSIS_SUMMARY.md` - This summary

## Usage
To reproduce or modify the analysis:
```bash
python yoav/cosine_similarity_calculator.py
```

The script automatically:
- Loads the first 50 games from the CSV
- Loads all 14 community dominant features
- Builds feature vectors for both
- Calculates all pairwise similarities
- Saves results to CSV format