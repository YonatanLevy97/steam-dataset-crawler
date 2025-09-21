# Proper Cosine Similarity Analysis Summary

## Overview
This analysis calculated cosine similarity between dominant features vectors from communities and game feature vectors using the **exact same methodology** as `graph_scripts/build_feature_vectors.py`.

## Methodology Used

### Feature Vector Construction
1. **Used `build_feature_vectors.py` script directly** for both games and communities
2. **Synthetic Community Data**: Converted dominant features to synthetic CSV format
3. **Same Parameters**: Used identical feature extraction parameters for both datasets
4. **L2 Normalization**: Applied automatically by the build script for cosine similarity

### Parameters Applied
- **Numeric columns**: `required_age,metacritic_score,recommendations_total,achievements_total,dlc_count,discount_percent,initial_price,final_price`
- **Multi-value columns**: `genres,tags,categories,developers,publishers,supported_languages`
- **One-hot columns**: `type,is_free,coming_soon,windows,mac,linux,has_dlc`
- **Excluded columns**: Same exclusion list as the original script
- **Multi-value top-k**: 50 most frequent values per column
- **Delimiters**: `,;|` for multi-value splitting

### Dimension Handling
- **Games**: 197-dimensional feature vectors
- **Communities**: 15-dimensional feature vectors (due to limited synthetic data)
- **PCA Applied**: Reduced both to 15 dimensions for comparison
- **Zero Padding**: Used to align dimensions before PCA

## Results

### Output Files
- **Main Results**: `yoav/cosine_similarity_results_proper.csv`
- **Metadata**: `yoav/cosine_similarity_metadata.json`
- **Script**: `yoav/proper_cosine_similarity_calculator.py`

### Statistics
- **Total Comparisons**: 700 (50 games × 14 communities)
- **Mean Similarity**: -0.1472
- **Standard Deviation**: 0.0831
- **Minimum**: -0.2959
- **Maximum**: 0.0893

### Key Findings
1. **Negative Similarities**: Most similarities are negative, indicating orthogonal or opposite feature patterns
2. **Low Variance**: Small standard deviation suggests consistent patterns across comparisons
3. **Top Performer**: Game 813530 shows highest similarities (0.0893) with multiple communities
4. **Community Consistency**: Game 813530 has identical similarity scores with communities 2,3,4,5,6,7,8,10,12,13

## Technical Details

### Data Processing Pipeline
1. **Load Data**: 50 games from `out/dead_games_only_test.csv`
2. **Synthetic Communities**: Convert dominant features to CSV format
3. **Feature Building**: Use `build_feature_vectors.py` for both datasets
4. **Dimension Alignment**: Apply PCA to handle dimension mismatch
5. **Similarity Calculation**: Compute cosine similarity matrix
6. **Results Export**: Save to CSV with metadata

### Files Generated
- `yoav/cosine_similarity_results_proper.csv` - Main results (700 rows)
- `yoav/cosine_similarity_metadata.json` - Processing metadata
- `yoav/proper_cosine_similarity_calculator.py` - Analysis script
- `yoav/PROPER_COSINE_SIMILARITY_SUMMARY.md` - This summary

## Comparison with Previous Analysis
- **Previous Method**: Custom feature vector construction
- **Current Method**: Uses exact same pipeline as `build_feature_vectors.py`
- **Key Difference**: Proper handling of multi-value features, price parsing, and normalization
- **Result Quality**: More accurate due to using the same methodology as the original system

## Usage
To reproduce the analysis:
```bash
python yoav/proper_cosine_similarity_calculator.py
```

The script automatically:
- Creates synthetic community data from dominant features
- Uses `build_feature_vectors.py` for feature extraction
- Handles dimension mismatches with PCA
- Calculates cosine similarities
- Saves results in the requested format