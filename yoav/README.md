# Dominant Features Profiler

A new approach to community profiling that identifies the most characteristic features of each community and matches games based on shared dominant features.

## 🎯 Key Innovation

Instead of using average feature vectors (like traditional community profiling), this profiler:

1. **Identifies Dominant Features**: For each community, finds features that >70% of games share the same values
2. **Creates Feature-Based Profiles**: Each community is characterized by its dominant features rather than average values
3. **Matches by Shared Features**: Games are matched to communities based on how many dominant features they share

## 🚀 How It Works

### 1. Dominant Feature Identification
- For each community, analyze all features
- A feature is "dominant" if >70% of games in the community have the same value
- For binary features (0/1), check if majority is 1 or 0
- Focus on the most distinctive characteristics of each community

### 2. Community Profiling
- Each community gets a profile of its dominant features
- Features are categorized by type (genres, categories, tags, developers, etc.)
- Calculate summary statistics (size, dead game ratio, pricing, etc.)

### 3. Game-Community Matching
- For each game, count how many dominant features it shares with each community
- Calculate match score as: `shared_dominant_features / total_dominant_features`
- Assign games to communities with highest match scores

## 📊 Example Results

From the test run:
- **5 communities** analyzed with **1000 games**
- Each community had **5 dominant features** (80%+ games sharing same values)
- **High match scores**: Mean 0.893, Median 1.000
- **Clear differentiation**: Each community has distinct dominant features

### Sample Community Profile:
```
Community 0: 210 games
- Dominant features: 5
- Dead games: 32.9%
- Top dominant features:
  - feature_4: 89.5% (188/210)
  - feature_0: 89.0% (187/210)
  - feature_1: 89.0% (187/210)
```

## 🛠️ Usage

### Basic Usage
```bash
python dominant_features_profiler.py \
    --communities /path/to/community_assignments.csv \
    --metadata /path/to/games_metadata.csv \
    --features-dir /path/to/features \
    --out-dir ./results
```

### With Custom Threshold
```bash
python dominant_features_profiler.py \
    --communities /path/to/community_assignments.csv \
    --metadata /path/to/games_metadata.csv \
    --features-dir /path/to/features \
    --out-dir ./results \
    --threshold 0.8  # 80% threshold for dominant features
```

### Test with Synthetic Data
```bash
python test_dominant_profiler.py
```

## 📁 Input Requirements

### Community Assignments CSV
```csv
node_id,community_id,community_size
game_001,0,150
game_002,0,150
game_003,1,200
```

### Games Metadata CSV
```csv
appid,name,genres,initial_price,final_price,metacritic_score,label_dead_binary
game_001,Test Game 1,Action,29.99,19.99,85,0
game_002,Test Game 2,RPG,39.99,29.99,78,1
```

### Features Directory
```
features/
├── X_csr.npz          # Sparse feature matrix
├── feature_names.txt   # Feature names (one per line)
├── features_meta.json # Feature metadata
└── appids.npy         # Game IDs matching matrix rows
```

## 📈 Output Files

### 1. `dominant_features.json`
Detailed dominant features for each community:
```json
{
  "0": {
    "feature_0": {
      "dominant_value": 1,
      "percentage": 0.89,
      "count": 187,
      "total": 210,
      "feature_type": "binary"
    }
  }
}
```

### 2. `community_profiles.json`
Complete community profiles with statistics:
```json
{
  "0": {
    "community_id": 0,
    "size": 210,
    "dominant_features_count": 5,
    "dead_games_count": 69,
    "dead_games_percentage": 0.329,
    "avg_metacritic_score": 75.0,
    "avg_final_price": 53.08
  }
}
```

### 3. `game_community_matches.json`
Match scores for each game with each community:
```json
{
  "game_001": {
    "0": 0.8,
    "1": 0.6,
    "2": 0.4
  }
}
```

### 4. `evaluation_results.json`
Performance metrics and statistics:
```json
{
  "total_games_matched": 1000,
  "total_communities": 5,
  "match_score_distribution": {
    "mean": 0.893,
    "median": 1.000,
    "std": 0.234
  }
}
```

### 5. `summary_report.md`
Human-readable summary with key insights.

## 🔧 Configuration

### Dominant Threshold
- **Default**: 0.7 (70%)
- **Meaning**: A feature is dominant if >70% of games in community share same value
- **Range**: 0.5-0.95 (50%-95%)
- **Higher values**: More selective, fewer dominant features
- **Lower values**: More inclusive, more dominant features

### Feature Types Supported
- **Binary features**: 0/1 values (platforms, boolean flags)
- **Categorical features**: One-hot encoded (genres, categories, tags)
- **Continuous features**: Currently skipped (could be extended)

## 🎯 Advantages Over Traditional Profiling

### 1. **Interpretability**
- Clear understanding of what makes each community unique
- Easy to identify key characteristics (e.g., "Action games with Windows support")

### 2. **Robustness**
- Less sensitive to outliers than average-based approaches
- Focuses on majority characteristics rather than mean values

### 3. **Feature Importance**
- Automatically identifies most important features for each community
- No need for manual feature selection or weighting

### 4. **Clear Matching Logic**
- Transparent scoring: games match communities by sharing dominant features
- Easy to understand why a game belongs to a community

## 🔬 Technical Details

### Algorithm Complexity
- **Time**: O(C × F × G) where C=communities, F=features, G=games
- **Space**: O(C × F + G × C) for storing dominant features and matches
- **Scalable**: Efficient sparse matrix operations

### Memory Usage
- Uses sparse matrices for efficient storage
- Processes games in batches to manage memory
- JSON serialization handles numpy types automatically

### Error Handling
- Graceful handling of missing data
- Validation of input file formats
- Comprehensive error messages

## 🧪 Testing

The profiler includes comprehensive testing:

```bash
# Run test with synthetic data
python test_dominant_profiler.py

# Test results validation
- Verifies dominant feature identification
- Checks community profile generation
- Validates game-community matching
- Confirms output file generation
```

## 🚀 Future Enhancements

### 1. **Continuous Features**
- Extend to handle continuous features (prices, scores)
- Implement statistical tests for dominance
- Add binning strategies for continuous values

### 2. **Multi-Value Features**
- Support features with multiple values per game
- Handle comma-separated categorical features
- Implement set-based similarity measures

### 3. **Advanced Matching**
- Weight features by importance
- Implement fuzzy matching for similar values
- Add confidence scores for matches

### 4. **Visualization**
- Generate community feature heatmaps
- Create match score distributions
- Build interactive community explorer

## 📝 Notes

- **File Safety**: All output goes to the `yoav/` directory to avoid conflicts
- **Reproducibility**: Uses fixed random seeds for consistent results
- **Compatibility**: Works with existing community detection outputs
- **Extensibility**: Modular design allows easy feature additions

## 🤝 Integration

This profiler can be integrated with existing community detection pipelines:

1. **Input**: Use community assignments from Louvain/Girvan-Newman
2. **Processing**: Run dominant features analysis
3. **Output**: Generate community profiles and game matches
4. **Analysis**: Combine with existing visualization tools

The profiler complements rather than replaces existing approaches, providing a new perspective on community characteristics and game-community relationships.