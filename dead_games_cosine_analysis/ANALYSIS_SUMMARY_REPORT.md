# Dead Games vs Community Profiles: Cosine Similarity Analysis Report

## Executive Summary

**Key Finding**: Out of 3,220 dead games analyzed, only **20 games (0.6%)** match at least one community profile with a cosine similarity ≥ 0.8.

This result indicates that the vast majority of dead games (99.4%) have significantly different feature profiles compared to the established community clusters, suggesting they represent outlier or niche gaming experiences that don't align well with mainstream community preferences.

## Analysis Overview

- **Dataset**: 3,220 dead games from `out/dead_games_only_test.csv`
- **Community Profiles**: 14 community average profiles
- **Similarity Metric**: Cosine similarity with L2-normalized feature vectors
- **Threshold**: 0.8 (indicating strong similarity)
- **Analysis Date**: 2025-09-20

## Key Results

### Threshold Analysis
| Threshold | Games Above | Percentage |
|-----------|-------------|------------|
| ≥ 0.50    | 448         | 13.9%      |
| ≥ 0.60    | 205         | 6.4%       |
| ≥ 0.70    | 133         | 4.1%       |
| ≥ 0.75    | 102         | 3.2%       |
| **≥ 0.80**| **20**      | **0.6%**   |
| ≥ 0.85    | 4           | 0.1%       |
| ≥ 0.90    | 0           | 0.0%       |

### Similarity Statistics
- **Mean Similarity**: 0.376 (moderate dissimilarity)
- **Median Similarity**: 0.357
- **Standard Deviation**: 0.135
- **Range**: [-0.055, 0.874]
- **95th Percentile**: 0.659 (only 5% of games exceed this)

### High-Similarity Games Distribution by Community
| Community | High-Sim Games | Percentage of High-Sim |
|-----------|----------------|------------------------|
| Community 11 | 10 games | 50.0% |
| Community 1  | 5 games  | 25.0% |
| Community 9  | 4 games  | 20.0% |
| Community 3  | 1 game   | 5.0%  |

## Top Matching Games

The highest-scoring dead games that match community profiles:

1. **Delta Force: Task Force Dagger** (AppID: 32650)
   - Community Match: 11 
   - Similarity: 0.874
   - Genre: Action/FPS/Military
   - Release: 2002, Price: ₪36.95

2. **Global Ops: Commando Libya** (AppID: 200020)
   - Community Match: 11
   - Similarity: 0.872
   - Genre: Action/Third-Person Shooter/Military
   - Release: 2011, Price: ₪3.50

3. **Amazing Adventures Around the World** (AppID: 3530)
   - Community Match: 11
   - Similarity: 0.865
   - Genre: Casual/Hidden Object/Puzzle
   - Release: 2008, Price: ₪18.50

## Community Profile Analysis

### Community 11 (Primary Match Target)
- **Size**: 267 games
- **Dominant Genre**: Action (52.6%)
- **Platform**: 95.5% Windows, 25.5% Mac, 17.2% Linux
- **Price Profile**: Average ₪46.62, Median ₪36.95
- **Notable**: Strong action/shooter orientation with moderate pricing

### Community 1 (Secondary Match)
- **Size**: 1,139 games  
- **Dominant Genre**: Indie (64.4%)
- **Platform**: 100% Windows, 60.8% Mac, 56.8% Linux
- **Price Profile**: Average ₪30.92, Median ₪12.45
- **Notable**: Indie-focused with broad platform support

## Implications & Insights

### 1. Dead Games Are Truly Different
The extremely low match rate (0.6%) suggests that games that "die" typically have feature combinations that don't align with successful community patterns. This could indicate:
- Niche gameplay mechanics
- Poor market fit
- Unique but unmarketable feature combinations
- Outdated or obsolete game types

### 2. Community 11 Attracts Dead Action Games  
Half of the matching dead games align with Community 11 (Action/Military games), suggesting that even failed games in this genre share some characteristics with successful action titles.

### 3. Similarity Distribution is Heavily Left-Skewed
With a median similarity of 0.357 and only 13.9% of games exceeding 0.5 similarity, most dead games are quite dissimilar from successful community profiles.

### 4. Feature Engineering Effectiveness
The unified feature building approach successfully created comparable 249-dimensional vectors for both datasets, enabling meaningful similarity computation across:
- Numeric features (13): price, ratings, counts
- Categorical features (141): genres, tags, platforms
- Hash features (64): developers, publishers  
- One-hot features (31): types, support options

## Technical Implementation

### Feature Vector Composition
- **Total Dimensions**: 249 features
- **Numeric Block**: 13 features (price, ratings, user metrics)
- **Multi-value Features**: 148 features (genres, tags, categories, languages)
- **Hashed Features**: 64 features (developers, publishers)
- **One-hot Features**: 24 features (type, support, platform)

### Processing Pipeline
1. **Data Harmonization**: Community profiles converted to synthetic game data
2. **Unified Feature Building**: Both datasets processed through identical pipeline
3. **L2 Normalization**: All vectors normalized for cosine similarity
4. **Block-wise Computation**: 500-game blocks for memory efficiency
5. **Comprehensive Analysis**: Multi-threshold evaluation and statistics

## Recommendations

### For Game Developers
1. **Avoid Dead Game Patterns**: Study the feature profiles of dead games to identify potential red flags
2. **Target Community Alignment**: Ensure new games have feature combinations that align with at least one successful community
3. **Genre-Market Fit**: Pay special attention to action/military games, as they show some recoverability patterns

### For Future Analysis
1. **Lower Threshold Studies**: Analyze games with 0.6-0.8 similarity for "near miss" patterns
2. **Community-Specific Deep Dives**: Investigate why Community 11 attracts the most dead game matches
3. **Temporal Analysis**: Examine if similarity patterns change over time periods
4. **Success Factor Correlation**: Correlate high similarity with other success metrics

## Files Generated

- `dead_games_similarity_results.csv`: Complete results for all 3,220 games
- `dead_games_high_similarity.csv`: Only the 20 games above 0.8 threshold  
- `dead_games_similarity_analysis.json`: Detailed statistics and metadata
- `features/`: Feature engineering artifacts and metadata

---

**Analysis completed**: 2025-09-20  
**Total processing time**: ~4 minutes for 3,220 games × 14 communities  
**Memory efficiency**: Block-wise processing enabled analysis on standard hardware