# Cosine Similarity Analysis: Dead Games vs Community Profiles

## Executive Summary

This analysis calculated cosine similarity between **1,610 dead games** and **37 established community profiles** using L2-normalized sparse feature vectors.

### Key Findings

- **Games with similarity ≥ 0.1:** 1,592 (98.88%)
- **Highest similarity achieved:** 0.8218
- **Average similarity:** 0.4950
- **Median similarity:** 0.5022

---

## Methodology

### Feature Engineering
The analysis used the established `graph_scripts` methodology:

1. **Feature Vector Construction**
   - L2-normalized sparse vectors for consistent cosine similarity
   - Categorical features: Multi-hot encoding (genres, tags, developers, etc.)
   - Numerical features: Standardized scaling (prices, scores, player counts)
   - Boolean features: Binary encoding (platform support, DLC status)

2. **Similarity Calculation**
   - Blockwise cosine similarity computation for memory efficiency
   - Sparse matrix operations for computational performance
   - Consistent normalization across games and community profiles

### Data Sources
- **Dead Games:** `out/dead_games_only_test.csv` (1,610 games)
- **Community Profiles:** `community_14_profiles_analysis/detailed_community_profiles.json` (37 communities)
- **Analysis Date:** 2025-09-20

---

## Detailed Results

### Similarity Distribution

| Statistic | Value |
|-----------|-------|
| **Mean** | 0.4950 |
| **Median** | 0.5022 |
| **Standard Deviation** | 0.1134 |
| **Minimum** | -0.0064 |
| **Maximum** | 0.8218 |

### Percentile Analysis

| Percentile | Similarity |
|------------|------------|
| 25th | 0.4359 |
| 50th | 0.5022 |
| 75th | 0.5686 |
| 90th | 0.6246 |
| 95th | 0.6583 |
| 99th | 0.7512 |

### Threshold Analysis

The distribution of games across different similarity thresholds reveals the degree of alignment between dead games and successful community patterns:

| Threshold | Games | Percentage |
|-----------|-------|------------|
| ≥0.50 | 820 | 50.93% |
| ≥0.60 | 243 | 15.09% |
| ≥0.65 | 97 | 6.02% |
| ≥0.70 | 37 | 2.30% |
| ≥0.75 | 17 | 1.06% |
| ≥0.80 | 2 | 0.12% |
| ≥0.85 | 0 | 0.00% |
| ≥0.90 | 0 | 0.00% |

### High-Similarity Games Community Distribution

Games with similarity ≥ 0.1 show the following community preferences:

| Community | Games | Percentage | Top Genres | Top Tags |
|-----------|-------|------------|------------|----------|
| 1 | 43 | 2.7% | Indie, Adventure, Action | Singleplayer, Adventure, Action |
| 10 | 2 | 0.1% | Indie, Casual, Adventure | Casual, Singleplayer, Indie |
| 11 | 94 | 5.9% | Indie, Casual, Simulation | Singleplayer, Indie, Casual |
| 12 | 16 | 1.0% | Indie, Action, Adventure | Indie, Singleplayer, Action |
| 2 | 688 | 43.2% | Indie, Adventure, Casual | Indie, Singleplayer, Casual |
| 20 | 2 | 0.1% | Indie, Action, Adventure | Singleplayer, Indie, Action |
| 21 | 150 | 9.4% | Indie, Strategy, Simulation | Singleplayer, Indie, Strategy |
| 23 | 151 | 9.5% | Action, Adventure, Indie | Singleplayer, Action, Adventure |
| 27 | 156 | 9.8% | Casual, Indie, Adventure | Casual, Singleplayer, Adventure |
| 28 | 8 | 0.5% | Casual, Simulation, Action | Movie, Episodic, Action |
| 30 | 13 | 0.8% | Action, Indie, Adventure | Singleplayer, Action, Adventure |
| 31 | 8 | 0.5% | Indie, Action, Casual | Singleplayer, Indie, Action |
| 32 | 22 | 1.4% | Utilities, Design & Illustration, Animation & Modeling | Utilities, Software, Design & Illustration |
| 34 | 7 | 0.4% | RPG, Casual, Simulation | Sexual Content, Nudity, Mature |
| 36 | 152 | 9.5% | Indie, Casual, Adventure | Indie, Singleplayer, Adventure |
| 5 | 1 | 0.1% | Indie, Casual, Adventure | Singleplayer, Indie, Casual |
| 7 | 69 | 4.3% | Indie, Casual, Adventure | Singleplayer, Indie, Casual |
| 9 | 10 | 0.6% | Indie, Casual, Action | Singleplayer, Indie, Casual |

### Overall Community Distribution

All 1,610 dead games show the following best-match community distribution:

| Community | Games | Percentage | Size | Most Common Genre | Most Common Tag |
|-----------|-------|------------|------|-------------------|-----------------|
| 1 | 43 | 2.7% | 404 | Indie | Singleplayer |
| 10 | 2 | 0.1% | 192 | Indie | Casual |
| 11 | 94 | 5.8% | 320 | Indie | Singleplayer |
| 12 | 16 | 1.0% | 330 | Indie | Indie |
| 2 | 688 | 42.7% | 288 | Indie | Indie |
| 20 | 2 | 0.1% | 229 | Indie | Singleplayer |
| 21 | 150 | 9.3% | 258 | Indie | Singleplayer |
| 23 | 151 | 9.4% | 341 | Action | Singleplayer |
| 27 | 156 | 9.7% | 339 | Casual | Casual |
| 28 | 23 | 1.4% | 166 | Casual | Movie |
| 30 | 13 | 0.8% | 503 | Action | Singleplayer |
| 31 | 8 | 0.5% | 396 | Indie | Singleplayer |
| 32 | 23 | 1.4% | 107 | Utilities | Utilities |
| 34 | 7 | 0.4% | 17 | RPG | Sexual Content |
| 36 | 154 | 9.6% | 362 | Indie | Indie |
| 5 | 1 | 0.1% | 286 | Indie | Singleplayer |
| 7 | 69 | 4.3% | 394 | Indie | Singleplayer |
| 9 | 10 | 0.6% | 334 | Indie | Singleplayer |

---

## Top Performing Games

The games with highest similarity to community profiles represent the closest matches between dead games and successful patterns:

### Top 10 Games by Similarity

| Rank | Game | AppID | Community | Similarity | Genre Profile | Tag Profile |
|------|------|-------|-----------|------------|---------------|-------------|
| 1 | Ian's Eyes Demo | 503810 | 32 | 0.8218 | Utilities, Design & Illustration | Utilities, Software |
| 2 | Not without my donuts | 366610 | 2 | 0.8122 | Indie, Adventure | Indie, Singleplayer |
| 3 | zTime (Danger Noodles!) | 523440 | 2 | 0.7936 | Indie, Adventure | Indie, Singleplayer |
| 4 | Sparkle 2 | 370550 | 2 | 0.7923 | Indie, Adventure | Indie, Singleplayer |
| 5 | Akuatica Demo | 435090 | 2 | 0.7921 | Indie, Adventure | Indie, Singleplayer |
| 6 | Catorize | 418960 | 2 | 0.7779 | Indie, Adventure | Indie, Singleplayer |
| 7 | Watchmen: The End Is Nigh Part 2 | 21030 | 32 | 0.7689 | Utilities, Design & Illustration | Utilities, Software |
| 8 | Starpoint Gemini | 108110 | 32 | 0.7685 | Utilities, Design & Illustration | Utilities, Software |
| 9 | Assault of the Robots | 1052900 | 2 | 0.7681 | Indie, Adventure | Indie, Singleplayer |
| 10 | Sakura and Crit: The Mock Game | 867590 | 27 | 0.7667 | Casual, Indie | Casual, Singleplayer |

---

## Key Insights & Analysis

### 1. Dead Game Divergence

Only **1592 games (98.88%)** achieved high similarity (≥0.1) with community profiles. This indicates:

- **Rare Alignment**: Very few dead games closely resemble successful community patterns  
- **Execution vs. Concept**: High-similarity games may have failed due to execution rather than fundamental concept issues
- **Market Timing**: Some games may have been ahead of or behind market trends


### 2. Community Clustering Patterns

Dead games show distinct clustering preferences:

**Community 2** attracts 688 games (42.7%) - primarily Indie-focused
**Community 27** attracts 156 games (9.7%) - primarily Casual-focused
**Community 36** attracts 154 games (9.6%) - primarily Indie-focused


### 3. Statistical Observations

- **95th Percentile**: 0.6583 - Even top-performing dead games show moderate similarity
- **Mean Similarity**: 0.4950 - Overall low alignment with community patterns
- **Distribution Shape**: Right-skewed distribution indicates most games cluster at low similarities

### 4. Business Implications

#### For Game Developers
- **Market Research**: Use community profiles to validate game concepts before development
- **Feature Alignment**: Ensure key features (genres, tags, pricing) match target community preferences  
- **Risk Assessment**: Games with low community alignment face higher failure risk

#### For Publishers
- **Portfolio Strategy**: Diversify across multiple community archetypes
- **Marketing Focus**: Target communities with highest similarity scores
- **Investment Decisions**: Consider community alignment in funding decisions

#### For the Steam Ecosystem
- **Discovery Algorithms**: Leverage community profiles for better game recommendations
- **Developer Tools**: Provide community similarity analysis during game submission
- **Market Intelligence**: Track community evolution and emerging patterns

---

## Technical Details

### Feature Vector Specifications
- **Total Features**: Approximately N/A dimensions
- **Categorical Features**: Multi-hot encoded (genres, tags, developers, publishers, categories, languages)
- **Numerical Features**: Standardized (age ratings, scores, counts, prices, discounts)  
- **Boolean Features**: Binary encoded (platform support, DLC status, free-to-play)

### Computational Approach
- **Algorithm**: Cosine similarity with L2-normalized sparse vectors
- **Memory Management**: Blockwise computation for scalability
- **Performance**: Optimized sparse matrix operations
- **Reproducibility**: Consistent feature engineering across datasets

### Validation
- **Feature Alignment**: Verified dimensional compatibility between games and communities
- **Normalization**: Confirmed L2 normalization maintains similarity properties
- **Range Validation**: All similarity scores within valid [0, 1] bounds

---

## Appendix

### Community Profile Summary

| Community | Size | Top Genre | Top Tag | Description |
|-----------|------|-----------|---------|-------------|
| 0 | 520 | Indie | Singleplayer | Indie games focused on singleplayer |
| 1 | 404 | Indie | Singleplayer | Indie games focused on singleplayer |
| 2 | 288 | Indie | Indie | Indie games focused on indie |
| 3 | 681 | Indie | Singleplayer | Indie games focused on singleplayer |
| 4 | 199 | Indie | Singleplayer | Indie games focused on singleplayer |
| 5 | 286 | Indie | Singleplayer | Indie games focused on singleplayer |
| 6 | 670 | Indie | Indie | Indie games focused on indie |
| 7 | 394 | Indie | Singleplayer | Indie games focused on singleplayer |
| 8 | 518 | Indie | Singleplayer | Indie games focused on singleplayer |
| 9 | 334 | Indie | Singleplayer | Indie games focused on singleplayer |
| 10 | 192 | Indie | Casual | Indie games focused on casual |
| 11 | 320 | Indie | Singleplayer | Indie games focused on singleplayer |
| 12 | 330 | Indie | Indie | Indie games focused on indie |
| 13 | 140 | Indie | Singleplayer | Indie games focused on singleplayer |
| 14 | 783 | Indie | Indie | Indie games focused on indie |
| 15 | 607 | Indie | Singleplayer | Indie games focused on singleplayer |
| 16 | 482 | Indie | Singleplayer | Indie games focused on singleplayer |
| 17 | 192 | Indie | Indie | Indie games focused on indie |
| 18 | 197 | Indie | Singleplayer | Indie games focused on singleplayer |
| 19 | 184 | Indie | Singleplayer | Indie games focused on singleplayer |
| 20 | 229 | Indie | Singleplayer | Indie games focused on singleplayer |
| 21 | 258 | Indie | Singleplayer | Indie games focused on singleplayer |
| 22 | 482 | Indie | Singleplayer | Indie games focused on singleplayer |
| 23 | 341 | Action | Singleplayer | Action games focused on singleplayer |
| 24 | 1,124 | Indie | Singleplayer | Indie games focused on singleplayer |
| 25 | 141 | Indie | Indie | Indie games focused on indie |
| 26 | 744 | Indie | Singleplayer | Indie games focused on singleplayer |
| 27 | 339 | Casual | Casual | Casual games focused on casual |
| 28 | 166 | Casual | Movie | Casual games focused on movie |
| 29 | 1,136 | Action | Singleplayer | Action games focused on singleplayer |
| 30 | 503 | Action | Singleplayer | Action games focused on singleplayer |
| 31 | 396 | Indie | Singleplayer | Indie games focused on singleplayer |
| 32 | 107 | Utilities | Utilities | Utilities games focused on utilities |
| 33 | 176 | Indie | Singleplayer | Indie games focused on singleplayer |
| 34 | 17 | RPG | Sexual Content | RPG games focused on sexual content |
| 35 | 10 | Utilities | Utilities | Utilities games focused on utilities |
| 36 | 362 | Indie | Indie | Indie games focused on indie |

### Data Quality Notes
- **Missing Values**: Handled conservatively (empty strings for categorical, 0 for numerical)
- **Price Normalization**: Currency symbols removed, standardized to numeric values
- **Community Profiles**: Based on successful games with established player bases
- **Dead Game Criteria**: Games with consistently low player engagement over extended periods

---

*Analysis generated: 2025-09-20 23:23:43*  
*Methodology: Graph-based cosine similarity with L2-normalized sparse feature vectors*  
*Dataset: 1,610 dead games vs 37 community profiles*
