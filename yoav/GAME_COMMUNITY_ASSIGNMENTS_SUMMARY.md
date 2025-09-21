# Game-Community Assignments Summary

## Overview
This analysis assigned each of the 50 games to their best-matching community based on highest cosine similarity, with ties broken by choosing the smallest community ID.

## Assignment Results

### Distribution Summary
- **Total Games**: 50
- **Communities Used**: 3 out of 14 available communities
- **Games with Ties**: 48 out of 50 games (96%)

### Community Distribution
| Community ID | Number of Games | Percentage |
|--------------|----------------|------------|
| Community 1 | 35 games | 70% |
| Community 0 | 12 games | 24% |
| Community 2 | 3 games | 6% |

### Similarity Statistics
- **Mean Similarity**: -0.1064
- **Standard Deviation**: 0.0775
- **Minimum Similarity**: -0.2268
- **Maximum Similarity**: 0.0893

### Tie Analysis
- **Games with Ties**: 48 games
- **Average Ties per Game**: 5.10 communities
- **Maximum Ties**: 11 communities (for some games)

## Top Assignments (by Similarity)

| Rank | Game ID | Assigned Community | Similarity | Tied Communities |
|------|---------|-------------------|------------|------------------|
| 1 | 813530 | Community 2 | 0.0893 | 10 |
| 2 | 22700 | Community 1 | 0.0439 | 3 |
| 3 | 37940 | Community 1 | 0.0207 | 3 |
| 4 | 716390 | Community 1 | 0.0207 | 3 |
| 5 | 628040 | Community 1 | 0.0180 | 3 |
| 6 | 2743970 | Community 0 | 0.0005 | 11 |
| 7 | 496450 | Community 1 | -0.0001 | 3 |
| 8 | 2613060 | Community 0 | -0.0210 | 11 |
| 9 | 1039960 | Community 0 | -0.0323 | 11 |
| 10 | 529640 | Community 1 | -0.0440 | 3 |

## Key Findings

### 1. Community Concentration
- **Community 1 dominates**: 70% of games assigned to Community 1
- **Limited diversity**: Only 3 out of 14 communities received assignments
- **Community 2**: Only 3 games but includes the highest similarity match

### 2. Tie Frequency
- **High tie rate**: 96% of games had multiple communities with identical similarity scores
- **Tie resolution**: Successfully resolved by choosing smallest community ID
- **Complex ties**: Some games tied with up to 11 different communities

### 3. Similarity Patterns
- **Mostly negative**: Most similarities are negative, indicating orthogonal feature patterns
- **Low variance**: Small standard deviation suggests consistent patterns
- **Best match**: Game 813530 has the only positive similarity (0.0893)

### 4. Assignment Quality
- **Clear winner**: Game 813530 clearly belongs to Community 2
- **Ambiguous cases**: Most games have very similar similarities across communities
- **Tie resolution**: Successfully handled using smallest community ID rule

## Files Generated
- `yoav/game_community_assignments.csv` - Main assignment results
- `yoav/game_community_assigner.py` - Assignment script
- `yoav/GAME_COMMUNITY_ASSIGNMENTS_SUMMARY.md` - This summary

## Output Format
The assignment CSV contains:
- `appid`: Game identifier
- `assigned_community_id`: Best-matching community ID
- `cosine_similarity`: Similarity score with assigned community
- `num_tied_communities`: Number of communities that tied for best match

## Usage
To reproduce the assignments:
```bash
python yoav/game_community_assigner.py
```

The script automatically:
- Loads cosine similarity results
- Finds best community for each game
- Resolves ties using smallest community ID
- Provides detailed analysis and statistics
- Saves results to CSV format