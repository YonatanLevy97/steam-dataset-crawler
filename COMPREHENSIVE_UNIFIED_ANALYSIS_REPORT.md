# Dead Games Community Analysis - Comprehensive Unified Report

## Executive Summary

This report presents a comprehensive analysis of **16 communities** containing **6,113 dead games** from the Steam platform, analyzed using Louvain community detection with complete feature vector analysis.

### Key Findings:
- **Community Structure**: 16 distinct communities (avg size: 382 games)
- **Pricing Strategy**: Average final price $35.25 across all games
- **Quality Distribution**: Average Metacritic score 70.8/100
- **Platform Focus**: 98.0% Windows, 34.3% Mac, 25.3% Linux support

---

## 🏗️ Community Structure Analysis

### Size Distribution:
- **Total Communities**: 16
- **Total Games**: 6,113
- **Average Community Size**: 382 games
- **Median Community Size**: 341 games
- **Size Range**: 82 - 966 games
- **Standard Deviation**: 253.3

### Genre Clustering Patterns:

- **Indie**: 14 communities (12, 11, 5, ...+11 more)
- **Action**: 1 communities (2)
- **RPG**: 1 communities (14)

---

## 💰 Comprehensive Pricing Analysis

### Overall Price Statistics:
- **Average Final Price**: $35.25
- **Median Final Price**: $30.35
- **Price Range**: $20.47 - $178.75
- **Price Standard Deviation**: $31.70

### Community Price Tiers:

- **Mid-range ($15-$35)**: 14 communities ([0, 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15])
- **Premium ($35-$60)**: 1 communities ([2])
- **Luxury ($60+)**: 1 communities ([14])

### Community-by-Community Price Analysis:

| Community | Size | Final Price | Initial Price | Discount | Coverage |
|-----------|------|-------------|---------------|----------|----------|
| 0 | 127 | $22.06 | $22.06 | 0.0% | 94.5% |
| 1 | 921 | $30.51 | $86.55 | 64.7% | 99.7% |
| 2 | 209 | $47.43 | $48.88 | 3.0% | 94.7% |
| 3 | 373 | $22.03 | $28.54 | 22.8% | 91.2% |
| 4 | 455 | $30.66 | $33.31 | 8.0% | 95.4% |
| 5 | 136 | $27.88 | $34.47 | 19.1% | 91.2% |
| 6 | 454 | $30.35 | $31.15 | 2.6% | 91.8% |
| 7 | 249 | $28.23 | $32.29 | 12.6% | 94.8% |
| 8 | 502 | $27.56 | $31.94 | 13.7% | 93.0% |
| 9 | 548 | $26.96 | $31.54 | 14.5% | 92.5% |
| 10 | 371 | $21.18 | $21.83 | 3.0% | 92.7% |
| 11 | 131 | $20.47 | $22.99 | 11.0% | 91.6% |
| 12 | 311 | $23.39 | $24.82 | 5.8% | 93.6% |
| 13 | 966 | $30.43 | $31.12 | 2.2% | 93.3% |
| 14 | 278 | $178.75 | $174.84 | -2.2% | 100.0% |
| 15 | 82 | $28.87 | $32.81 | 12.0% | 92.7% |

---

## ⭐ Quality & Engagement Analysis

### Metacritic Score Analysis:
- **Average Score**: 70.8/100
- **Median Score**: 74.2/100

### Score Distribution:
- **Excellent (80+)**: 190 games
- **Good (70-79)**: 263 games
- **Mixed (60-69)**: 0 games
- **Poor (<60)**: 209 games

### Player Engagement:
- **Average Player Count**: 7.1
- **Median Player Count**: 6.7
- **High Engagement Communities**: 0 (>50 avg players)

---

## 💻 Platform Ecosystem Analysis

### Platform Support Coverage:
- **Windows**: 98.0% (across 16 communities)
- **Mac**: 34.3% (across 16 communities)
- **Linux**: 25.3% (across 16 communities)

### Cross-Platform Strategy Insights:
- **High Cross-Platform Communities**: 1 communities ([1])
- **Windows-Focused Communities**: 8 communities ([0, 2, 4, 5, 9]...)

---

## 🔍 Key Insights & Strategic Implications

### Market Segmentation:
1. **Indie Dominance**: 1/16 communities are Indie-focused, indicating Steam's indie-centric dead game ecosystem
2. **Pricing Stratification**: Clear price tiers from budget ($0-$15) to luxury ($60+) segments
3. **Quality Distribution**: Average Metacritic score of 70.8 suggests moderate quality across dead games
4. **Platform Strategy**: Windows-first approach with selective cross-platform support
5. **Community Size Variance**: Large variation in community sizes (std: 253.3) indicates diverse market niches

### Business Model Patterns:
- **Discounting Strategy**: 10/16 communities show significant discounting (>5%)
- **Premium Positioning**: 1 communities maintain luxury pricing ($60+)
- **Market Coverage**: Price range spans $20.47 to $178.75

---

## 📊 Data Sources & Methodology

- **Dataset**: `out/dead_games_only_train.csv` (Dead Games Training Set)
- **Algorithm**: Louvain Community Detection (resolution=0.05)
- **Features Analyzed**: Complete feature vector including pricing, quality, platform support
- **Analysis Scope**: 16 communities, 6,113 games
- **Price Data Coverage**: 90-100% across communities

*Report generated on: 2025-09-20 17:39:27*