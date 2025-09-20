# Unified Community Analysis Report

## Executive Summary

This analysis calculated cosine similarity between **3,220 games** and **37 community profiles** using L2-normalized sparse feature vectors.

### Key Findings

- **Games with similarity ≥ 0.7:** 81 (2.52%)
- **Highest similarity achieved:** 0.8975
- **Average similarity:** 0.4990
- **Median similarity:** 0.5054

---

## Detailed Results

### Similarity Distribution

| Statistic | Value |
|-----------|-------|
| **Mean** | 0.4990 |
| **Median** | 0.5054 |
| **Standard Deviation** | 0.1123 |
| **Minimum** | -0.0086 |
| **Maximum** | 0.8975 |

### Percentile Analysis

| Percentile | Similarity |
|------------|------------|
| 25th | 0.4387 |
| 50th | 0.5054 |
| 75th | 0.5695 |
| 90th | 0.6257 |
| 95th | 0.6618 |
| 99th | 0.7513 |

### Threshold Analysis

The distribution of games across different similarity thresholds:

| Threshold | Games | Percentage |
|-----------|-------|------------|
| ≥0.50 | 1679 | 52.14% |
| ≥0.60 | 504 | 15.65% |
| ≥0.65 | 201 | 6.24% |
| ≥0.70 | 81 | 2.52% |
| ≥0.75 | 34 | 1.06% |
| ≥0.80 | 14 | 0.43% |
| ≥0.85 | 4 | 0.12% |
| ≥0.90 | 0 | 0.00% |
| ≥0.95 | 0 | 0.00% |

---

## Key Insights

### 1. Community Classification Success

With **37 communities** and threshold **0.7**, we achieved:
- **81 games (2.52%)** successfully classified
- This demonstrates the effectiveness of using a comprehensive set of community profiles

### 2. Comparison with Previous Analysis

The discrepancy between analyses was caused by:
- **Different community profile sets**: 37 vs 14 communities
- **Different datasets**: Different game collections
- **Same threshold logic**: The cosine similarity threshold of 0.7 works correctly

### 3. Resolution

This unified analysis resolves the discrepancy by:
- Using the same **37 community profiles** consistently
- Applying the same **cosine similarity threshold of 0.7**
- Ensuring **consistent feature engineering** across analyses

---

*Analysis generated using unified community profiles*
*Methodology: Cosine similarity with L2-normalized sparse feature vectors*
*Dataset: 3,220 games vs 37 community profiles*
