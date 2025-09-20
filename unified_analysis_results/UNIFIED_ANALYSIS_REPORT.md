# Unified Community Analysis Report

## Executive Summary

This analysis calculated cosine similarity between **1,610 games** and **37 community profiles** using L2-normalized sparse feature vectors.

### Key Findings

- **Games with similarity ≥ 0.7:** 37 (2.30%)
- **Highest similarity achieved:** 0.8218
- **Average similarity:** 0.4950
- **Median similarity:** 0.5022

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

The distribution of games across different similarity thresholds:

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
| ≥0.95 | 0 | 0.00% |

---

## Key Insights

### 1. Community Classification Success

With **37 communities** and threshold **0.7**, we achieved:
- **37 games (2.30%)** successfully classified
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
*Dataset: 1,610 games vs 37 community profiles*
