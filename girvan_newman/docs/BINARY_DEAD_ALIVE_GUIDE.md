# Binary Dead vs Alive Community Analysis - Complete Guide

## 🎯 How to Get 2 Communities (Dead vs Alive)

### Quick Start
```bash
# Step 1: Run binary Girvan-Newman (stops at first split = 2 communities)
./run_girvan_newman_binary.sh --edges ./out/graph_runs/.../edges_top100.csv.gz --giant-only

# Step 2: Analyze which community is dead vs alive
python3 detailed_community_feature_analysis.py \
    --communities ./out/binary_test/community_assignments_best.csv \
    --metadata ./out/dead_labels_enriched.csv \
    --out-dir ./out/binary_analysis

# Step 3: Get labeled assignments (DEAD vs ALIVE labels)
python3 interpret_binary_communities.py \
    --analysis ./out/binary_analysis/detailed_feature_analysis.json \
    --communities ./out/binary_test/community_assignments_best.csv \
    --out-dir ./out/binary_interpretation
```

### Key Parameters for Binary Analysis
```bash
./run_girvan_newman_binary.sh \
    --edges YOUR_EDGES.csv.gz \
    --giant-only \              # Focus on main component
    --kcore 2 \                 # Remove poorly connected games  
    --max-communities 1 \       # Stop at first split (2 communities)
    --min-community-size 10     # Avoid tiny communities
```

## 📊 Results from Your Test Data

### Community Distribution (600 games analyzed)
- **Community 0** (576 games, 96% dead): "DEAD GAMES" community
- **Community 1** (24 games, 92% dead): "ALIVE GAMES" community

### Key Findings

#### 🔍 **Separation Quality**: WEAK (4% difference)
- Dead community: 96.0% dead games
- Alive community: 92.0% dead games  
- **Only 4% separation** - not ideal for binary classification

#### 🎮 **Community Characteristics**

**"DEAD" Community (576 games)**
- **Genres**: Early Access (9.2%), Simulation (8.5%), Indie (55.9%)
- **Publishers**: Conglomerate 5 (6.4%), Valve (3.3%)
- **Quality**: Higher Metacritic scores (avg 77.0)
- **Reality**: Still 96% dead - paradoxically higher quality but still failed

**"ALIVE" Community (24 games)**  
- **Genres**: Action (79.2%), Free to Play (8.3%), Indie (50.0%)
- **Publishers**: Scattered small publishers
- **Quality**: Lower Metacritic scores (avg 44.5)
- **Reality**: 92% dead - barely more "alive" than the other group

## 🤔 Why Is the Separation Weak?

### Possible Reasons
1. **Graph structure reflects similarity, not success** - similar games can both be dead or alive
2. **Market timing effects** - even good games can die due to market conditions
3. **Sample bias** - your dataset may be heavily skewed toward dead games
4. **Feature similarity** - dead and alive games might have similar technical characteristics

### This Is Actually Valuable Information!
- Shows that **game similarity ≠ game success**
- Suggests that **network position alone** isn't predictive of survival
- Points to need for **additional features** beyond similarity

## 🚀 How to Use These Results

### Option 1: Use Despite Weak Separation
```python
import pandas as pd

# Load labeled results
df = pd.read_csv('out/binary_interpretation/binary_community_assignments_labeled.csv')

# Use as binary feature
df['is_dead_community'] = (df['community_label'] == 'DEAD').astype(int)

# Even 4% improvement might be valuable for ML models
X = df[['node_id', 'is_dead_community']]
```

### Option 2: Improve Separation Parameters

#### A. Try Different Graph Filters
```bash
# More aggressive filtering
./run_girvan_newman_binary.sh --edges YOUR_EDGES.csv.gz \
    --giant-only --kcore 3 --max-nodes 1000 --min-weight 0.8

# Focus on premium games only  
./run_girvan_newman_binary.sh --edges YOUR_EDGES.csv.gz \
    --giant-only --kcore 5 --max-nodes 500 --min-weight 0.9
```

#### B. Try Different Edge Sources
```bash
# Use full cosine edges instead of top-K
./run_girvan_newman_binary.sh --edges ./out/graph_runs/.../edges_cosine_ge_0p70.csv.gz

# Use different similarity threshold
# Regenerate graph with --threshold 0.8 or 0.9
```

### Option 3: Multi-Level Analysis
```bash
# Get more communities to find better separations
./run_girvan_newman.sh --edges YOUR_EDGES.csv.gz --max-communities 5
```

## 📈 Improving Binary Separation

### Strategy 1: Pre-filter the Graph
Remove games that might confuse the analysis:

```python
# Filter before building graph
metadata = pd.read_csv('./out/dead_labels_enriched.csv')

# Option A: Remove games with ambiguous status  
clear_cases = metadata[
    (metadata['label_dead_binary'] == 0) |  # Clearly alive
    (metadata['label_dead_binary'] == 1)    # Clearly dead  
]

# Option B: Focus on games with enough data
well_documented = metadata[
    (metadata['metacritic_score'].notna()) &
    (metadata['recommendations_total'] > 10)
]
```

### Strategy 2: Use Multiple Similarity Types
Instead of just cosine similarity:

```python
# Combine different similarity metrics
# - Genre similarity
# - Publisher similarity  
# - Price similarity
# - Release date similarity
```

### Strategy 3: Temporal Analysis
```bash
# Analyze community evolution over time
# Run Girvan-Newman on different time periods
# See if dead/alive separation improves in certain periods
```

## 📁 Files Generated

### Core Results
- **`binary_community_assignments_labeled.csv`** - Games labeled as DEAD/ALIVE
- **`binary_interpretation_summary.json`** - Analysis summary with metrics

### Detailed Analysis  
- **`detailed_feature_analysis.json`** - Full feature distributions
- **`*_summary.csv`** - Feature breakdowns by community

### Example Usage
```python
import pandas as pd
import json

# Load labeled assignments
assignments = pd.read_csv('out/binary_interpretation/binary_community_assignments_labeled.csv')

# Load interpretation summary
with open('out/binary_interpretation/binary_interpretation_summary.json') as f:
    summary = json.load(f)

print(f"Separation quality: {summary['separation_quality']:.1%}")
print(f"Dead community size: {summary['dead_community']['size']}")
print(f"Alive community size: {summary['alive_community']['size']}")
```

## 🎯 Recommendations

### For Weak Separation (like your current results):
1. **Use as supplementary feature** - even 4% improvement can help ML models
2. **Try different parameters** - more aggressive filtering might improve separation
3. **Combine with other features** - network position + metadata + temporal features
4. **Analyze failure modes** - understand why similar games have different outcomes

### For Strong Separation (>20% difference):
1. **Use as primary binary classifier** - network communities predict success
2. **Build recommendation systems** - avoid recommending games from "dead" communities  
3. **Investment decisions** - favor games clustering with "alive" community
4. **Market analysis** - understand what drives successful vs failed game clusters

## 🔍 Next Steps

1. **Try improved parameters** using the suggestions above
2. **Analyze temporal patterns** - do communities change over time?
3. **Combine with metadata** - use community + game features for better prediction
4. **Market segmentation** - even weak separation might reveal market niches

The binary analysis framework is ready - now you can experiment with parameters to find the optimal dead/alive separation for your specific use case! 🚀