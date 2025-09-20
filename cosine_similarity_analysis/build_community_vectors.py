#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_community_vectors.py

Purpose:
    Convert community profiles JSON into normalized feature vectors compatible 
    with the existing graph_scripts feature vector system. Creates synthetic
    CSV data from community profiles and uses build_feature_vectors.py to
    generate consistent L2-normalized sparse vectors.

Inputs:
    --community-profiles    Path to detailed_community_profiles.json
    --out-dir              Output directory for feature artifacts
    --overall-profile      Path to overall_average_profile.csv (optional)
    
Outputs:
    - community_synthetic.csv     Synthetic CSV representation of communities
    - community_features/         Feature vector artifacts (X_csr.npz, etc.)

Usage:
    python ./build_community_vectors.py \
      --community-profiles community_14_profiles_analysis/detailed_community_profiles.json \
      --out-dir cosine_similarity_analysis/community_features \
      --overall-profile community_14_profiles_analysis/overall_average_profile.csv
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Any
import pandas as pd
import numpy as np
import subprocess
import os

def load_community_profiles(profiles_path: Path) -> Dict[str, Any]:
    """Load community profiles from JSON"""
    with open(profiles_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data['community_profiles']

def load_overall_profile(overall_path: Path) -> pd.DataFrame:
    """Load overall average profile"""
    return pd.read_csv(overall_path)

def extract_top_categorical_values(profile: Dict[str, Any], field: str, top_k: int = 5) -> List[str]:
    """Extract top categorical values from community profile"""
    categorical_features = profile.get('categorical_features', {})
    field_data = categorical_features.get(field, {})
    top_values = field_data.get('top_values', [])
    
    values = []
    for item in top_values[:top_k]:
        if isinstance(item, dict) and 'value' in item:
            values.append(str(item['value']))
    
    return values

def create_synthetic_csv(profiles: Dict[str, Any], overall_profile: pd.DataFrame, 
                        out_path: Path) -> None:
    """Create synthetic CSV from community profiles"""
    
    print(f"[INFO] Creating synthetic CSV for {len(profiles)} communities...")
    
    rows = []
    
    # Add overall average as community -1 (for comparison)
    overall_row = create_overall_row(overall_profile)
    if overall_row:
        rows.append(overall_row)
    
    # Process each community profile
    for community_id, profile in profiles.items():
        row = create_community_row(community_id, profile)
        if row:
            rows.append(row)
    
    # Create DataFrame and save
    df = pd.DataFrame(rows)
    df.to_csv(out_path, index=False)
    
    print(f"[OK] Synthetic CSV saved: {out_path}")
    print(f"[INFO] Created {len(df)} rows ({len(df.columns)} columns)")

def create_overall_row(overall_profile: pd.DataFrame) -> Dict[str, Any]:
    """Create row for overall average profile"""
    row = {'appid': 'OVERALL_AVG', 'community_id': -1}
    
    # Process overall profile data
    for _, entry in overall_profile.iterrows():
        field = entry['field']
        metric = entry['metric']
        value = entry['value']
        
        if metric == 'most_common_across_communities':
            row[field] = str(value)
        elif metric == 'average_across_communities':
            row[field] = float(value)
        elif metric == 'average_true_percentage':
            row[field] = float(value) > 50.0  # Convert percentage to boolean
    
    return row

def create_community_row(community_id: str, profile: Dict[str, Any]) -> Dict[str, Any]:
    """Create row for a single community profile"""
    row = {
        'appid': f'COMMUNITY_{community_id}',
        'community_id': int(community_id),
        'community_size': profile.get('size', 0)
    }
    
    # Categorical features
    categorical_features = profile.get('categorical_features', {})
    for field_name, field_data in categorical_features.items():
        if field_name in ['type', 'coming_soon']:  # Handle simple categoricals
            most_common = field_data.get('most_common', '')
            row[field_name] = str(most_common)
        else:  # Handle multi-value categoricals (genres, tags, etc.)
            top_values = extract_top_categorical_values(profile, field_name, top_k=5)
            row[field_name] = ','.join(top_values) if top_values else ''
    
    # Numerical features  
    numerical_features = profile.get('numerical_features', {})
    for field_name, field_data in numerical_features.items():
        if field_name == 'price':
            # Handle price specially
            avg_price = field_data.get('average_price', 0)
            row['final_price'] = float(avg_price) if avg_price else 0.0
            row['initial_price'] = float(avg_price) if avg_price else 0.0
        else:
            mean_value = field_data.get('mean', 0)
            row[field_name] = float(mean_value) if mean_value is not None else 0.0
    
    # Boolean features
    boolean_features = profile.get('boolean_features', {})
    for field_name, field_data in boolean_features.items():
        percentage = field_data.get('percentage', 0)
        row[field_name] = float(percentage) > 50.0  # Convert to boolean
    
    return row

def build_feature_vectors(synthetic_csv: Path, out_dir: Path) -> None:
    """Use existing build_feature_vectors.py to create feature vectors"""
    
    script_path = Path(__file__).parent.parent / "graph_scripts" / "build_feature_vectors.py"
    
    if not script_path.exists():
        raise FileNotFoundError(f"build_feature_vectors.py not found at {script_path}")
    
    cmd = [
        "python", str(script_path),
        "--in", str(synthetic_csv),
        "--out-dir", str(out_dir),
        "--id-col", "appid",
        "--numeric-cols", "required_age,metacritic_score,recommendations_total,achievements_total,dlc_count,discount_percent,final_price,initial_price,community_size",
        "--multi-cols", "genres,tags,categories,developers,publishers,supported_languages",
        "--onehot-cols", "type,coming_soon",
        "--multi-delim", ",;|",
        "--multi-topk", "50",
        # Use minimal exclusions for communities
        "--exclude-cols", "crawl_timestamp,crawl_status,label_dead,label_dead_binary,avg_players_median_6m,months_used,min_months_required,min_months_ok,first_month_in_window,last_month,pc_min_requirements,controller_support"
    ]
    
    print(f"[INFO] Running build_feature_vectors.py...")
    print(f"[CMD] {' '.join(cmd)}")
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"[ERROR] build_feature_vectors.py failed:", file=sys.stderr)
        print(f"STDOUT: {result.stdout}", file=sys.stderr)
        print(f"STDERR: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    
    print("[OK] Community feature vectors built successfully")
    print(result.stdout)

def main():
    ap = argparse.ArgumentParser(description="Build feature vectors from community profiles")
    ap.add_argument("--community-profiles", required=True, help="Path to detailed_community_profiles.json")
    ap.add_argument("--out-dir", required=True, help="Output directory for feature artifacts")
    ap.add_argument("--overall-profile", help="Path to overall_average_profile.csv")
    
    args = ap.parse_args()
    
    profiles_path = Path(args.community_profiles)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    # Load data
    profiles = load_community_profiles(profiles_path)
    
    overall_profile = None
    if args.overall_profile:
        overall_profile = load_overall_profile(Path(args.overall_profile))
    
    # Create synthetic CSV
    synthetic_csv = out_dir / "community_synthetic.csv"
    create_synthetic_csv(profiles, overall_profile, synthetic_csv)
    
    # Build feature vectors
    features_dir = out_dir / "features"
    build_feature_vectors(synthetic_csv, features_dir)
    
    print(f"[OK] Community vectors built in {out_dir}")

if __name__ == "__main__":
    main()