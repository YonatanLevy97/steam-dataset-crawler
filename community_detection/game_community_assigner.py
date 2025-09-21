#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
game_community_assigner.py

Purpose:
    Assign each game to its best-matching community based on highest cosine similarity.
    For ties, choose the community with the smallest ID (number).

Usage:
    python game_community_assigner.py
"""

import pandas as pd
import numpy as np
from pathlib import Path

def assign_games_to_communities(csv_path: str) -> pd.DataFrame:
    """
    Assign each game to its best-matching community based on highest cosine similarity.
    For ties, choose the community with the smallest ID.
    """
    
    # Load the similarity results
    df = pd.read_csv(csv_path)
    print(f"[INFO] Loaded {len(df)} similarity comparisons")
    print(f"[INFO] Unique games: {df['appid'].nunique()}")
    print(f"[INFO] Unique communities: {df['community_id'].nunique()}")
    
    # Group by game and find the best community for each
    assignments = []
    
    for appid in df['appid'].unique():
        game_data = df[df['appid'] == appid]
        
        # Find the maximum similarity for this game
        max_similarity = game_data['cosine_similarity'].max()
        
        # Get all communities with this maximum similarity
        best_communities = game_data[game_data['cosine_similarity'] == max_similarity]
        
        # If there are ties, choose the community with the smallest ID
        best_community = best_communities.loc[best_communities['community_id'].idxmin()]
        
        assignments.append({
            'appid': appid,
            'assigned_community_id': best_community['community_id'],
            'cosine_similarity': best_community['cosine_similarity'],
            'num_tied_communities': len(best_communities)
        })
    
    return pd.DataFrame(assignments)

def analyze_assignments(assignments_df: pd.DataFrame) -> None:
    """Analyze the community assignments and provide statistics"""
    
    print(f"\n[INFO] Assignment Analysis:")
    print(f"  Total games assigned: {len(assignments_df)}")
    print(f"  Unique communities used: {assignments_df['assigned_community_id'].nunique()}")
    
    # Community distribution
    community_counts = assignments_df['assigned_community_id'].value_counts().sort_index()
    print(f"\n[INFO] Games per community:")
    for community_id, count in community_counts.items():
        print(f"  Community {community_id}: {count} games")
    
    # Similarity statistics
    print(f"\n[INFO] Similarity statistics:")
    print(f"  Mean similarity: {assignments_df['cosine_similarity'].mean():.4f}")
    print(f"  Std similarity: {assignments_df['cosine_similarity'].std():.4f}")
    print(f"  Min similarity: {assignments_df['cosine_similarity'].min():.4f}")
    print(f"  Max similarity: {assignments_df['cosine_similarity'].max():.4f}")
    
    # Tie analysis
    tied_games = assignments_df[assignments_df['num_tied_communities'] > 1]
    print(f"\n[INFO] Tie analysis:")
    print(f"  Games with ties: {len(tied_games)}")
    if len(tied_games) > 0:
        print(f"  Average ties per game: {tied_games['num_tied_communities'].mean():.2f}")
        print(f"  Max ties for a game: {tied_games['num_tied_communities'].max()}")
    
    # Top assignments
    print(f"\n[INFO] Top 10 assignments (by similarity):")
    top_assignments = assignments_df.nlargest(10, 'cosine_similarity')
    for _, row in top_assignments.iterrows():
        print(f"  Game {row['appid']} -> Community {row['assigned_community_id']}: {row['cosine_similarity']:.4f}")

def main():
    """Main function to assign games to communities"""
    
    print("[INFO] Starting game-community assignment...")
    
    # Load and process assignments
    csv_path = "yoav/cosine_similarity_results_proper.csv"
    assignments_df = assign_games_to_communities(csv_path)
    
    # Analyze the assignments
    analyze_assignments(assignments_df)
    
    # Save results
    output_path = Path("yoav/game_community_assignments.csv")
    assignments_df.to_csv(output_path, index=False)
    
    print(f"\n[INFO] Assignments saved to {output_path}")
    print(f"[INFO] Output format: appid, assigned_community_id, cosine_similarity, num_tied_communities")
    
    # Show some examples
    print(f"\n[INFO] Sample assignments:")
    sample = assignments_df.head(10)
    for _, row in sample.iterrows():
        print(f"  Game {row['appid']} -> Community {row['assigned_community_id']} (similarity: {row['cosine_similarity']:.4f})")

if __name__ == "__main__":
    main()