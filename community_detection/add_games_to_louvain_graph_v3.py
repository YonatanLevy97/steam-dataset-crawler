#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
add_games_to_louvain_graph_v3.py

Purpose:
    Add the 50 games to the existing graph with forced connections,
    run Louvain algorithm and then merge communities to limit to 15,
    and analyze which communities the games end up in.

Usage:
    python add_games_to_louvain_graph_v3.py
"""

import pandas as pd
import numpy as np
import networkx as nx
import gzip
from pathlib import Path
from typing import Dict, List, Set, Tuple
from collections import defaultdict, Counter
import json
import subprocess
import sys

def load_existing_graph(edges_path: str) -> nx.Graph:
    """Load the existing graph from edges CSV"""
    print(f"[INFO] Loading existing graph from {edges_path}")
    
    G = nx.Graph()
    edge_count = 0
    
    with gzip.open(edges_path, 'rt') as f:
        # Skip header
        next(f)
        
        for line in f:
            parts = line.strip().split(',')
            if len(parts) >= 3:
                src, dst, weight = parts[0], parts[1], float(parts[2])
                G.add_edge(src, dst, weight=weight)
                edge_count += 1
    
    print(f"[INFO] Loaded graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    return G

def load_existing_communities(community_assignments_path: str) -> Dict[str, int]:
    """Load existing community assignments"""
    print(f"[INFO] Loading existing community assignments from {community_assignments_path}")
    
    communities = {}
    df = pd.read_csv(community_assignments_path)
    
    for _, row in df.iterrows():
        communities[row['node_id']] = row['community_id']
    
    print(f"[INFO] Loaded {len(communities)} existing community assignments")
    return communities

def load_game_assignments(assignments_path: str) -> pd.DataFrame:
    """Load the game-community assignments we made earlier"""
    print(f"[INFO] Loading game assignments from {assignments_path}")
    
    df = pd.read_csv(assignments_path)
    print(f"[INFO] Loaded {len(df)} game assignments")
    return df

def add_games_to_graph_with_forced_connections(G: nx.Graph, games_df: pd.DataFrame, 
                                              existing_communities: Dict[str, int]) -> nx.Graph:
    """Add the 50 games to the graph with forced connections to ensure integration"""
    
    print(f"[INFO] Adding {len(games_df)} games to the graph with forced connections")
    
    # Get the 50 game appids
    game_appids = games_df['appid'].astype(str).tolist()
    
    # Add games as nodes
    for appid in game_appids:
        G.add_node(appid)
    
    print(f"[INFO] Added {len(game_appids)} game nodes to graph")
    
    # Force connections for each game
    edges_added = 0
    
    for _, row in games_df.iterrows():
        game_appid = str(row['appid'])
        assigned_community = row['assigned_community_id']
        similarity = row['cosine_similarity']
        
        # Find existing nodes in the assigned community
        community_members = [node for node, comm_id in existing_communities.items() 
                           if comm_id == assigned_community]
        
        # Get existing nodes in the community that are in the graph
        community_nodes = [node for node in community_members if node in G.nodes()]
        
        if community_nodes:
            # Sort by degree and connect to top nodes
            degrees = [(node, G.degree(node)) for node in community_nodes]
            degrees.sort(key=lambda x: x[1], reverse=True)
            
            # Force connections to top 5 nodes in the community
            connections_made = 0
            for node, _ in degrees[:5]:
                if node != game_appid:
                    # Use a reasonable weight based on similarity or default
                    weight = max(abs(similarity), 0.3) if similarity > -0.5 else 0.3
                    G.add_edge(game_appid, node, weight=weight)
                    edges_added += 1
                    connections_made += 1
        else:
            # If no community members found, connect to random high-degree nodes
            all_nodes = list(G.nodes())
            if len(all_nodes) > 1:
                # Get top degree nodes
                degrees = [(node, G.degree(node)) for node in all_nodes if node != game_appid]
                degrees.sort(key=lambda x: x[1], reverse=True)
                
                # Connect to top 3 random high-degree nodes
                for node, _ in degrees[:3]:
                    G.add_edge(game_appid, node, weight=0.3)
                    edges_added += 1
    
    print(f"[INFO] Added {edges_added} forced edges connecting games to existing nodes")
    print(f"[INFO] Graph now has {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    
    return G

def run_louvain_and_merge_communities(G: nx.Graph, max_communities: int = 15) -> Tuple[List[Set[str]], float]:
    """Run Louvain algorithm and merge communities to limit to max_communities"""
    
    print(f"[INFO] Running Louvain algorithm and merging to limit communities to {max_communities}")
    print(f"[INFO] Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    
    # Run Louvain with default parameters
    try:
        from networkx.algorithms import community as nx_community
        communities = nx_community.louvain_communities(G, resolution=1.0, seed=42)
        print(f"[INFO] Initial Louvain: {len(communities)} communities")
    except Exception as e:
        print(f"[ERROR] Louvain failed: {e}")
        return [], 0.0
    
    # If we have too many communities, merge them
    if len(communities) > max_communities:
        print(f"[INFO] Merging {len(communities)} communities down to {max_communities}")
        
        # Sort communities by size (largest first)
        communities_sorted = sorted(communities, key=len, reverse=True)
        
        # Keep the largest communities and merge the rest
        merged_communities = communities_sorted[:max_communities-1]  # Keep top N-1
        
        # Merge all remaining communities into one
        remaining_communities = communities_sorted[max_communities-1:]
        if remaining_communities:
            merged_community = set()
            for comm in remaining_communities:
                merged_community.update(comm)
            merged_communities.append(merged_community)
        
        communities = merged_communities
        print(f"[INFO] After merging: {len(communities)} communities")
    
    # Calculate final modularity
    modularity = nx.community.modularity(G, communities)
    print(f"[INFO] Final result: {len(communities)} communities with modularity {modularity:.4f}")
    
    return communities, modularity

def analyze_game_communities(communities: List[Set[str]], game_appids: List[str], 
                           output_dir: Path) -> pd.DataFrame:
    """Analyze which communities the 50 games ended up in"""
    
    print(f"[INFO] Analyzing community assignments for {len(game_appids)} games")
    
    # Create community mapping
    node_to_community = {}
    for i, community in enumerate(communities):
        for node in community:
            node_to_community[node] = i
    
    # Analyze game assignments
    game_analysis = []
    
    for appid in game_appids:
        appid_str = str(appid)
        if appid_str in node_to_community:
            community_id = node_to_community[appid_str]
            community_size = len(communities[community_id])
            
            game_analysis.append({
                'appid': appid,
                'louvain_community_id': community_id,
                'community_size': community_size,
                'in_graph': True
            })
        else:
            game_analysis.append({
                'appid': appid,
                'louvain_community_id': None,
                'community_size': 0,
                'in_graph': False
            })
    
    results_df = pd.DataFrame(game_analysis)
    
    # Save results
    results_path = output_dir / "game_louvain_communities_v3.csv"
    results_df.to_csv(results_path, index=False)
    
    print(f"[INFO] Game community analysis saved to {results_path}")
    
    # Print statistics
    games_in_graph = results_df['in_graph'].sum()
    print(f"[INFO] Games successfully added to graph: {games_in_graph}/{len(game_appids)}")
    
    if games_in_graph > 0:
        community_dist = results_df[results_df['in_graph']]['louvain_community_id'].value_counts().sort_index()
        print(f"[INFO] Community distribution for games:")
        for comm_id, count in community_dist.items():
            print(f"  Community {comm_id}: {count} games")
    
    return results_df

def save_updated_community_assignments(communities: List[Set[str]], output_dir: Path):
    """Save the updated community assignments including the new games"""
    
    print(f"[INFO] Saving updated community assignments")
    
    assignments = []
    for i, community in enumerate(communities):
        for node in community:
            assignments.append({
                'node_id': node,
                'community_id': i,
                'community_size': len(community)
            })
    
    assignments_df = pd.DataFrame(assignments)
    assignments_path = output_dir / "updated_community_assignments_v3.csv"
    assignments_df.to_csv(assignments_path, index=False)
    
    print(f"[INFO] Updated community assignments saved to {assignments_path}")
    print(f"[INFO] Total nodes: {len(assignments_df)}")

def main():
    """Main function to add games to graph and run Louvain with community merging"""
    
    print("[INFO] Starting process to add games to existing graph and run Louvain (max 15 communities)")
    
    # Paths
    edges_path = "out/graph_runs/20250920_224510/edges/edges_top100.csv.gz"
    existing_communities_path = "out/louvain_4_communities_final_20250920_233707/community_assignments.csv"
    game_assignments_path = "yoav/game_community_assignments.csv"
    output_dir = Path("yoav/louvain_with_added_games_v3")
    output_dir.mkdir(exist_ok=True)
    
    # Load existing data
    G = load_existing_graph(edges_path)
    existing_communities = load_existing_communities(existing_communities_path)
    games_df = load_game_assignments(game_assignments_path)
    
    # Get game appids
    game_appids = games_df['appid'].astype(str).tolist()
    
    # Add games to graph with forced connections
    G_updated = add_games_to_graph_with_forced_connections(G, games_df, existing_communities)
    
    # Run Louvain algorithm and merge communities
    communities, modularity = run_louvain_and_merge_communities(G_updated, max_communities=15)
    
    # Analyze game communities
    game_results = analyze_game_communities(communities, game_appids, output_dir)
    
    # Save updated community assignments
    save_updated_community_assignments(communities, output_dir)
    
    # Save metadata
    metadata = {
        'original_graph_nodes': int(G.number_of_nodes()),
        'original_graph_edges': int(G.number_of_edges()),
        'updated_graph_nodes': int(G_updated.number_of_nodes()),
        'updated_graph_edges': int(G_updated.number_of_edges()),
        'games_added': int(len(game_appids)),
        'total_communities': int(len(communities)),
        'modularity': float(modularity),
        'games_in_graph': int(game_results['in_graph'].sum()),
        'max_communities_target': 15,
        'community_limit_achieved': len(communities) <= 15
    }
    
    with open(output_dir / "analysis_metadata_v3.json", 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"\n[INFO] Analysis complete!")
    print(f"[INFO] Original graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    print(f"[INFO] Updated graph: {G_updated.number_of_nodes()} nodes, {G_updated.number_of_edges()} edges")
    print(f"[INFO] Communities detected: {len(communities)} (target: <= 15)")
    print(f"[INFO] Modularity: {modularity:.4f}")
    print(f"[INFO] Games successfully added: {game_results['in_graph'].sum()}/{len(game_appids)}")
    print(f"[INFO] Community limit achieved: {len(communities) <= 15}")
    print(f"[INFO] Results saved to: {output_dir}")

if __name__ == "__main__":
    main()