#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Demonstration of Dominant Features with Real Feature Names

This script shows what dominant features would look like with real Steam game data,
including publishers, developers, genres, platforms, etc.
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from yoav.dominant_features_profiler import DominantFeaturesProfiler


def create_realistic_demo_data():
    """Create realistic demo data with real Steam feature names."""
    print("[INFO] Creating realistic demo data with real Steam features...")
    
    # Real Steam feature names (from the actual project)
    real_features = [
        # Basic info
        "required_age", "metacritic_score", "recommendations_total", 
        "achievements_total", "dlc_count", "discount_percent",
        "initial_price", "final_price",
        
        # Type
        "type=game", "type=software", "type=", "type=OTHER",
        
        # Free status
        "is_free=False", "is_free=True", "is_free=", "is_free=OTHER",
        
        # Platform support
        "windows=True", "windows=False", "mac=True", "mac=False", 
        "linux=True", "linux=False",
        
        # DLC
        "has_dlc=False", "has_dlc=True", "has_dlc=", "has_dlc=OTHER",
        
        # Genres
        "genres:Indie", "genres:Adventure", "genres:Action", "genres:Casual",
        "genres:Simulation", "genres:Strategy", "genres:RPG", "genres:Free To Play",
        "genres:Early Access", "genres:Sports", "genres:Massively Multiplayer",
        "genres:Racing", "genres:Utilities",
        
        # Categories
        "categories:Indie", "categories:Adventure", "categories:Action",
        "categories:Casual", "categories:Simulation", "categories:Strategy",
        "categories:RPG", "categories:Free To Play", "categories:Early Access",
        
        # Tags
        "tags:Singleplayer", "tags:Indie", "tags:Adventure", "tags:Casual",
        "tags:Action", "tags:2D", "tags:Simulation", "tags:Strategy", "tags:RPG",
        "tags:Story Rich", "tags:Atmospheric", "tags:Multiplayer", "tags:Puzzle",
        "tags:3D", "tags:Cute", "tags:Fantasy", "tags:Exploration", "tags:Anime",
        "tags:Funny", "tags:Pixel Graphics", "tags:Colorful", "tags:First-Person",
        "tags:Free to Play", "tags:Female Protagonist", "tags:Early Access",
        
        # Developers (sample)
        "developers:Valve", "developers:Square Enix", "developers:CAPCOM Co.",
        "developers:Ubisoft Montreal", "developers:THQ Nordic", "developers:Bethesda Softworks",
        "developers:Electronic Arts", "developers:Activision", "developers:Blizzard Entertainment",
        "developers:CD Projekt RED", "developers:Rockstar Games", "developers:Naughty Dog",
        
        # Publishers (sample)
        "publishers:Valve", "publishers:Square Enix", "publishers:CAPCOM Co.",
        "publishers:Ubisoft", "publishers:THQ Nordic", "publishers:Bethesda Softworks",
        "publishers:Electronic Arts", "publishers:Activision", "publishers:Blizzard Entertainment",
        "publishers:CD Projekt", "publishers:Rockstar Games", "publishers:Sony Interactive Entertainment"
    ]
    
    # Create realistic community assignments
    np.random.seed(42)
    n_games = 2000
    n_communities = 6
    
    # Generate community assignments with realistic sizes
    community_sizes = [400, 350, 300, 250, 200, 500]  # Different community sizes
    community_assignments = []
    
    for community_id, size in enumerate(community_sizes):
        for i in range(size):
            community_assignments.append({
                'node_id': f"game_{len(community_assignments):04d}",
                'community_id': community_id,
                'community_size': size
            })
    
    community_df = pd.DataFrame(community_assignments)
    
    # Generate realistic game metadata
    games_metadata = []
    for i in range(n_games):
        games_metadata.append({
            'appid': f"game_{i:04d}",
            'name': f"Demo Game {i}",
            'genres': np.random.choice(['Action', 'RPG', 'Strategy', 'Simulation', 'Indie']),
            'initial_price': np.random.uniform(5, 100),
            'final_price': np.random.uniform(5, 100),
            'metacritic_score': np.random.uniform(50, 100),
            'label_dead_binary': np.random.choice([0, 1], p=[0.7, 0.3])
        })
    
    games_df = pd.DataFrame(games_metadata)
    
    # Create realistic feature matrix with dominant features
    n_features = len(real_features)
    feature_matrix = np.zeros((n_games, n_features))
    
    # Define realistic dominant features for each community
    community_dominant_features = {
        0: {  # Indie Games Community
            'genres:Indie': 0.85,
            'genres:Action': 0.75,
            'windows=True': 0.95,
            'mac=True': 0.60,
            'linux=True': 0.70,
            'is_free=False': 0.80,
            'tags:Singleplayer': 0.90,
            'tags:Indie': 0.85,
            'developers:Valve': 0.15,  # Some Valve games
        },
        1: {  # AAA Action Games Community
            'genres:Action': 0.90,
            'genres:Adventure': 0.70,
            'windows=True': 0.95,
            'mac=False': 0.80,
            'linux=False': 0.85,
            'is_free=False': 0.95,
            'tags:3D': 0.85,
            'tags:Action': 0.90,
            'developers:Ubisoft Montreal': 0.20,
            'publishers:Ubisoft': 0.25,
        },
        2: {  # Strategy Games Community
            'genres:Strategy': 0.88,
            'genres:Simulation': 0.65,
            'windows=True': 0.90,
            'mac=True': 0.50,
            'linux=True': 0.60,
            'is_free=False': 0.85,
            'tags:Strategy': 0.88,
            'tags:Turn-Based Strategy': 0.70,
            'developers:THQ Nordic': 0.18,
        },
        3: {  # RPG Community
            'genres:RPG': 0.92,
            'genres:Adventure': 0.75,
            'windows=True': 0.95,
            'mac=False': 0.70,
            'linux=False': 0.80,
            'is_free=False': 0.90,
            'tags:RPG': 0.92,
            'tags:Story Rich': 0.85,
            'developers:Square Enix': 0.22,
            'publishers:Square Enix': 0.25,
        },
        4: {  # Free-to-Play Community
            'genres:Free To Play': 0.95,
            'genres:Massively Multiplayer': 0.80,
            'windows=True': 0.90,
            'mac=False': 0.85,
            'linux=False': 0.90,
            'is_free=True': 0.95,
            'tags:Free to Play': 0.95,
            'tags:Multiplayer': 0.90,
            'developers:Valve': 0.30,  # Many Valve F2P games
        },
        5: {  # Early Access Community
            'genres:Early Access': 0.90,
            'genres:Indie': 0.80,
            'windows=True': 0.95,
            'mac=True': 0.70,
            'linux=True': 0.75,
            'is_free=False': 0.85,
            'tags:Early Access': 0.90,
            'tags:Indie': 0.80,
            'developers:Valve': 0.25,
        }
    }
    
    # Apply dominant features to communities
    for community_id, dominant_features in community_dominant_features.items():
        community_games = community_df[community_df['community_id'] == community_id]
        game_indices = community_games.index.tolist()
        
        for feature_name, dominance_percentage in dominant_features.items():
            if feature_name in real_features:
                feature_idx = real_features.index(feature_name)
                n_games_in_community = len(game_indices)
                n_dominant = int(n_games_in_community * dominance_percentage)
                
                if n_dominant > 0:
                    dominant_indices = np.random.choice(game_indices, n_dominant, replace=False)
                    feature_matrix[dominant_indices, feature_idx] = 1
    
    # Add some random noise to other features
    noise_indices = np.random.choice(n_games * n_features, int(0.1 * n_games * n_features), replace=False)
    for idx in noise_indices:
        game_idx, feature_idx = divmod(idx, n_features)
        feature_matrix[game_idx, feature_idx] = np.random.choice([0, 1])
    
    # Save realistic demo data
    demo_dir = Path(__file__).parent / "realistic_demo_data"
    demo_dir.mkdir(exist_ok=True)
    
    community_df.to_csv(demo_dir / "community_assignments.csv", index=False)
    games_df.to_csv(demo_dir / "games_metadata.csv", index=False)
    
    # Save feature matrix
    from scipy.sparse import csr_matrix
    sparse_matrix = csr_matrix(feature_matrix)
    import scipy.sparse
    scipy.sparse.save_npz(demo_dir / "X_csr.npz", sparse_matrix)
    
    # Save feature names
    with open(demo_dir / "feature_names.txt", 'w') as f:
        for feature_name in real_features:
            f.write(f"{feature_name}\n")
    
    # Save features metadata
    import json
    features_meta = {
        "n_rows": n_games,
        "n_cols": n_features,
        "feature_names_count": n_features,
        "real_features": True
    }
    with open(demo_dir / "features_meta.json", 'w') as f:
        json.dump(features_meta, f, indent=2)
    
    print(f"[INFO] Realistic demo data created in {demo_dir}")
    print(f"[INFO] {n_games} games across {n_communities} communities")
    print(f"[INFO] {n_features} real Steam features")
    
    return demo_dir / "community_assignments.csv", demo_dir / "games_metadata.csv", demo_dir


def run_realistic_demo():
    """Run the profiler with realistic Steam features."""
    print("="*80)
    print("DOMINANT FEATURES PROFILER - REALISTIC DEMO")
    print("="*80)
    
    # Create realistic demo data
    community_file, metadata_file, features_dir = create_realistic_demo_data()
    
    # Set up output directory
    output_dir = Path(__file__).parent / "realistic_demo_results"
    
    print(f"\n📊 Running analysis with realistic Steam features:")
    print(f"  Communities: {community_file}")
    print(f"  Metadata: {metadata_file}")
    print(f"  Features: {features_dir}")
    print(f"  Output: {output_dir}")
    
    try:
        # Initialize profiler
        profiler = DominantFeaturesProfiler(
            communities_path=community_file,
            metadata_path=metadata_file,
            features_dir=features_dir,
            output_dir=output_dir,
            dominant_threshold=0.7  # 70% threshold
        )
        
        # Run analysis
        results = profiler.run_full_analysis()
        
        # Display summary with real feature names
        print("\n" + "="*80)
        print("REALISTIC DEMO RESULTS - DOMINANT FEATURES")
        print("="*80)
        
        print(f"📈 Communities analyzed: {len(results['community_profiles'])}")
        print(f"🎮 Games processed: {len(results['game_matches'])}")
        
        # Show dominant features for each community with real names
        print(f"\n🔍 Dominant features per community:")
        for community_id, profile in results['community_profiles'].items():
            print(f"\n  🎯 Community {community_id}: {profile['dominant_features_count']} dominant features")
            print(f"     Size: {profile['size']} games")
            print(f"     Dead games: {profile['dead_games_count']} ({profile['dead_games_percentage']:.1%})")
            
            if profile['dominant_features_count'] > 0:
                # Sort features by percentage and show top ones
                sorted_features = sorted(
                    profile['dominant_features'].items(),
                    key=lambda x: x[1]['percentage'],
                    reverse=True
                )
                
                print(f"     Top dominant features:")
                for feature_name, feature_info in sorted_features[:8]:  # Show top 8
                    percentage = feature_info['percentage']
                    count = feature_info['count']
                    total = feature_info['total']
                    
                    # Categorize the feature
                    if feature_name.startswith('genres:'):
                        category = "🎮 Genre"
                        feature_display = feature_name.replace('genres:', '')
                    elif feature_name.startswith('categories:'):
                        category = "📂 Category"
                        feature_display = feature_name.replace('categories:', '')
                    elif feature_name.startswith('tags:'):
                        category = "🏷️ Tag"
                        feature_display = feature_name.replace('tags:', '')
                    elif feature_name.startswith('developers:'):
                        category = "👨‍💻 Developer"
                        feature_display = feature_name.replace('developers:', '')
                    elif feature_name.startswith('publishers:'):
                        category = "🏢 Publisher"
                        feature_display = feature_name.replace('publishers:', '')
                    elif feature_name in ['windows=True', 'mac=True', 'linux=True']:
                        category = "💻 Platform"
                        feature_display = feature_name.replace('=True', '')
                    elif feature_name.startswith('is_free='):
                        category = "💰 Pricing"
                        feature_display = feature_name.replace('is_free=', 'Free: ')
                    else:
                        category = "📊 Other"
                        feature_display = feature_name
                    
                    print(f"       {category}: {feature_display} ({percentage:.1%}, {count}/{total})")
        
        # Show evaluation metrics
        evaluation = results['evaluation']
        if 'match_score_distribution' in evaluation:
            score_dist = evaluation['match_score_distribution']
            print(f"\n📊 Match quality:")
            print(f"  Mean score: {score_dist['mean']:.3f}")
            print(f"  Median score: {score_dist['median']:.3f}")
            print(f"  Score range: [{score_dist['min']:.3f}, {score_dist['max']:.3f}]")
        
        print(f"\n✅ Realistic demo completed successfully!")
        print(f"📁 Results saved to: {output_dir}")
        print(f"📄 Check summary_report.md for detailed insights")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main function."""
    print("🎯 Dominant Features Profiler - Realistic Demo")
    print("="*50)
    print("This demo shows what dominant features look like with real Steam data:")
    print("- Publishers (Valve, Ubisoft, Square Enix, etc.)")
    print("- Developers (Ubisoft Montreal, THQ Nordic, etc.)")
    print("- Genres (Action, RPG, Strategy, Indie, etc.)")
    print("- Platforms (Windows, Mac, Linux)")
    print("- Tags (Singleplayer, Multiplayer, Early Access, etc.)")
    print("- Categories and other Steam features")
    print("="*50)
    
    success = run_realistic_demo()
    
    if success:
        print("\n🎉 Demo completed successfully!")
        print("\nKey insights:")
        print("1. Each community has distinct dominant features")
        print("2. Features include publishers, developers, genres, platforms")
        print("3. Games are matched based on shared dominant features")
        print("4. Results are interpretable and actionable")
    else:
        print("\n💡 Demo failed - check error messages above")


if __name__ == '__main__':
    main()