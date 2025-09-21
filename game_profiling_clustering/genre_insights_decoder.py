#!/usr/bin/env python3
"""
Genre Insights Decoder
=====================

This script decodes the numeric genre insights from the analysis
and provides meaningful genre-based recommendations.
"""

import pandas as pd
import numpy as np

def decode_genre_insights():
    """Decode genre insights and provide meaningful recommendations."""
    
    # Load the original data to get genre mappings
    df_original = pd.read_csv('dead_labels_enriched.csv')
    
    # Get unique genres and their death rates
    genre_death_rates = df_original.groupby('genres')['label_dead_binary'].agg(['count', 'mean']).reset_index()
    genre_death_rates.columns = ['genre', 'game_count', 'death_rate']
    
    # Filter genres with sufficient sample size (at least 10 games)
    genre_death_rates = genre_death_rates[genre_death_rates['game_count'] >= 10]
    
    # Sort by death rate
    genre_death_rates = genre_death_rates.sort_values('death_rate', ascending=False)
    
    print("GENRE-BASED GAME DEATH ANALYSIS")
    print("=" * 50)
    print(f"Analyzed {len(genre_death_rates)} genres with sufficient sample size")
    print()
    
    # High-risk genres
    high_risk = genre_death_rates.head(10)
    print("HIGH-RISK GENRES (Top 10)")
    print("-" * 30)
    for _, row in high_risk.iterrows():
        print(f"{row['genre']:<25} | Death Rate: {row['death_rate']:.1%} | Games: {row['game_count']}")
    print()
    
    # Low-risk genres
    low_risk = genre_death_rates.tail(10)
    print("LOW-RISK GENRES (Bottom 10)")
    print("-" * 30)
    for _, row in low_risk.iterrows():
        print(f"{row['genre']:<25} | Death Rate: {row['death_rate']:.1%} | Games: {row['game_count']}")
    print()
    
    # Genre insights
    print("GENRE INSIGHTS")
    print("-" * 15)
    
    # Calculate overall death rate
    overall_death_rate = df_original['label_dead_binary'].mean()
    print(f"Overall death rate: {overall_death_rate:.1%}")
    print()
    
    # Most dangerous genre
    most_dangerous = high_risk.iloc[0]
    print(f"Most dangerous genre: {most_dangerous['genre']} ({most_dangerous['death_rate']:.1%} death rate)")
    
    # Safest genre
    safest = low_risk.iloc[0]
    print(f"Safest genre: {safest['genre']} ({safest['death_rate']:.1%} death rate)")
    print()
    
    # Genre recommendations
    print("GENRE RECOMMENDATIONS")
    print("-" * 20)
    print("AVOID these high-risk genres:")
    for _, row in high_risk.head(5).iterrows():
        print(f"  • {row['genre']} ({row['death_rate']:.1%} death rate)")
    print()
    
    print("CONSIDER these low-risk genres:")
    for _, row in low_risk.tail(5).iterrows():
        print(f"  • {row['genre']} ({row['death_rate']:.1%} death rate)")
    print()
    
    # Save detailed results
    genre_death_rates.to_csv('genre_death_analysis.csv', index=False)
    print("Detailed genre analysis saved to: genre_death_analysis.csv")
    
    return genre_death_rates

if __name__ == "__main__":
    decode_genre_insights()