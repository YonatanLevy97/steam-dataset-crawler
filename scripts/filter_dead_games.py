#!/usr/bin/env python3
"""
Script to filter CSV file to keep only dead games.
Takes the enriched CSV and creates a new CSV with only dead games.
"""

import pandas as pd
import sys
from pathlib import Path

def filter_dead_games(input_file, output_file=None):
    """
    Filter CSV to keep only dead games (where label_dead == 'Dead' or label_dead_binary == 1)
    
    Args:
        input_file (str): Path to input CSV file
        output_file (str, optional): Path to output CSV file. If None, creates output in same directory
    """
    
    input_path = Path(input_file)
    
    if not input_path.exists():
        print(f"Error: Input file '{input_file}' not found!")
        sys.exit(1)
    
    # Set default output file if not provided
    if output_file is None:
        output_file = input_path.parent / f"{input_path.stem}_dead_only{input_path.suffix}"
    
    print(f"Reading CSV file: {input_file}")
    
    try:
        # Read the CSV file
        df = pd.read_csv(input_file)
        print(f"Total games in dataset: {len(df)}")
        
        # Check which columns exist for filtering
        if 'label_dead' in df.columns:
            dead_games = df[df['label_dead'] == 'Dead']
            filter_column = 'label_dead'
            filter_value = 'Dead'
        elif 'label_dead_binary' in df.columns:
            dead_games = df[df['label_dead_binary'] == 1]
            filter_column = 'label_dead_binary'
            filter_value = 1
        else:
            print("Error: Neither 'label_dead' nor 'label_dead_binary' column found in the CSV!")
            print(f"Available columns: {list(df.columns)}")
            sys.exit(1)
        
        print(f"Dead games found: {len(dead_games)}")
        print(f"Filtering by: {filter_column} == {filter_value}")
        
        # Save filtered data
        dead_games.to_csv(output_file, index=False)
        print(f"Dead games saved to: {output_file}")
        
        # Show some statistics
        if len(dead_games) > 0:
            print(f"\nFiltered dataset statistics:")
            print(f"- Total dead games: {len(dead_games)}")
            if 'name' in dead_games.columns:
                print(f"- Sample games: {list(dead_games['name'].head(3))}")
        
    except Exception as e:
        print(f"Error processing CSV file: {e}")
        sys.exit(1)

def main():
    """Main function to handle command line arguments"""
    
    if len(sys.argv) < 2:
        print("Usage: python filter_dead_games.py <input_csv_file> [output_csv_file]")
        print("Example: python filter_dead_games.py dead_labels_enriched.csv dead_games_only.csv")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    filter_dead_games(input_file, output_file)

if __name__ == "__main__":
    main()