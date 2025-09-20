#!/usr/bin/env python3
"""
Simple script to split a CSV file into train and test sets.
Usage: python split_csv_train_test.py <input_csv> [--test_size=0.2] [--random_state=42]
"""

import pandas as pd
import argparse
import os
from sklearn.model_selection import train_test_split


def split_csv_train_test(input_csv_path, test_size=0.2, random_state=42, output_dir="out"):
    """
    Split a CSV file into train and test sets and save them with appropriate names.
    
    Args:
        input_csv_path (str): Path to the input CSV file
        test_size (float): Proportion of data to use for testing (default: 0.2)
        random_state (int): Random seed for reproducibility (default: 42)
        output_dir (str): Directory to save output files (default: "out")
    """
    # Read the CSV file
    print(f"Reading CSV file: {input_csv_path}")
    df = pd.read_csv(input_csv_path)
    print(f"Total rows: {len(df)}")
    
    # Split the data
    train_df, test_df = train_test_split(df, test_size=test_size, random_state=random_state)
    
    print(f"Train set size: {len(train_df)} rows ({len(train_df)/len(df)*100:.1f}%)")
    print(f"Test set size: {len(test_df)} rows ({len(test_df)/len(df)*100:.1f}%)")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate output file names based on input file name
    input_filename = os.path.basename(input_csv_path)
    name_without_ext = os.path.splitext(input_filename)[0]
    
    train_filename = f"{name_without_ext}_train.csv"
    test_filename = f"{name_without_ext}_test.csv"
    
    train_path = os.path.join(output_dir, train_filename)
    test_path = os.path.join(output_dir, test_filename)
    
    # Save the split datasets
    train_df.to_csv(train_path, index=False)
    test_df.to_csv(test_path, index=False)
    
    print(f"Train set saved to: {train_path}")
    print(f"Test set saved to: {test_path}")


def main():
    parser = argparse.ArgumentParser(description="Split CSV file into train and test sets")
    parser.add_argument("input_csv", help="Path to input CSV file")
    parser.add_argument("--test_size", type=float, default=0.2, 
                       help="Proportion of data for test set (default: 0.2)")
    parser.add_argument("--random_state", type=int, default=42,
                       help="Random seed for reproducibility (default: 42)")
    parser.add_argument("--output_dir", default="out",
                       help="Output directory for split files (default: out)")
    
    args = parser.parse_args()
    
    # Check if input file exists
    if not os.path.exists(args.input_csv):
        print(f"Error: Input file '{args.input_csv}' does not exist.")
        return 1
    
    try:
        split_csv_train_test(
            input_csv_path=args.input_csv,
            test_size=args.test_size,
            random_state=args.random_state,
            output_dir=args.output_dir
        )
        print("✅ CSV split completed successfully!")
        return 0
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1


if __name__ == "__main__":
    exit(main())