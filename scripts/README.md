# CSV Train/Test Split Script

## Overview
Simple Python script to split a CSV file into train and test sets.

## Usage

### Basic Usage
```bash
python scripts/split_csv_train_test.py <input_csv>
```

### With Custom Parameters
```bash
python scripts/split_csv_train_test.py <input_csv> --test_size=0.3 --random_state=123 --output_dir=data
```

## Parameters
- `input_csv`: Path to the input CSV file (required)
- `--test_size`: Proportion of data for test set (default: 0.2 = 20%)
- `--random_state`: Random seed for reproducibility (default: 42)
- `--output_dir`: Output directory for split files (default: "out")

## Example
```bash
# Split dead_games_only.csv into 80% train / 20% test
python scripts/split_csv_train_test.py out/dead_games_only.csv

# Results:
# - out/dead_games_only_train.csv (80% of data)
# - out/dead_games_only_test.csv (20% of data)
```

## Output Files
The script automatically generates output filenames based on the input filename:
- Input: `dead_games_only.csv` → Outputs: `dead_games_only_train.csv` and `dead_games_only_test.csv`
- Input: `my_data.csv` → Outputs: `my_data_train.csv` and `my_data_test.csv`

## Dependencies
- pandas
- scikit-learn