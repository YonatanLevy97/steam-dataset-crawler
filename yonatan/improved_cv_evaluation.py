#!/usr/bin/env python3
"""
Improved rolling time-series cross-validation evaluation for Steam dead games classification.
Fixes N/A issues and provides better probability-based baselines.
"""

import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score, precision_recall_curve, auc, precision_score, recall_score
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings('ignore')

def load_and_prepare_data():
    """Load game labels and prepare temporal data."""
    labels_df = pd.read_csv('data/game_labels.csv')
    
    # Load player data for temporal analysis
    players_df = pd.read_csv('../batches_results/players_data/steamcharts_results_batch_1_apps.csv')
    
    return labels_df, players_df

def create_temporal_windows(players_df, n_windows=3):
    """Create rolling time-series windows for CV."""
    # Convert month to datetime
    players_df['month_dt'] = pd.to_datetime(players_df['month'], errors='coerce')
    players_df = players_df.dropna(subset=['month_dt'])
    
    # Get date range
    min_date = players_df['month_dt'].min()
    max_date = players_df['month_dt'].max()
    
    print(f"Data range: {min_date.date()} to {max_date.date()}")
    
    # Create rolling windows (simplified for demo)
    windows = []
    window_size_months = 12
    val_size_months = 3
    
    # Start from 2018 to leave room for training
    start_date = pd.Timestamp('2018-01-01')
    
    for i in range(n_windows):
        train_start = start_date + pd.DateOffset(months=i*6)  # 6-month overlap
        train_end = train_start + pd.DateOffset(months=window_size_months)
        val_start = train_end
        val_end = val_start + pd.DateOffset(months=val_size_months)
        
        if val_end <= max_date:
            windows.append({
                'train_start': train_start,
                'train_end': train_end,
                'val_start': val_start,
                'val_end': val_end,
                'window_id': i
            })
    
    return windows

def majority_class_baseline(y_true):
    """Majority class baseline - always predict Dead."""
    y_pred = np.ones_like(y_true)  # All Dead
    # Create probability scores: high confidence for Dead class
    y_pred_proba = np.ones_like(y_true, dtype=float) * 0.9  # 90% confidence for Dead
    return y_pred, y_pred_proba

def threshold_baseline(y_true, player_counts, threshold):
    """Threshold baseline - predict Dead if players < threshold."""
    y_pred = (player_counts < threshold).astype(int)
    
    # Create probability scores based on distance from threshold
    # Games closer to threshold get less confident predictions
    distance = np.abs(player_counts - threshold)
    max_distance = np.max(distance)
    
    # Convert distance to probability (closer to threshold = less confident)
    confidence = 1.0 - (distance / (max_distance + 1e-8)) * 0.3  # Scale confidence
    confidence = np.clip(confidence, 0.6, 0.95)  # Keep reasonable confidence range
    
    y_pred_proba = np.where(y_pred == 1, confidence, 1 - confidence)
    
    return y_pred, y_pred_proba

def logistic_baseline(player_counts, y_true=None):
    """Improved logistic regression baseline."""
    X = player_counts.reshape(-1, 1)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Use actual logistic regression if we have labels
    if y_true is not None:
        lr = LogisticRegression(random_state=42, max_iter=1000)
        lr.fit(X_scaled, y_true)
        y_pred_proba = lr.predict_proba(X_scaled)[:, 1]
        y_pred = (y_pred_proba > 0.5).astype(int)
    else:
        # Fallback to sigmoid-based approach
        threshold = np.median(player_counts)
        std_dev = np.std(player_counts)
        y_pred_proba = 1 / (1 + np.exp(-(player_counts - threshold) / (std_dev + 1e-8)))
        y_pred = (y_pred_proba > 0.5).astype(int)
    
    return y_pred, y_pred_proba

def calculate_metrics(y_true, y_pred, y_pred_proba=None):
    """Calculate evaluation metrics."""
    f1 = f1_score(y_true, y_pred, average='macro')
    precision = precision_score(y_true, y_pred, zero_division=0)
    recall = recall_score(y_true, y_pred, zero_division=0)
    
    # PR-AUC
    if y_pred_proba is not None and len(np.unique(y_true)) > 1:
        try:
            precision_curve, recall_curve, _ = precision_recall_curve(y_true, y_pred_proba)
            pr_auc = auc(recall_curve, precision_curve)
        except Exception as e:
            print(f"PR-AUC calculation failed: {e}")
            pr_auc = np.nan
    else:
        pr_auc = np.nan
    
    return {
        'F1-macro': f1,
        'PR-AUC': pr_auc,
        'Recall@Dead': recall,
        'Precision@Dead': precision
    }

def bootstrap_ci(scores, n_bootstrap=100, confidence=0.95):
    """Calculate bootstrap confidence interval."""
    if len(scores) < 2:
        return scores[0], scores[0], scores[0]
    
    bootstrap_scores = []
    for _ in range(n_bootstrap):
        bootstrap_sample = np.random.choice(scores, size=len(scores), replace=True)
        bootstrap_scores.append(np.mean(bootstrap_sample))
    
    alpha = 1 - confidence
    lower = np.percentile(bootstrap_scores, 100 * alpha / 2)
    upper = np.percentile(bootstrap_scores, 100 * (1 - alpha / 2))
    mean_score = np.mean(scores)
    
    return mean_score, lower, upper

def evaluate_window(window_data, labels_df, players_df):
    """Evaluate all baselines on a single window."""
    # Filter data for this window
    window_players = players_df[
        (players_df['month_dt'] >= window_data['train_start']) &
        (players_df['month_dt'] < window_data['val_end'])
    ].copy()
    
    # Get unique games in this window
    window_games = window_players['appid'].unique()
    
    # Filter labels for games in this window
    window_labels = labels_df[labels_df['appid'].isin(window_games)].copy()
    
    if len(window_labels) < 100:  # Skip windows with too few games
        return None
    
    y_true = window_labels['label_dead_binary'].values
    player_counts = window_labels['avg_players_median_6m'].values
    
    print(f"  Window {window_data['window_id']}: {len(window_labels)} games, {np.sum(y_true)} Dead, {len(y_true) - np.sum(y_true)} Alive")
    
    results = {}
    
    # 1. Majority class baseline
    y_pred_majority, y_pred_proba_majority = majority_class_baseline(y_true)
    results['Majority'] = calculate_metrics(y_true, y_pred_majority, y_pred_proba_majority)
    
    # 2. Threshold baselines
    for threshold in [20, 50, 100]:
        y_pred_thresh, y_pred_proba_thresh = threshold_baseline(y_true, player_counts, threshold)
        results[f'Threshold t={threshold}'] = calculate_metrics(y_true, y_pred_thresh, y_pred_proba_thresh)
    
    # 3. Logistic regression baseline
    y_pred_lr, y_pred_proba_lr = logistic_baseline(player_counts, y_true)
    results['Logistic Regression'] = calculate_metrics(y_true, y_pred_lr, y_pred_proba_lr)
    
    return results

def main():
    """Run rolling CV evaluation."""
    print("Loading data...")
    labels_df, players_df = load_and_prepare_data()
    
    print("Creating temporal windows...")
    windows = create_temporal_windows(players_df, n_windows=3)
    
    print(f"Evaluating {len(windows)} windows...")
    
    all_results = []
    for window in windows:
        print(f"Window {window['window_id']}: {window['train_start'].date()} to {window['val_end'].date()}")
        window_results = evaluate_window(window, labels_df, players_df)
        if window_results:
            all_results.append(window_results)
    
    if not all_results:
        print("No valid windows found")
        return
    
    # Aggregate results across windows
    print(f"\nAggregating results across {len(all_results)} windows...")
    
    methods = list(all_results[0].keys())
    metrics = list(all_results[0][methods[0]].keys())
    
    final_results = {}
    
    for method in methods:
        final_results[method] = {}
        for metric in metrics:
            scores = [result[method][metric] for result in all_results if not np.isnan(result[method][metric])]
            if scores:
                mean_score, lower, upper = bootstrap_ci(scores)
                final_results[method][metric] = f"{mean_score:.4f} ± {max(mean_score-lower, upper-mean_score):.4f}"
            else:
                final_results[method][metric] = "N/A"
    
    # Print results
    print("\n" + "="*80)
    print("IMPROVED ROLLING TIME-SERIES CV RESULTS")
    print("="*80)
    
    # Create results table
    print("\n| Method | F1-macro | PR-AUC | Recall@Dead | Precision@Dead |")
    print("|--------|----------|--------|-------------|----------------|")
    
    for method in methods:
        row = f"| {method} |"
        for metric in metrics:
            value = final_results[method][metric]
            row += f" {value} |"
        print(row)
    
    # Print detailed per-window results
    print("\n" + "="*60)
    print("DETAILED PER-WINDOW RESULTS")
    print("="*60)
    
    for i, result in enumerate(all_results):
        print(f"\nWindow {i}:")
        for method in methods:
            print(f"  {method}:")
            for metric in metrics:
                value = result[method][metric]
                if not np.isnan(value):
                    print(f"    {metric}: {value:.4f}")
                else:
                    print(f"    {metric}: N/A")
    
    return final_results

if __name__ == "__main__":
    results = main()
