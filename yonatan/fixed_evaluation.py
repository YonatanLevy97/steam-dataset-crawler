#!/usr/bin/env python3
"""
Clean evaluation for Steam dead games classification - FIXED VERSION.
"""

import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score, precision_recall_curve, auc, precision_score, recall_score, accuracy_score
from sklearn.preprocessing import StandardScaler
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

def load_and_prepare_data():
    """Load game labels and prepare temporal data."""
    labels_df = pd.read_csv('data/game_labels.csv')
    players_df = pd.read_csv('../batches_results/players_data/steamcharts_results_batch_1_apps.csv')
    return labels_df, players_df

def create_temporal_windows(players_df, n_windows=5):
    """Create 5 rolling time-series windows for CV."""
    players_df['month_dt'] = pd.to_datetime(players_df['month'], errors='coerce')
    players_df = players_df.dropna(subset=['month_dt'])
    
    windows = []
    window_size_months = 12
    val_size_months = 3
    
    start_date = pd.Timestamp('2018-01-01')
    
    for i in range(n_windows):
        train_start = start_date + pd.DateOffset(months=i*6)
        train_end = train_start + pd.DateOffset(months=window_size_months)
        val_start = train_end
        val_end = val_start + pd.DateOffset(months=val_size_months)
        
        if val_end <= players_df['month_dt'].max():
            windows.append({
                'train_start': train_start,
                'train_end': train_end,
                'val_start': val_start,
                'val_end': val_end,
                'window_id': i
            })
    
    return windows

def majority_baseline(y_true):
    """Majority class baseline - always predict Dead (label 1)."""
    y_pred = np.ones_like(y_true)  # All Dead
    return y_pred

def threshold_baseline(y_true, player_counts, threshold):
    """Threshold baseline - predict Dead if players < threshold."""
    y_pred = (player_counts < threshold).astype(int)
    return y_pred

def logistic_baseline(player_counts, y_true):
    """Logistic regression baseline with proper probability scores."""
    X = player_counts.reshape(-1, 1)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    lr = LogisticRegression(random_state=42, max_iter=1000)
    lr.fit(X_scaled, y_true)
    y_pred_proba = lr.predict_proba(X_scaled)[:, 1]  # P(Dead)
    y_pred = (y_pred_proba > 0.5).astype(int)
    
    return y_pred, y_pred_proba

def calculate_metrics(y_true, y_pred, y_pred_proba=None, include_pr_auc=False):
    """Calculate evaluation metrics."""
    f1 = f1_score(y_true, y_pred, average='macro')
    precision = precision_score(y_true, y_pred, zero_division=0)
    recall = recall_score(y_true, y_pred, zero_division=0)
    
    metrics = {
        'F1-macro': f1,
        'Recall@Dead': recall,
        'Precision@Dead': precision
    }
    
    # PR-AUC only for probabilistic models
    if include_pr_auc and y_pred_proba is not None and len(np.unique(y_true)) > 1:
        try:
            precision_curve, recall_curve, _ = precision_recall_curve(y_true, y_pred_proba)
            pr_auc = auc(recall_curve, precision_curve)
            metrics['PR-AUC'] = pr_auc
        except Exception as e:
            print(f"PR-AUC calculation failed: {e}")
            metrics['PR-AUC'] = np.nan
    elif include_pr_auc:
        metrics['PR-AUC'] = np.nan
    
    return metrics

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
    window_players = players_df[
        (players_df['month_dt'] >= window_data['train_start']) &
        (players_df['month_dt'] < window_data['val_end'])
    ].copy()
    
    window_games = window_players['appid'].unique()
    window_labels = labels_df[labels_df['appid'].isin(window_games)].copy()
    
    if len(window_labels) < 100:
        return None
    
    y_true = window_labels['label_dead_binary'].values
    player_counts = window_labels['avg_players_median_6m'].values
    
    results = {}
    
    # 1. Majority class baseline
    y_pred_majority = majority_baseline(y_true)
    results['Majority'] = calculate_metrics(y_true, y_pred_majority, include_pr_auc=False)
    
    # 2. Threshold baselines (excluding t=50 as tautological)
    for threshold in [20, 100]:
        y_pred_thresh = threshold_baseline(y_true, player_counts, threshold)
        results[f'Threshold t={threshold}'] = calculate_metrics(y_true, y_pred_thresh, include_pr_auc=False)
    
    # 3. Logistic regression baseline (with probabilities)
    y_pred_lr, y_pred_proba_lr = logistic_baseline(player_counts, y_true)
    results['Logistic Regression'] = calculate_metrics(y_true, y_pred_lr, y_pred_proba_lr, include_pr_auc=True)
    
    return results

def main():
    """Run clean evaluation."""
    print("Loading data...")
    labels_df, players_df = load_and_prepare_data()
    
    print("Creating 5 rolling CV windows...")
    windows = create_temporal_windows(players_df, n_windows=5)
    
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
    
    # Aggregate results across windows - SIMPLIFIED APPROACH
    print(f"\nAggregating results across {len(all_results)} windows...")
    
    # Collect all scores for each method and metric
    method_scores = {}
    
    for method in ['Majority', 'Threshold t=20', 'Threshold t=100', 'Logistic Regression']:
        method_scores[method] = {}
        for metric in ['F1-macro', 'Recall@Dead', 'Precision@Dead', 'PR-AUC']:
            scores = []
            for result in all_results:
                if method in result and metric in result[method]:
                    score = result[method][metric]
                    if not np.isnan(score):
                        scores.append(score)
            
            if scores:
                mean_score, lower, upper = bootstrap_ci(scores)
                method_scores[method][metric] = f"{mean_score:.4f} ± {max(mean_score-lower, upper-mean_score):.4f}"
            else:
                method_scores[method][metric] = "N/A"
    
    # Print clean results table
    print("\n" + "="*80)
    print("03_results_clean")
    print("="*80)
    
    # Create results table with only required columns
    print("\n| Method | F1-macro | Recall@Dead | Precision@Dead | PR-AUC |")
    print("|--------|----------|-------------|----------------|--------|")
    
    for method in ['Majority', 'Threshold t=20', 'Threshold t=100', 'Logistic Regression']:
        row = f"| {method} |"
        
        # F1-macro
        f1_value = method_scores[method].get('F1-macro', 'N/A')
        row += f" {f1_value} |"
        
        # Recall@Dead
        recall_value = method_scores[method].get('Recall@Dead', 'N/A')
        row += f" {recall_value} |"
        
        # Precision@Dead
        precision_value = method_scores[method].get('Precision@Dead', 'N/A')
        row += f" {precision_value} |"
        
        # PR-AUC (only for probabilistic models)
        if method == 'Logistic Regression':
            pr_auc_value = method_scores[method].get('PR-AUC', 'N/A')
            row += f" {pr_auc_value} |"
        else:
            row += f" N/A |"
        
        print(row)
    
    # Statistical significance tests
    print("\n" + "="*60)
    print("STATISTICAL SIGNIFICANCE TESTS")
    print("="*60)
    
    # Compare logistic vs other methods
    logistic_f1_scores = []
    for result in all_results:
        if 'Logistic Regression' in result and 'F1-macro' in result['Logistic Regression']:
            score = result['Logistic Regression']['F1-macro']
            if not np.isnan(score):
                logistic_f1_scores.append(score)
    
    for method in ['Majority', 'Threshold t=20', 'Threshold t=100']:
        method_f1_scores = []
        for result in all_results:
            if method in result and 'F1-macro' in result[method]:
                score = result[method]['F1-macro']
                if not np.isnan(score):
                    method_f1_scores.append(score)
        
        if logistic_f1_scores and method_f1_scores:
            # Paired t-test for F1-macro
            t_stat, p_value = stats.ttest_rel(logistic_f1_scores, method_f1_scores)
            print(f"Logistic vs {method} (F1-macro): t={t_stat:.4f}, p={p_value:.4f}")
    
    return method_scores

if __name__ == "__main__":
    results = main()
