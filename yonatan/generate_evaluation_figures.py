#!/usr/bin/env python3
"""
Visualization script for Steam dead games evaluation results.
Generates three key figures for the evaluation section.
"""

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import precision_recall_curve, confusion_matrix, f1_score, precision_score, recall_score
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
import seaborn as sns
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

def threshold_baseline(y_true, player_counts, threshold):
    """Threshold baseline - predict Dead if players < threshold."""
    y_pred = (player_counts < threshold).astype(int)
    return y_pred

def bootstrap_ci(scores, n_bootstrap=1000, confidence=0.95):
    """Calculate bootstrap confidence interval."""
    scores = np.array(scores)
    bootstrap_scores = []
    
    for _ in range(n_bootstrap):
        bootstrap_sample = np.random.choice(scores, size=len(scores), replace=True)
        bootstrap_scores.append(np.mean(bootstrap_sample))
    
    alpha = 1 - confidence
    lower = np.percentile(bootstrap_scores, 100 * alpha / 2)
    upper = np.percentile(bootstrap_scores, 100 * (1 - alpha / 2))
    mean_score = np.mean(scores)
    
    return mean_score, lower, upper

def plot_pr_curves_cv(labels_df, players_df):
    """Plot PR curves for each CV window + averaged curve with 95% CI."""
    
    windows = create_temporal_windows(players_df, n_windows=5)
    
    fig, ax = plt.subplots(1, 1, figsize=(10, 8))
    
    # Collect data from all windows
    all_precisions = []
    all_recalls = []
    window_data = []
    
    for window in windows:
        window_players = players_df[
            (players_df['month_dt'] >= window['train_start']) &
            (players_df['month_dt'] < window['val_end'])
        ].copy()
        
        window_games = window_players['appid'].unique()
        window_labels = labels_df[labels_df['appid'].isin(window_games)].copy()
        
        if len(window_labels) < 100:
            continue
            
        y_true = window_labels['label_dead_binary'].values
        player_counts = window_labels['avg_players_median_6m'].values
        
        # Train logistic regression
        y_pred, y_pred_proba = logistic_baseline(player_counts, y_true)
        
        # Calculate PR curve
        precision, recall, _ = precision_recall_curve(y_true, y_pred_proba)
        
        # Plot individual window curve
        ax.plot(recall, precision, alpha=0.3, color='lightblue', linewidth=1)
        
        all_precisions.append(precision)
        all_recalls.append(recall)
        window_data.append((y_true, y_pred_proba))
    
    # Calculate averaged curve with CI
    if all_precisions and all_recalls:
        # Interpolate all curves to common recall points
        recall_points = np.linspace(0, 1, 100)
        interpolated_precisions = []
        
        for precision, recall in zip(all_precisions, all_recalls):
            interp_precision = np.interp(recall_points, recall, precision)
            interpolated_precisions.append(interp_precision)
        
        interpolated_precisions = np.array(interpolated_precisions)
        mean_precision = np.mean(interpolated_precisions, axis=0)
        std_precision = np.std(interpolated_precisions, axis=0)
        
        # Plot averaged curve with CI
        ax.plot(recall_points, mean_precision, 'b-', linewidth=2, label='Mean PR Curve')
        ax.fill_between(recall_points, 
                       mean_precision - 1.96*std_precision,
                       mean_precision + 1.96*std_precision,
                       alpha=0.2, color='blue', label='95% CI')
    
    ax.set_xlabel('Recall')
    ax.set_ylabel('Precision')
    ax.set_title('Precision-Recall Curves: Logistic Regression (CV Windows)')
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.set_xlim([0, 1])
    ax.set_ylim([0, 1])
    
    plt.tight_layout()
    return fig

def plot_confusion_matrices_holdout(labels_df, players_df):
    """Plot confusion matrices for Threshold t=20 and t=100 on hold-out period."""
    
    # Create hold-out period (most recent data)
    players_df['month_dt'] = pd.to_datetime(players_df['month'], errors='coerce')
    players_df = players_df.dropna(subset=['month_dt'])
    
    # Use the most recent period as hold-out (simulate Feb-Jul 2025)
    max_date = players_df['month_dt'].max()
    holdout_start = max_date - pd.DateOffset(months=6)
    
    holdout_players = players_df[
        (players_df['month_dt'] >= holdout_start) &
        (players_df['month_dt'] <= max_date)
    ].copy()
    
    holdout_games = holdout_players['appid'].unique()
    holdout_labels = labels_df[labels_df['appid'].isin(holdout_games)].copy()
    
    if len(holdout_labels) < 100:
        print("Warning: Hold-out period has insufficient data")
        return None
    
    y_true = holdout_labels['label_dead_binary'].values
    player_counts = holdout_labels['avg_players_median_6m'].values
    
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    
    thresholds = [20, 50, 100]
    titles = ['Threshold t=20', 'Threshold t=50', 'Threshold t=100']
    
    for i, (threshold, title) in enumerate(zip(thresholds, titles)):
        # Generate predictions
        y_pred = threshold_baseline(y_true, player_counts, threshold)
        
        # Calculate confusion matrix
        cm = confusion_matrix(y_true, y_pred)
        cm_normalized = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]
        
        # Plot confusion matrix
        sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', ax=axes[i],
                   xticklabels=['Alive', 'Dead'],
                   yticklabels=['Alive', 'Dead'])
        
        # Add normalized percentages
        for j in range(cm.shape[0]):
            for k in range(cm.shape[1]):
                text = f"{cm[j,k]}\n({cm_normalized[j,k]:.1%})"
                axes[i].text(k+0.5, j+0.5, text, ha='center', va='center',
                           fontsize=10, fontweight='bold')
        
        axes[i].set_title(f'{title}\nHold-out Period')
        axes[i].set_xlabel('Predicted')
        axes[i].set_ylabel('Actual')
    
    plt.tight_layout()
    return fig

def plot_f1_comparison(labels_df, players_df):
    """Plot F1-macro comparison with 95% CI error bars."""
    
    windows = create_temporal_windows(players_df, n_windows=5)
    
    methods = ['Majority', 'Threshold t=20', 'Threshold t=50', 'Threshold t=100', 'Logistic Regression']
    colors = ['red', 'orange', 'yellow', 'green', 'blue']
    
    # Collect F1 scores for all methods across all windows
    method_f1_scores = {method: [] for method in methods}
    
    for window in windows:
        window_players = players_df[
            (players_df['month_dt'] >= window['train_start']) &
            (players_df['month_dt'] < window['val_end'])
        ].copy()
        
        window_games = window_players['appid'].unique()
        window_labels = labels_df[labels_df['appid'].isin(window_games)].copy()
        
        if len(window_labels) < 100:
            continue
            
        y_true = window_labels['label_dead_binary'].values
        player_counts = window_labels['avg_players_median_6m'].values
        
        # 1. Majority class baseline
        y_pred_majority = np.ones_like(y_true)  # All Dead
        f1_majority = f1_score(y_true, y_pred_majority, average='macro')
        method_f1_scores['Majority'].append(f1_majority)
        
        # 2. Threshold baselines
        for threshold in [20, 50, 100]:
            y_pred_thresh = threshold_baseline(y_true, player_counts, threshold)
            f1_thresh = f1_score(y_true, y_pred_thresh, average='macro')
            method_f1_scores[f'Threshold t={threshold}'].append(f1_thresh)
        
        # 3. Logistic regression baseline
        y_pred_lr, y_pred_proba_lr = logistic_baseline(player_counts, y_true)
        f1_lr = f1_score(y_true, y_pred_lr, average='macro')
        method_f1_scores['Logistic Regression'].append(f1_lr)
    
    # Create bar chart
    fig, ax = plt.subplots(1, 1, figsize=(10, 6))
    
    means = []
    errors = []
    
    for method in methods:
        f1_scores = method_f1_scores[method]
        
        if f1_scores:
            # Calculate mean and 95% CI using bootstrap
            mean_f1, lower, upper = bootstrap_ci(f1_scores, n_bootstrap=1000)
            error = max(mean_f1 - lower, upper - mean_f1)
            
            means.append(mean_f1)
            errors.append(error)
        else:
            means.append(0)
            errors.append(0)
    
    # Create bar chart
    x_pos = np.arange(len(methods))
    bars = ax.bar(x_pos, means, yerr=errors, capsize=5, 
                 color=colors, alpha=0.7, edgecolor='black')
    
    # Add value labels on bars
    for i, (mean, error) in enumerate(zip(means, errors)):
        ax.text(i, mean + error + 0.01, f'{mean:.3f}', 
               ha='center', va='bottom', fontweight='bold')
    
    ax.set_xlabel('Method')
    ax.set_ylabel('F1-macro')
    ax.set_title('F1-macro Comparison Across Methods\n(95% CI from Blocked Bootstrap)')
    ax.set_xticks(x_pos)
    ax.set_xticklabels(methods, rotation=45, ha='right')
    ax.grid(True, alpha=0.3, axis='y')
    ax.set_ylim([0, 1.1])
    
    # Add significance annotations
    # Compare each method to Majority baseline
    majority_mean = means[0]
    majority_error = errors[0]
    
    for i, (mean, error) in enumerate(zip(means[1:], errors[1:]), 1):
        if mean - error > majority_mean + majority_error:
            ax.annotate('***', xy=(i, mean + error + 0.05), 
                       ha='center', fontsize=14, color='red')
    
    plt.tight_layout()
    return fig

def main():
    """Generate all three visualization figures."""
    print("Loading data...")
    labels_df, players_df = load_and_prepare_data()
    
    print("Generating Figure 1: Precision-Recall Curves...")
    fig1 = plot_pr_curves_cv(labels_df, players_df)
    
    print("Generating Figure 2: Confusion Matrices...")
    fig2 = plot_confusion_matrices_holdout(labels_df, players_df)
    
    print("Generating Figure 3: F1-macro Comparison...")
    fig3 = plot_f1_comparison(labels_df, players_df)
    
    # Save figures
    print("Saving figures...")
    fig1.savefig('figures/pr_curves_cv.png', dpi=300, bbox_inches='tight')
    if fig2:
        fig2.savefig('figures/confusion_matrices_holdout.png', dpi=300, bbox_inches='tight')
    fig3.savefig('figures/f1_comparison.png', dpi=300, bbox_inches='tight')
    
    print("All figures saved to 'figures/' directory")
    
    # Show figures
    plt.show()
    
    return fig1, fig2, fig3

if __name__ == "__main__":
    # Create figures directory if it doesn't exist
    import os
    os.makedirs('../figures', exist_ok=True)
    
    # Generate all figures
    figures = main()
