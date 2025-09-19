#!/usr/bin/env python3
"""
Methodologically correct visualization script for Steam dead games evaluation.
Aligned with course principles - proper time-aware evaluation with no tautological baselines.
"""

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import confusion_matrix, f1_score, precision_recall_curve, auc
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

def auto_detect_players_column(df):
    """Auto-detect the players column name."""
    candidates = ['avg_players', 'players', 'avg_players_median', 'avg_players_mean']
    for col in candidates:
        if col in df.columns:
            return col
    raise ValueError(f"Could not find players column. Available columns: {list(df.columns)}")

def labels_and_feature_for_period(players_df, start, end, label_threshold=50.0):
    """
    Compute labels and features for a specific time period.
    
    Args:
        players_df: DataFrame with appid, month, and players columns
        start: Start date (inclusive)
        end: End date (exclusive)
        label_threshold: Threshold for Dead classification
    
    Returns:
        DataFrame with appid, median_players_period, label_dead_binary
    """
    # Filter to period [start, end)
    period_data = players_df[
        (players_df['month_dt'] >= start) & 
        (players_df['month_dt'] < end)
    ].copy()
    
    if len(period_data) == 0:
        return pd.DataFrame(columns=['appid', 'median_players_period', 'label_dead_binary'])
    
    # Group by appid and compute median players for this period
    players_col = auto_detect_players_column(period_data)
    
    period_summary = period_data.groupby('appid')[players_col].agg(['median', 'count']).reset_index()
    period_summary.columns = ['appid', 'median_players_period', 'month_count']
    
    # Only include games with at least 3 months of data
    period_summary = period_summary[period_summary['month_count'] >= 3]
    
    # Create binary labels
    period_summary['label_dead_binary'] = (period_summary['median_players_period'] < label_threshold).astype(int)
    
    return period_summary[['appid', 'median_players_period', 'label_dead_binary']]

def create_temporal_windows(players_df, n_windows=5):
    """Create 5 rolling time-series windows for CV."""
    players_df['month_dt'] = pd.to_datetime(players_df['month'], errors='coerce')
    players_df = players_df.dropna(subset=['month_dt'])
    
    windows = []
    train_size_months = 12
    val_size_months = 3
    step_months = 6
    
    start_date = pd.Timestamp('2018-01-01')
    
    for i in range(n_windows):
        train_start = start_date + pd.DateOffset(months=i*step_months)
        train_end = train_start + pd.DateOffset(months=train_size_months)
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

def evaluate_logistic_baseline(train_data, val_data):
    """Train logistic regression on train_data and evaluate on val_data."""
    # Intersect appids for fairness
    common_appids = set(train_data['appid']).intersection(set(val_data['appid']))
    train_subset = train_data[train_data['appid'].isin(common_appids)]
    val_subset = val_data[val_data['appid'].isin(common_appids)]
    
    if len(train_subset) < 10 or len(val_subset) < 10:
        return np.nan
    
    # Prepare features and labels
    X_train = train_subset['median_players_period'].values.reshape(-1, 1)
    y_train = train_subset['label_dead_binary'].values
    X_val = val_subset['median_players_period'].values.reshape(-1, 1)
    y_val = val_subset['label_dead_binary'].values
    
    # Scale features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_val_scaled = scaler.transform(X_val)
    
    # Train logistic regression
    lr = LogisticRegression(random_state=42, max_iter=1000)
    lr.fit(X_train_scaled, y_train)
    
    # Predict on validation set
    y_pred = lr.predict(X_val_scaled)
    
    # Compute F1-macro
    f1 = f1_score(y_val, y_pred, average='macro')
    
    return f1

def evaluate_threshold_baseline(val_data, threshold):
    """Evaluate threshold baseline on validation data."""
    if len(val_data) < 10:
        return np.nan
    
    y_true = val_data['label_dead_binary'].values
    y_pred = (val_data['median_players_period'] < threshold).astype(int)
    
    f1 = f1_score(y_true, y_pred, average='macro')
    return f1

def evaluate_majority_baseline(val_data):
    """Evaluate majority class baseline (always predict Dead=1)."""
    if len(val_data) < 10:
        return np.nan
    
    y_true = val_data['label_dead_binary'].values
    y_pred = np.ones_like(y_true)  # All Dead
    
    f1 = f1_score(y_true, y_pred, average='macro')
    return f1

def bootstrap_ci(scores, n_bootstrap=1000, confidence=0.95):
    """Calculate bootstrap confidence interval over window-level scores."""
    scores = np.array(scores)
    valid_scores = scores[~np.isnan(scores)]
    
    if len(valid_scores) < 2:
        return np.nan, np.nan, np.nan
    
    bootstrap_scores = []
    for _ in range(n_bootstrap):
        bootstrap_sample = np.random.choice(valid_scores, size=len(valid_scores), replace=True)
        bootstrap_scores.append(np.mean(bootstrap_sample))
    
    alpha = 1 - confidence
    lower = np.percentile(bootstrap_scores, 100 * alpha / 2)
    upper = np.percentile(bootstrap_scores, 100 * (1 - alpha / 2))
    mean_score = np.mean(valid_scores)
    
    return mean_score, lower, upper

def plot_confusion_matrices_holdout(players_df):
    """Figure 1: Confusion matrices for Threshold t=20 and t=100 on hold-out period."""
    
    # Define hold-out period (last 6 months)
    max_date = players_df['month_dt'].max()
    holdout_start = max_date - pd.DateOffset(months=6)
    holdout_end = max_date
    
    print(f"Hold-out period: {holdout_start.date()} to {holdout_end.date()}")
    
    # Get hold-out data
    holdout_data = labels_and_feature_for_period(players_df, holdout_start, holdout_end, label_threshold=50.0)
    
    if len(holdout_data) < 100:
        print(f"Warning: Hold-out period has only {len(holdout_data)} games")
        return None
    
    y_true = holdout_data['label_dead_binary'].values
    player_counts = holdout_data['median_players_period'].values
    
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    
    thresholds = [20, 100]
    titles = ['Threshold t=20', 'Threshold t=100']
    
    for i, (threshold, title) in enumerate(zip(thresholds, titles)):
        # Generate predictions
        y_pred = (player_counts < threshold).astype(int)
        
        # Calculate confusion matrix
        cm = confusion_matrix(y_true, y_pred)
        cm_normalized = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]
        
        # Plot confusion matrix using matplotlib
        im = axes[i].imshow(cm, interpolation='nearest', cmap='Blues')
        axes[i].figure.colorbar(im, ax=axes[i])
        
        # Add text annotations
        thresh = cm.max() / 2.
        for j in range(cm.shape[0]):
            for k in range(cm.shape[1]):
                text = f"{cm[j,k]}\n({cm_normalized[j,k]:.1%})"
                axes[i].text(k, j, text, ha='center', va='center',
                           color='white' if cm[j,k] > thresh else 'black',
                           fontsize=12, fontweight='bold')
        
        axes[i].set_title(f'{title}\nHold-out Period', fontsize=14, fontweight='bold')
        axes[i].set_xlabel('Predicted', fontsize=12)
        axes[i].set_ylabel('Actual', fontsize=12)
        axes[i].set_xticks([0, 1])
        axes[i].set_yticks([0, 1])
        axes[i].set_xticklabels(['Alive', 'Dead'])
        axes[i].set_yticklabels(['Alive', 'Dead'])
    
    plt.tight_layout()
    return fig

def plot_f1_comparison(players_df):
    """Figure 2: F1-macro comparison with 95% CI error bars."""
    
    windows = create_temporal_windows(players_df, n_windows=5)
    
    print(f"Created {len(windows)} CV windows")
    
    methods = ['Majority', 'Threshold t=20', 'Threshold t=100', 'Logistic Regression']
    colors = ['red', 'orange', 'green', 'blue']
    
    # Collect F1 scores for all methods across all windows
    method_f1_scores = {method: [] for method in methods}
    
    for window in windows:
        print(f"\nWindow {window['window_id']}: {window['train_start'].date()} to {window['val_end'].date()}")
        
        # Get train and validation data for this window
        train_data = labels_and_feature_for_period(players_df, window['train_start'], window['train_end'])
        val_data = labels_and_feature_for_period(players_df, window['val_start'], window['val_end'])
        
        if len(val_data) < 100:
            print(f"Warning: Window {window['window_id']} has only {len(val_data)} validation games")
            continue
        
        # Evaluate all methods
        f1_majority = evaluate_majority_baseline(val_data)
        f1_t20 = evaluate_threshold_baseline(val_data, 20)
        f1_t100 = evaluate_threshold_baseline(val_data, 100)
        f1_logistic = evaluate_logistic_baseline(train_data, val_data)
        
        # Store results
        method_f1_scores['Majority'].append(f1_majority)
        method_f1_scores['Threshold t=20'].append(f1_t20)
        method_f1_scores['Threshold t=100'].append(f1_t100)
        method_f1_scores['Logistic Regression'].append(f1_logistic)
        
        # Print per-window results
        print(f"  Majority: {f1_majority:.4f}")
        print(f"  Threshold t=20: {f1_t20:.4f}")
        print(f"  Threshold t=100: {f1_t100:.4f}")
        print(f"  Logistic: {f1_logistic:.4f}")
    
    # Create bar chart
    fig, ax = plt.subplots(1, 1, figsize=(10, 6))
    
    means = []
    errors = []
    
    print(f"\nFinal Results (mean ± 95% CI):")
    for method in methods:
        f1_scores = method_f1_scores[method]
        
        if f1_scores:
            # Calculate mean and 95% CI using bootstrap
            mean_f1, lower, upper = bootstrap_ci(f1_scores, n_bootstrap=1000)
            error = max(mean_f1 - lower, upper - mean_f1)
            
            means.append(mean_f1)
            errors.append(error)
            
            print(f"  {method}: {mean_f1:.4f} ± {error:.4f}")
        else:
            means.append(0)
            errors.append(0)
            print(f"  {method}: No valid scores")
    
    # Create bar chart
    x_pos = np.arange(len(methods))
    bars = ax.bar(x_pos, means, yerr=errors, capsize=5, 
                 color=colors, alpha=0.7, edgecolor='black', linewidth=1)
    
    # Add value labels on bars
    for i, (mean, error) in enumerate(zip(means, errors)):
        ax.text(i, mean + error + 0.01, f'{mean:.3f}', 
               ha='center', va='bottom', fontweight='bold', fontsize=11)
    
    ax.set_xlabel('Method', fontsize=12, fontweight='bold')
    ax.set_ylabel('F1-macro', fontsize=12, fontweight='bold')
    ax.set_title('F1-macro Comparison Across Methods\n(95% CI from Blocked Bootstrap)', 
                fontsize=14, fontweight='bold')
    ax.set_xticks(x_pos)
    ax.set_xticklabels(methods, rotation=45, ha='right', fontsize=11)
    ax.grid(True, alpha=0.3, axis='y')
    ax.set_ylim([0, 1.1])
    
    # Add significance annotations (only if p < 0.05)
    majority_f1_scores = method_f1_scores['Majority']
    
    for i, method in enumerate(methods[1:], 1):  # Skip majority
        method_f1_scores_list = method_f1_scores[method]
        
        if majority_f1_scores and method_f1_scores_list:
            # Paired t-test vs majority
            t_stat, p_value = stats.ttest_rel(method_f1_scores_list, majority_f1_scores)
            
            if p_value < 0.05:
                ax.annotate('*', xy=(i, means[i] + errors[i] + 0.05), 
                           ha='center', fontsize=16, color='red', fontweight='bold')
    
    plt.tight_layout()
    return fig

def plot_logistic_pr_curve_appendix(players_df):
    """Figure A3: Logistic PR curve for Appendix only (illustrative - definitional coupling)."""
    
    windows = create_temporal_windows(players_df, n_windows=5)
    
    fig, ax = plt.subplots(1, 1, figsize=(10, 8))
    
    # Collect data from all windows
    all_precisions = []
    all_recalls = []
    
    for window in windows:
        # Get train and validation data for this window
        train_data = labels_and_feature_for_period(players_df, window['train_start'], window['train_end'])
        val_data = labels_and_feature_for_period(players_df, window['val_start'], window['val_end'])
        
        # Intersect appids for fairness
        common_appids = set(train_data['appid']).intersection(set(val_data['appid']))
        train_subset = train_data[train_data['appid'].isin(common_appids)]
        val_subset = val_data[val_data['appid'].isin(common_appids)]
        
        if len(train_subset) < 10 or len(val_subset) < 10:
            continue
        
        # Prepare features and labels
        X_train = train_subset['median_players_period'].values.reshape(-1, 1)
        y_train = train_subset['label_dead_binary'].values
        X_val = val_subset['median_players_period'].values.reshape(-1, 1)
        y_val = val_subset['label_dead_binary'].values
        
        # Scale features
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_val_scaled = scaler.transform(X_val)
        
        # Train logistic regression
        lr = LogisticRegression(random_state=42, max_iter=1000)
        lr.fit(X_train_scaled, y_train)
        
        # Get probability scores
        y_pred_proba = lr.predict_proba(X_val_scaled)[:, 1]
        
        # Calculate PR curve
        precision, recall, _ = precision_recall_curve(y_val, y_pred_proba)
        
        # Plot individual window curve
        ax.plot(recall, precision, alpha=0.3, color='lightblue', linewidth=1)
        
        all_precisions.append(precision)
        all_recalls.append(recall)
    
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
    
    ax.set_xlabel('Recall', fontsize=12, fontweight='bold')
    ax.set_ylabel('Precision', fontsize=12, fontweight='bold')
    ax.set_title('Logistic Regression PR Curve (Illustrative)\nDefinitional Coupling: Median Feature Relates to Label', 
                fontsize=14, fontweight='bold')
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    ax.set_xlim([0, 1])
    ax.set_ylim([0, 1])
    
    plt.tight_layout()
    return fig

def main():
    """Generate all visualization figures."""
    print("Loading data...")
    players_df = pd.read_csv('../batches_results/players_data/steamcharts_results_batch_1_apps.csv')
    
    # Auto-detect players column
    players_col = auto_detect_players_column(players_df)
    print(f"Using players column: {players_col}")
    
    # Parse dates
    players_df['month_dt'] = pd.to_datetime(players_df['month'], errors='coerce')
    players_df = players_df.dropna(subset=['month_dt'])
    
    print(f"Data range: {players_df['month_dt'].min().date()} to {players_df['month_dt'].max().date()}")
    print(f"Total games: {players_df['appid'].nunique()}")
    
    print("\nGenerating Figure 1: Confusion Matrices (Hold-out)...")
    fig1 = plot_confusion_matrices_holdout(players_df)
    
    print("\nGenerating Figure 2: F1-macro Comparison...")
    fig2 = plot_f1_comparison(players_df)
    
    print("\nGenerating Figure A3: Logistic PR Curve (Appendix)...")
    fig3 = plot_logistic_pr_curve_appendix(players_df)
    
    # Save figures
    print("\nSaving figures...")
    if fig1:
        fig1.savefig('reports/figures/confusion_holdout.png', dpi=300, bbox_inches='tight')
    fig2.savefig('reports/figures/f1_bar_windows.png', dpi=300, bbox_inches='tight')
    fig3.savefig('reports/figures/appendix_logistic_pr.png', dpi=300, bbox_inches='tight')
    
    print("All figures saved to 'reports/figures/' directory")
    
    return fig1, fig2, fig3

if __name__ == "__main__":
    # Create directories if they don't exist
    import os
    os.makedirs('../reports/figures', exist_ok=True)
    
    # Generate all figures
    figures = main()
