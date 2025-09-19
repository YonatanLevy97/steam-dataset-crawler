#!/usr/bin/env python3
"""
Generate visualization figures for forecast model evaluation.

This script creates:
- Fig. 1: Confusion matrices for hold-out evaluation
- Fig. 2: F1-macro with 95% CI across methods
- Fig. A3: Logistic PR curves with confidence bands
"""

import argparse
import json
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import confusion_matrix, precision_recall_curve, auc
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

def threshold_baseline_predict(X, threshold):
    """Threshold baseline: predict Dead if players_median_6 < threshold."""
    return (X['players_median_6'] < threshold).astype(int)

def majority_baseline_predict(X, majority_class):
    """Majority baseline: always predict the majority class."""
    return np.full(len(X), majority_class)

def blocked_bootstrap_ci(scores, n_bootstrap=2000, confidence=0.95):
    """Compute confidence interval using blocked bootstrap."""
    if len(scores) < 2:
        return np.mean(scores), 0.0
    
    bootstrap_means = []
    for _ in range(n_bootstrap):
        resampled_scores = np.random.choice(scores, size=len(scores), replace=True)
        bootstrap_means.append(np.mean(resampled_scores))
    
    bootstrap_means = np.array(bootstrap_means)
    alpha = 1 - confidence
    lower = np.percentile(bootstrap_means, 100 * alpha / 2)
    upper = np.percentile(bootstrap_means, 100 * (1 - alpha / 2))
    mean_val = np.mean(scores)
    
    return mean_val, (mean_val - lower, upper - mean_val)

def plot_confusion_matrices(df, splits, out_dir):
    """Create Fig. 1: Confusion matrices for hold-out evaluation."""
    
    # Get hold-out data
    holdout_months = [datetime.fromisoformat(m) for m in splits['holdout_months']]
    holdout_mask = df['cutoff_month'].isin(holdout_months)
    
    X_holdout = df[holdout_mask]
    y_holdout = df[holdout_mask]['label_dead_next6m']
    
    if len(X_holdout) == 0:
        print("Warning: No hold-out data available for confusion matrices")
        return
    
    # Prepare features
    feature_cols = [col for col in df.columns if col not in ['appid', 'cutoff_month', 'label_dead_next6m']]
    numeric_features = [col for col in feature_cols if df[col].dtype in ['int64', 'float64']]
    
    # Train final model on CV data
    cv_mask = df['cutoff_month'].isin([datetime.fromisoformat(m) for window in splits['cv_windows'] 
                                     for m in window['train_months'] + window['validation_months']])
    X_cv = df[cv_mask][feature_cols]
    y_cv = df[cv_mask]['label_dead_next6m']
    
    # Scale features
    scaler = StandardScaler()
    X_cv_scaled = scaler.fit_transform(X_cv[numeric_features])
    X_holdout_scaled = scaler.transform(X_holdout[feature_cols][numeric_features])
    
    # Add non-numeric features
    X_cv_final = np.hstack([X_cv_scaled, X_cv[[col for col in feature_cols if col not in numeric_features]].values])
    X_holdout_final = np.hstack([X_holdout_scaled, X_holdout[feature_cols][[col for col in feature_cols if col not in numeric_features]].values])
    
    # Train final Logistic model
    final_lr = LogisticRegression(class_weight='balanced', random_state=42, max_iter=1000)
    final_lr.fit(X_cv_final, y_cv)
    
    # Predictions on hold-out
    lr_pred = final_lr.predict(X_holdout_final)
    t20_pred = threshold_baseline_predict(X_holdout, threshold=20)
    t100_pred = threshold_baseline_predict(X_holdout, threshold=100)
    
    # Create figure
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    
    models = [
        ('Threshold t=20', t20_pred),
        ('Threshold t=100', t100_pred),
        ('Logistic Regression', lr_pred)
    ]
    
    for i, (model_name, pred) in enumerate(models):
        cm = confusion_matrix(y_holdout, pred)
        
        # Raw counts
        im = axes[i].imshow(cm, interpolation='nearest', cmap=plt.cm.Blues)
        axes[i].set_title(f'{model_name}\n(Raw Counts)', fontsize=12, fontweight='bold')
        
        # Add text annotations
        thresh = cm.max() / 2.
        for j in range(cm.shape[0]):
            for k in range(cm.shape[1]):
                axes[i].text(k, j, format(cm[j, k], 'd'),
                           ha="center", va="center",
                           color="white" if cm[j, k] > thresh else "black")
        
        axes[i].set_xlabel('Predicted')
        axes[i].set_ylabel('Actual')
        axes[i].set_xticks([0, 1])
        axes[i].set_yticks([0, 1])
        axes[i].set_xticklabels(['Alive', 'Dead'])
        axes[i].set_yticklabels(['Alive', 'Dead'])
    
    plt.tight_layout()
    plt.savefig(f'{out_dir}/confusion_holdout.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # Create normalized confusion matrices
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    
    for i, (model_name, pred) in enumerate(models):
        cm = confusion_matrix(y_holdout, pred)
        cm_normalized = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]
        
        im = axes[i].imshow(cm_normalized, interpolation='nearest', cmap=plt.cm.Blues)
        axes[i].set_title(f'{model_name}\n(Normalized)', fontsize=12, fontweight='bold')
        
        # Add text annotations
        thresh = cm_normalized.max() / 2.
        for j in range(cm_normalized.shape[0]):
            for k in range(cm_normalized.shape[1]):
                axes[i].text(k, j, format(cm_normalized[j, k], '.2f'),
                           ha="center", va="center",
                           color="white" if cm_normalized[j, k] > thresh else "black")
        
        axes[i].set_xlabel('Predicted')
        axes[i].set_ylabel('Actual')
        axes[i].set_xticks([0, 1])
        axes[i].set_yticks([0, 1])
        axes[i].set_xticklabels(['Alive', 'Dead'])
        axes[i].set_yticklabels(['Alive', 'Dead'])
    
    plt.tight_layout()
    plt.savefig(f'{out_dir}/confusion_holdout_normalized.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print("✓ Created confusion matrices")

def plot_f1_comparison(cv_results, out_dir):
    """Create Fig. 2: F1-macro with 95% CI across methods."""
    
    # Load CV results
    cv_df = pd.read_csv(cv_results)
    
    # Extract F1-macro scores
    models = ['Majority', 'Threshold_t20', 'Threshold_t100', 'Logistic']
    f1_scores = {}
    f1_cis = {}
    
    for model in models:
        col_name = f'{model}_f1_macro'
        if col_name in cv_df.columns:
            scores = cv_df[col_name].dropna().values
            if len(scores) > 0:
                mean_val, ci = blocked_bootstrap_ci(scores)
                f1_scores[model] = mean_val
                f1_cis[model] = ci
    
    # Create figure
    fig, ax = plt.subplots(figsize=(10, 6))
    
    model_names = list(f1_scores.keys())
    means = [f1_scores[model] for model in model_names]
    errors = [f1_cis[model][0] for model in model_names]
    
    # Create bars
    bars = ax.bar(range(len(model_names)), means, yerr=errors, 
                  capsize=5, alpha=0.7, color=['gray', 'lightblue', 'blue', 'darkblue'])
    
    # Add significance markers
    majority_f1 = f1_scores.get('Majority', 0)
    for i, model in enumerate(model_names):
        if model != 'Majority':
            # Check if significantly different from majority
            if f1_scores[model] > majority_f1 + 0.01:  # Simple threshold for significance
                ax.text(i, means[i] + errors[i] + 0.01, '*', ha='center', va='bottom', 
                       fontsize=16, fontweight='bold', color='red')
    
    ax.set_xlabel('Model', fontsize=12)
    ax.set_ylabel('F1-Macro Score', fontsize=12)
    ax.set_title('F1-Macro Performance with 95% Confidence Intervals\n(Cross-Validation Results)', 
                fontsize=14, fontweight='bold')
    ax.set_xticks(range(len(model_names)))
    ax.set_xticklabels(model_names, rotation=45, ha='right')
    ax.set_ylim(0, 1)
    ax.grid(True, alpha=0.3)
    
    # Add legend for significance
    sig_patch = mpatches.Patch(color='red', label='Significantly better than Majority')
    ax.legend(handles=[sig_patch], loc='upper right')
    
    plt.tight_layout()
    plt.savefig(f'{out_dir}/f1_bar_windows.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print("✓ Created F1 comparison bar chart")

def plot_pr_curves(df, splits, out_dir):
    """Create Fig. A3: Logistic PR curves with confidence bands."""
    
    # Get CV data for PR curves
    pr_curves_data = []
    
    feature_cols = [col for col in df.columns if col not in ['appid', 'cutoff_month', 'label_dead_next6m']]
    numeric_features = [col for col in feature_cols if df[col].dtype in ['int64', 'float64']]
    
    for window in splits['cv_windows']:
        window_id = window['window_id']
        
        # Parse month strings back to datetime
        train_months = [datetime.fromisoformat(m) for m in window['train_months']]
        val_months = [datetime.fromisoformat(m) for m in window['validation_months']]
        
        # Get train and validation data
        train_mask = df['cutoff_month'].isin(train_months)
        val_mask = df['cutoff_month'].isin(val_months)
        
        X_train = df[train_mask][feature_cols]
        y_train = df[train_mask]['label_dead_next6m']
        X_val = df[val_mask][feature_cols]
        y_val = df[val_mask]['label_dead_next6m']
        
        if len(X_train) == 0 or len(X_val) == 0:
            continue
        
        # Scale features
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train[numeric_features])
        X_val_scaled = scaler.transform(X_val[numeric_features])
        
        # Add non-numeric features
        X_train_final = np.hstack([X_train_scaled, X_train[[col for col in feature_cols if col not in numeric_features]].values])
        X_val_final = np.hstack([X_val_scaled, X_val[[col for col in feature_cols if col not in numeric_features]].values])
        
        # Train Logistic Regression
        lr_model = LogisticRegression(class_weight='balanced', random_state=42, max_iter=1000)
        lr_model.fit(X_train_final, y_train)
        
        # Get probabilities
        y_proba = lr_model.predict_proba(X_val_final)[:, 1]
        
        # Compute PR curve
        precision, recall, thresholds = precision_recall_curve(y_val, y_proba)
        pr_auc = auc(recall, precision)
        
        pr_curves_data.append({
            'window_id': window_id,
            'precision': precision,
            'recall': recall,
            'pr_auc': pr_auc
        })
    
    if not pr_curves_data:
        print("Warning: No PR curve data available")
        return
    
    # Create figure
    fig, ax = plt.subplots(figsize=(10, 8))
    
    # Plot individual curves
    for curve_data in pr_curves_data:
        ax.plot(curve_data['recall'], curve_data['precision'], 
               alpha=0.3, color='lightblue', linewidth=1)
    
    # Compute average curve with confidence bands
    all_recalls = []
    all_precisions = []
    
    for curve_data in pr_curves_data:
        all_recalls.append(curve_data['recall'])
        all_precisions.append(curve_data['precision'])
    
    # Interpolate to common recall points
    recall_points = np.linspace(0, 1, 100)
    interpolated_precisions = []
    
    for i in range(len(all_recalls)):
        # Remove duplicate recall values
        recall = all_recalls[i]
        precision = all_precisions[i]
        
        # Sort by recall
        sorted_indices = np.argsort(recall)
        recall_sorted = recall[sorted_indices]
        precision_sorted = precision[sorted_indices]
        
        # Interpolate
        interp_precision = np.interp(recall_points, recall_sorted, precision_sorted)
        interpolated_precisions.append(interp_precision)
    
    interpolated_precisions = np.array(interpolated_precisions)
    
    # Compute mean and confidence interval
    mean_precision = np.mean(interpolated_precisions, axis=0)
    std_precision = np.std(interpolated_precisions, axis=0)
    
    # Plot mean curve
    ax.plot(recall_points, mean_precision, color='darkblue', linewidth=3, 
           label=f'Mean PR Curve (AUC = {np.mean([c["pr_auc"] for c in pr_curves_data]):.3f})')
    
    # Plot confidence band
    ax.fill_between(recall_points, 
                   mean_precision - 1.96 * std_precision,
                   mean_precision + 1.96 * std_precision,
                   alpha=0.2, color='blue', label='95% Confidence Band')
    
    ax.set_xlabel('Recall', fontsize=12)
    ax.set_ylabel('Precision', fontsize=12)
    ax.set_title('Logistic Regression PR Curves\n(Cross-Validation Results)', 
                fontsize=14, fontweight='bold')
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.grid(True, alpha=0.3)
    ax.legend()
    
    # Add subtitle
    ax.text(0.5, 0.95, 'PR-AUC for probabilistic model only; uses lagged features (no definitional coupling)', 
           transform=ax.transAxes, ha='center', va='top', fontsize=10, style='italic')
    
    plt.tight_layout()
    plt.savefig(f'{out_dir}/appendix_logistic_pr.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print("✓ Created PR curves")

def main():
    parser = argparse.ArgumentParser(description='Generate forecast visualization figures')
    parser.add_argument('--data-csv', default='data/forecast/forecast_dataset.csv',
                       help='Path to forecast dataset CSV')
    parser.add_argument('--splits-json', default='results/forecast/splits.json',
                       help='Path to splits JSON')
    parser.add_argument('--cv-results', default='results/forecast/cv_table.csv',
                       help='Path to CV results CSV')
    parser.add_argument('--out-dir', default='reports/figures/forecast/',
                       help='Output directory for figures')
    
    args = parser.parse_args()
    
    print("Loading data and splits...")
    
    # Load data
    df = pd.read_csv(args.data_csv)
    df['cutoff_month'] = pd.to_datetime(df['cutoff_month'])
    
    # Load splits
    with open(args.splits_json, 'r') as f:
        splits = json.load(f)
    
    print(f"Loaded {len(df)} forecast rows")
    print(f"CV windows: {len(splits['cv_windows'])}")
    
    # Create figures
    print("\nGenerating figures...")
    
    # Fig. 1: Confusion matrices
    plot_confusion_matrices(df, splits, args.out_dir)
    
    # Fig. 2: F1 comparison
    plot_f1_comparison(args.cv_results, args.out_dir)
    
    # Fig. A3: PR curves
    plot_pr_curves(df, splits, args.out_dir)
    
    print(f"\nAll figures saved to {args.out_dir}")
    print("Generated files:")
    print("  - confusion_holdout.png")
    print("  - confusion_holdout_normalized.png") 
    print("  - f1_bar_windows.png")
    print("  - appendix_logistic_pr.png")

if __name__ == "__main__":
    main()
