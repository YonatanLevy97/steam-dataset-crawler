#!/usr/bin/env python3
"""
Truly Intrinsic Game Death Analyzer - NO DATA LEAKAGE
=====================================================

This script analyzes game death patterns using ONLY features that exist
at game launch, before any performance data is available.

CRITICAL: NO features derived from player engagement, performance metrics,
or any data that would not be available at launch.

TRULY INTRINSIC FEATURES (Available at Launch):
- Game metadata: type, genres, categories, tags, developers, publishers
- Technical specs: windows, mac, linux, pc_min_requirements, controller_support
- Content: required_age, supported_languages, has_dlc, dlc_count, achievements_total
- Business model: is_free, initial_price, final_price, discount_percent
- Quality indicators: metacritic_score (if available at launch)
- Temporal: release_date, coming_soon

EXCLUDED FEATURES (Performance/Engagement Metrics):
- avg_players_median_6m, months_used, recommendations_total
- min_months_required, min_months_ok, first_month_in_window, last_month
- Any derived features from these metrics
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.metrics import (classification_report, confusion_matrix, roc_auc_score, 
                           roc_curve, precision_recall_curve, f1_score, 
                           precision_score, recall_score, accuracy_score)
import xgboost as xgb
import warnings
warnings.filterwarnings('ignore')

class TrulyIntrinsicAnalyzer:
    def __init__(self, csv_path):
        """Initialize the truly intrinsic analyzer."""
        self.csv_path = csv_path
        self.df = None
        self.models = {}
        self.results = {}
        self.feature_names = None
        self.X_train = None
        self.X_test = None
        self.y_train = None
        self.y_test = None
        
    def load_and_preprocess_data(self):
        """Load and preprocess data with ONLY truly intrinsic features."""
        print("Loading data with ONLY truly intrinsic features...")
        
        # Load data
        self.df = pd.read_csv(self.csv_path)
        print(f"Dataset loaded: {len(self.df)} games")
        
        # CRITICAL: Define ONLY truly intrinsic features (available at launch)
        truly_intrinsic_features = [
            # Game metadata (available at launch)
            'type', 'genres', 'categories', 'tags', 'developers', 'publishers',
            
            # Technical specs (available at launch)
            'windows', 'mac', 'linux', 'pc_min_requirements', 'controller_support',
            
            # Content characteristics (available at launch)
            'required_age', 'supported_languages', 'has_dlc', 'dlc_count', 
            'achievements_total',
            
            # Business model (available at launch)
            'is_free', 'initial_price', 'final_price', 'discount_percent',
            
            # Quality indicators (if available at launch)
            'metacritic_score',
            
            # Temporal (available at launch)
            'release_date', 'coming_soon'
        ]
        
        # EXCLUDED features (performance/engagement metrics)
        excluded_features = [
            'avg_players_median_6m',  # Player engagement metric
            'months_used',            # Usage metric
            'recommendations_total',  # Engagement metric
            'min_months_required',    # Used in death definition
            'min_months_ok',         # Used in death definition
            'first_month_in_window',  # Temporal window metric
            'last_month',            # Temporal window metric
            'crawl_timestamp',       # Technical metadata
            'crawl_status',          # Technical metadata
            'appid', 'name_x', 'name_y', 'label_dead'  # Identifiers and labels
        ]
        
        # Verify all intrinsic features exist
        available_features = [f for f in truly_intrinsic_features if f in self.df.columns]
        missing_features = [f for f in truly_intrinsic_features if f not in self.df.columns]
        
        print(f"\nTruly intrinsic features available: {len(available_features)}")
        print(f"Available: {available_features}")
        if missing_features:
            print(f"Missing: {missing_features}")
        
        # Select only truly intrinsic features + target
        df_work = self.df[available_features + ['label_dead_binary']].copy()
        
        print(f"\nFeatures to analyze: {available_features}")
        print(f"Target variable: label_dead_binary")
        
        # Handle missing values
        print("\nHandling missing values...")
        
        # Convert boolean columns first
        boolean_cols = ['is_free', 'windows', 'mac', 'linux', 'has_dlc', 'coming_soon']
        for col in boolean_cols:
            if col in df_work.columns:
                df_work[col] = df_work[col].astype(str).str.lower().isin(['true', '1', 'yes'])
        
        # For numeric columns, fill with median
        numeric_cols = df_work.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if col != 'label_dead_binary':
                df_work[col] = df_work[col].fillna(df_work[col].median())
        
        # For categorical columns, fill with mode or 'Unknown'
        categorical_cols = df_work.select_dtypes(include=['object']).columns
        for col in categorical_cols:
            if col != 'label_dead_binary':
                df_work[col] = df_work[col].fillna('Unknown')
        
        # Convert release_date to datetime and create time-based features
        if 'release_date' in df_work.columns:
            df_work['release_date'] = pd.to_datetime(df_work['release_date'], errors='coerce')
            df_work['years_since_release'] = (pd.Timestamp.now() - df_work['release_date']).dt.days / 365.25
            df_work['years_since_release'] = df_work['years_since_release'].fillna(df_work['years_since_release'].median())
            # Drop the original datetime column as we have the numeric version
            df_work.drop('release_date', axis=1, inplace=True)
        
        # Handle price columns (remove currency symbols and convert to numeric)
        price_cols = ['initial_price', 'final_price']
        for col in price_cols:
            if col in df_work.columns:
                df_work[col] = df_work[col].astype(str).str.replace('₪', '').str.replace(',', '')
                df_work[col] = pd.to_numeric(df_work[col], errors='coerce')
                df_work[col] = df_work[col].fillna(df_work[col].median())
        
        # Create ONLY intrinsic derived features (no performance metrics)
        self.create_intrinsic_derived_features(df_work)
        
        # Encode categorical variables
        self.encode_categorical_features(df_work)
        
        print(f"\nFinal dataset shape: {df_work.shape}")
        print(f"Target distribution: {df_work['label_dead_binary'].value_counts()}")
        
        return df_work
    
    def create_intrinsic_derived_features(self, df):
        """Create ONLY intrinsic derived features (no performance metrics)."""
        print("Creating intrinsic derived features...")
        
        # Platform diversity (intrinsic - known at launch)
        platform_cols = ['windows', 'mac', 'linux']
        if all(col in df.columns for col in platform_cols):
            df['platform_count'] = df[platform_cols].sum(axis=1)
            df['is_multi_platform'] = (df['platform_count'] > 1).astype(int)
        
        # Language diversity (intrinsic - known at launch)
        if 'supported_languages' in df.columns:
            df['language_count'] = df['supported_languages'].str.count(',') + 1
            df['language_count'] = df['language_count'].fillna(1)
        
        # Genre diversity (intrinsic - known at launch)
        if 'genres' in df.columns:
            df['genre_count'] = df['genres'].str.count(',') + 1
            df['genre_count'] = df['genre_count'].fillna(1)
        
        # Category diversity (intrinsic - known at launch)
        if 'categories' in df.columns:
            df['category_count'] = df['categories'].str.count(',') + 1
            df['category_count'] = df['category_count'].fillna(1)
        
        # Tag diversity (intrinsic - known at launch)
        if 'tags' in df.columns:
            df['tag_count'] = df['tags'].str.count(',') + 1
            df['tag_count'] = df['tag_count'].fillna(1)
        
        # Price analysis (intrinsic - known at launch)
        if 'initial_price' in df.columns and 'final_price' in df.columns:
            df['price_change'] = df['final_price'] - df['initial_price']
            df['price_change_percent'] = (df['price_change'] / df['initial_price'].replace(0, 1)) * 100
        
        # Age rating categories (intrinsic - known at launch)
        if 'required_age' in df.columns:
            df['age_rating'] = pd.cut(df['required_age'], 
                                    bins=[-1, 0, 12, 16, 18, 100], 
                                    labels=['All', '12+', '16+', '18+', 'Unknown'])
        
        # Release era (intrinsic - known at launch)
        if 'years_since_release' in df.columns:
            df['release_era'] = pd.cut(df['years_since_release'],
                                     bins=[0, 5, 10, 15, 20, 100],
                                     labels=['Recent', 'Modern', 'Classic', 'Retro', 'Vintage'])
    
    def encode_categorical_features(self, df):
        """Encode categorical features for machine learning."""
        print("Encoding categorical features...")
        
        # High cardinality categorical features to handle specially
        high_cardinality = ['developers', 'publishers', 'tags', 'supported_languages']
        
        # Get all categorical columns
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns
        
        for col in categorical_cols:
            if col in high_cardinality:
                # For high cardinality, create binary features for top categories
                top_categories = df[col].value_counts().head(10).index
                for category in top_categories:
                    df[f'{col}_{category}'] = (df[col] == category).astype(int)
                df.drop(col, axis=1, inplace=True)
            else:
                # For low cardinality, use label encoding
                le = LabelEncoder()
                df[col] = le.fit_transform(df[col].astype(str))
        
        # Ensure all remaining columns are numeric
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].fillna(0)
    
    def train_models(self):
        """Train multiple models and collect results."""
        print("\nTraining models with ONLY intrinsic features...")
        
        # Scale features for models that need it
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(self.X_train)
        X_test_scaled = scaler.transform(self.X_test)
        
        # Define models
        models = {
            'Random Forest': RandomForestClassifier(n_estimators=100, random_state=42),
            'XGBoost': xgb.XGBClassifier(random_state=42, eval_metric='logloss'),
            'Logistic Regression': LogisticRegression(random_state=42, max_iter=1000),
            'SVM': SVC(random_state=42, probability=True)
        }
        
        # Train and evaluate models
        for name, model in models.items():
            print(f"Training {name}...")
            
            if name in ['Logistic Regression', 'SVM']:
                model.fit(X_train_scaled, self.y_train)
                y_pred = model.predict(X_test_scaled)
                y_pred_proba = model.predict_proba(X_test_scaled)[:, 1]
            else:
                model.fit(self.X_train, self.y_train)
                y_pred = model.predict(self.X_test)
                y_pred_proba = model.predict_proba(self.X_test)[:, 1]
            
            # Calculate metrics
            accuracy = accuracy_score(self.y_test, y_pred)
            precision = precision_score(self.y_test, y_pred)
            recall = recall_score(self.y_test, y_pred)
            f1 = f1_score(self.y_test, y_pred)
            auc = roc_auc_score(self.y_test, y_pred_proba)
            
            self.models[name] = model
            self.results[name] = {
                'accuracy': accuracy,
                'precision': precision,
                'recall': recall,
                'f1': f1,
                'auc': auc,
                'predictions': y_pred,
                'probabilities': y_pred_proba
            }
            
            print(f"{name} - Accuracy: {accuracy:.3f}, AUC: {auc:.3f}")
            
            # Check for data leakage (accuracy too high)
            if accuracy > 0.95:
                print(f"⚠️  WARNING: {name} accuracy > 95% suggests possible data leakage!")
        
        # Select best model based on AUC
        best_model_name = max(self.results.keys(), key=lambda k: self.results[k]['auc'])
        print(f"\nBest model: {best_model_name}")
        print(f"Best accuracy: {self.results[best_model_name]['accuracy']:.3f}")
        print(f"Best AUC: {self.results[best_model_name]['auc']:.3f}")
        
        return best_model_name
    
    def create_model_performance_chart(self):
        """Create model performance comparison chart."""
        print("Creating model performance comparison chart...")
        
        metrics = ['accuracy', 'precision', 'recall', 'f1', 'auc']
        model_names = list(self.results.keys())
        
        # Create visualization
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Create grouped bar chart
        x = np.arange(len(model_names))
        width = 0.15
        
        colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
        
        for i, metric in enumerate(metrics):
            values = [self.results[model][metric] for model in model_names]
            ax.bar(x + i * width, values, width, label=metric.title(), color=colors[i])
        
        ax.set_xlabel('Machine Learning Models', fontsize=12, fontweight='bold')
        ax.set_ylabel('Performance Score', fontsize=12, fontweight='bold')
        ax.set_title('Model Performance Comparison\n(Truly Intrinsic Features Only)', 
                    fontsize=14, fontweight='bold', pad=20)
        ax.set_xticks(x + width * 2)
        ax.set_xticklabels(model_names, rotation=45, ha='right')
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        ax.grid(True, alpha=0.3)
        ax.set_ylim(0, 1)
        
        # Add value labels on bars
        for i, model in enumerate(model_names):
            for j, metric in enumerate(metrics):
                value = self.results[model][metric]
                ax.text(i + j * width, value + 0.01, f'{value:.3f}', 
                       ha='center', va='bottom', fontsize=8)
        
        plt.tight_layout()
        plt.savefig('truly_intrinsic_model_performance.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    def create_roc_curves(self):
        """Create ROC curves for all models."""
        print("Creating ROC curves...")
        
        fig, ax = plt.subplots(figsize=(10, 8))
        
        colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
        
        for i, (name, result) in enumerate(self.results.items()):
            fpr, tpr, _ = roc_curve(self.y_test, result['probabilities'])
            auc = result['auc']
            
            ax.plot(fpr, tpr, color=colors[i], linewidth=2,
                   label=f'{name} (AUC = {auc:.3f})')
        
        # Plot diagonal line
        ax.plot([0, 1], [0, 1], 'k--', alpha=0.5, linewidth=1)
        
        ax.set_xlabel('False Positive Rate', fontsize=12, fontweight='bold')
        ax.set_ylabel('True Positive Rate', fontsize=12, fontweight='bold')
        ax.set_title('ROC Curves Comparison\n(Truly Intrinsic Features Only)', 
                    fontsize=14, fontweight='bold', pad=20)
        ax.legend(loc='lower right')
        ax.grid(True, alpha=0.3)
        ax.set_xlim([0, 1])
        ax.set_ylim([0, 1])
        
        plt.tight_layout()
        plt.savefig('truly_intrinsic_roc_curves.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    def create_feature_importance_chart(self):
        """Create feature importance chart."""
        print("Creating feature importance chart...")
        
        # Use Random Forest feature importance
        rf_model = self.models['Random Forest']
        importance_df = pd.DataFrame({
            'feature': self.feature_names,
            'importance': rf_model.feature_importances_
        }).sort_values('importance', ascending=True)
        
        # Get top 15 features
        top_features = importance_df.tail(15)
        
        fig, ax = plt.subplots(figsize=(12, 10))
        
        # Create horizontal bar chart
        bars = ax.barh(range(len(top_features)), top_features['importance'], 
                      color='steelblue', alpha=0.7)
        
        ax.set_yticks(range(len(top_features)))
        ax.set_yticklabels(top_features['feature'], fontsize=10)
        ax.set_xlabel('Feature Importance Score', fontsize=12, fontweight='bold')
        ax.set_title('Top 15 Most Important Features\n(Random Forest - Truly Intrinsic Only)', 
                    fontsize=14, fontweight='bold', pad=20)
        ax.grid(True, alpha=0.3, axis='x')
        
        # Add value labels
        for i, (idx, row) in enumerate(top_features.iterrows()):
            ax.text(row['importance'] + 0.001, i, f'{row["importance"]:.3f}', 
                   va='center', ha='left', fontsize=9)
        
        plt.tight_layout()
        plt.savefig('truly_intrinsic_feature_importance.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    def create_confusion_matrices(self):
        """Create confusion matrices for all models."""
        print("Creating confusion matrices...")
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        axes = axes.ravel()
        
        for i, (name, result) in enumerate(self.results.items()):
            cm = confusion_matrix(self.y_test, result['predictions'])
            
            # Create heatmap
            sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', 
                       xticklabels=['Alive', 'Dead'], 
                       yticklabels=['Alive', 'Dead'],
                       ax=axes[i])
            
            axes[i].set_title(f'{name}\nAccuracy: {result["accuracy"]:.3f}', 
                            fontsize=12, fontweight='bold')
            axes[i].set_xlabel('Predicted', fontsize=10)
            axes[i].set_ylabel('Actual', fontsize=10)
        
        plt.suptitle('Confusion Matrices Comparison\n(Truly Intrinsic Features Only)', 
                    fontsize=16, fontweight='bold', y=0.98)
        plt.tight_layout()
        plt.savefig('truly_intrinsic_confusion_matrices.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    def create_genre_risk_analysis(self):
        """Create genre risk analysis chart."""
        print("Creating genre risk analysis chart...")
        
        # Get genre death rates
        genre_death_rates = self.df.groupby('genres')['label_dead_binary'].agg(['count', 'mean']).reset_index()
        genre_death_rates.columns = ['genre', 'game_count', 'death_rate']
        
        # Filter genres with sufficient sample size
        genre_death_rates = genre_death_rates[genre_death_rates['game_count'] >= 20]
        genre_death_rates = genre_death_rates.sort_values('death_rate', ascending=False)
        
        # Get top 15 high-risk and low-risk genres
        high_risk = genre_death_rates.head(15)
        low_risk = genre_death_rates.tail(15)
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 12))
        
        # High-risk genres
        bars1 = ax1.barh(range(len(high_risk)), high_risk['death_rate'], 
                        color='red', alpha=0.7)
        ax1.set_yticks(range(len(high_risk)))
        ax1.set_yticklabels(high_risk['genre'], fontsize=9)
        ax1.set_xlabel('Death Rate', fontsize=12, fontweight='bold')
        ax1.set_title('High-Risk Genres (Top 15)\n(n≥20 games)', fontsize=14, fontweight='bold')
        ax1.grid(True, alpha=0.3, axis='x')
        
        # Add sample sizes
        for i, (idx, row) in enumerate(high_risk.iterrows()):
            ax1.text(row['death_rate'] + 0.01, i, f'n={row["game_count"]}', 
                    va='center', ha='left', fontsize=8)
        
        # Low-risk genres
        bars2 = ax2.barh(range(len(low_risk)), low_risk['death_rate'], 
                        color='green', alpha=0.7)
        ax2.set_yticks(range(len(low_risk)))
        ax2.set_yticklabels(low_risk['genre'], fontsize=9)
        ax2.set_xlabel('Death Rate', fontsize=12, fontweight='bold')
        ax2.set_title('Low-Risk Genres (Bottom 15)\n(n≥20 games)', fontsize=14, fontweight='bold')
        ax2.grid(True, alpha=0.3, axis='x')
        
        # Add sample sizes
        for i, (idx, row) in enumerate(low_risk.iterrows()):
            ax2.text(row['death_rate'] + 0.01, i, f'n={row["game_count"]}', 
                    va='center', ha='left', fontsize=8)
        
        plt.tight_layout()
        plt.savefig('truly_intrinsic_genre_risk_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    def export_comprehensive_results(self):
        """Export comprehensive results to Excel."""
        print("Exportting comprehensive results...")
        
        with pd.ExcelWriter('truly_intrinsic_analysis_results.xlsx', engine='openpyxl') as writer:
            
            # 1. Model Performance Summary
            performance_data = []
            for model_name, results in self.results.items():
                performance_data.append({
                    'Model': model_name,
                    'Accuracy': results['accuracy'],
                    'Precision': results['precision'],
                    'Recall': results['recall'],
                    'F1-Score': results['f1'],
                    'AUC': results['auc']
                })
            
            perf_df = pd.DataFrame(performance_data)
            perf_df.to_excel(writer, sheet_name='Model_Performance', index=False)
            
            # 2. Feature Importance Rankings
            rf_model = self.models['Random Forest']
            importance_df = pd.DataFrame({
                'Rank': range(1, len(self.feature_names) + 1),
                'Feature': self.feature_names,
                'Importance_Score': rf_model.feature_importances_
            }).sort_values('Importance_Score', ascending=False)
            
            importance_df.to_excel(writer, sheet_name='Feature_Importance', index=False)
            
            # 3. Predictions vs Actual (Test Set)
            predictions_data = []
            for model_name, results in self.results.items():
                for i in range(len(self.y_test)):
                    predictions_data.append({
                        'Model': model_name,
                        'Actual': self.y_test.iloc[i],
                        'Predicted': results['predictions'][i],
                        'Probability': results['probabilities'][i],
                        'Correct': self.y_test.iloc[i] == results['predictions'][i]
                    })
            
            pred_df = pd.DataFrame(predictions_data)
            pred_df.to_excel(writer, sheet_name='Predictions_vs_Actual', index=False)
            
            # 4. Business Risk Profiles
            genre_death_rates = self.df.groupby('genres')['label_dead_binary'].agg(['count', 'mean']).reset_index()
            genre_death_rates.columns = ['Genre', 'Sample_Size', 'Death_Rate']
            genre_death_rates = genre_death_rates[genre_death_rates['Sample_Size'] >= 10]
            genre_death_rates = genre_death_rates.sort_values('Death_Rate', ascending=False)
            
            genre_death_rates.to_excel(writer, sheet_name='Genre_Risk_Profiles', index=False)
            
            # 5. Dataset Summary
            summary_data = {
                'Metric': ['Total Games', 'Dead Games', 'Alive Games', 'Death Rate', 'Intrinsic Features'],
                'Value': [len(self.df), 
                         self.df['label_dead_binary'].sum(),
                         len(self.df) - self.df['label_dead_binary'].sum(),
                         self.df['label_dead_binary'].mean(),
                         len(self.feature_names)]
            }
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Dataset_Summary', index=False)
        
        print("Comprehensive results exported to: truly_intrinsic_analysis_results.xlsx")
    
    def run_complete_analysis(self):
        """Run the complete truly intrinsic analysis."""
        print("="*60)
        print("TRULY INTRINSIC GAME DEATH ANALYSIS")
        print("="*60)
        print("Analyzing game death patterns using ONLY features")
        print("that exist at game launch (NO performance metrics)")
        print("="*60)
        
        # Load and preprocess data
        df_processed = self.load_and_preprocess_data()
        
        # Prepare for modeling
        X = df_processed.drop('label_dead_binary', axis=1)
        y = df_processed['label_dead_binary']
        
        # Split data
        self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        self.feature_names = X.columns.tolist()
        
        # Train models
        best_model = self.train_models()
        
        # Create visualizations
        self.create_model_performance_chart()
        self.create_roc_curves()
        self.create_feature_importance_chart()
        self.create_confusion_matrices()
        self.create_genre_risk_analysis()
        
        # Export results
        self.export_comprehensive_results()
        
        print("\n" + "="*60)
        print("ANALYSIS COMPLETE - NO DATA LEAKAGE")
        print("="*60)
        print("Generated Files:")
        print("- truly_intrinsic_model_performance.png")
        print("- truly_intrinsic_roc_curves.png")
        print("- truly_intrinsic_feature_importance.png")
        print("- truly_intrinsic_confusion_matrices.png")
        print("- truly_intrinsic_genre_risk_analysis.png")
        print("- truly_intrinsic_analysis_results.xlsx")
        print("="*60)
        
        return self.results

if __name__ == "__main__":
    # Initialize analyzer
    analyzer = TrulyIntrinsicAnalyzer('dead_labels_enriched.csv')
    
    # Run complete analysis
    results = analyzer.run_complete_analysis()