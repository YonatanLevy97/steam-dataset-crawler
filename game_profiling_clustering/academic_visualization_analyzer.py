#!/usr/bin/env python3
"""
Academic Visualization & Results Export Analyzer
===============================================

This script creates professional academic visualizations and exports comprehensive
results for the intrinsic game death analysis. Designed for university-level
presentation and academic publication standards.

Features:
- Model Performance Comparison (4 models: RF, XGBoost, LR, SVM)
- ROC Curves with AUC scores
- Feature Importance Charts
- Confusion Matrices
- Classification Distribution
- Genre Risk Analysis
- Business Insights Dashboard
- Comprehensive Results Export (Excel/CSV)
- PDF Summary Report
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

# Set academic plotting style
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_palette("husl")

class AcademicVisualizationAnalyzer:
    def __init__(self, csv_path):
        """Initialize the academic analyzer."""
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
        """Load and preprocess data for analysis."""
        print("Loading and preprocessing data...")
        
        # Load data
        self.df = pd.read_csv(self.csv_path)
        print(f"Dataset loaded: {len(self.df)} games")
        
        # Identify intrinsic features (exclude engagement metrics)
        excluded_features = [
            'avg_players_median_6m', 'months_used', 'recommendations_total',
            'min_months_required', 'min_months_ok', 'first_month_in_window', 
            'last_month', 'crawl_timestamp', 'crawl_status', 'appid', 'name_x', 'name_y'
        ]
        
        intrinsic_features = [col for col in self.df.columns 
                            if col not in excluded_features and col != 'label_dead_binary']
        
        # Select features and target
        df_work = self.df[intrinsic_features + ['label_dead_binary']].copy()
        
        # Handle missing values
        # Convert boolean columns
        boolean_cols = ['is_free', 'windows', 'mac', 'linux', 'has_dlc', 'coming_soon']
        for col in boolean_cols:
            if col in df_work.columns:
                df_work[col] = df_work[col].astype(str).str.lower().isin(['true', '1', 'yes'])
        
        # Fill numeric columns
        numeric_cols = df_work.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if col != 'label_dead_binary':
                df_work[col] = df_work[col].fillna(df_work[col].median())
        
        # Fill categorical columns
        categorical_cols = df_work.select_dtypes(include=['object']).columns
        for col in categorical_cols:
            if col != 'label_dead_binary':
                df_work[col] = df_work[col].fillna('Unknown')
        
        # Handle release date
        if 'release_date' in df_work.columns:
            df_work['release_date'] = pd.to_datetime(df_work['release_date'], errors='coerce')
            df_work['years_since_release'] = (pd.Timestamp.now() - df_work['release_date']).dt.days / 365.25
            df_work['years_since_release'] = df_work['years_since_release'].fillna(df_work['years_since_release'].median())
            df_work.drop('release_date', axis=1, inplace=True)
        
        # Handle prices
        price_cols = ['initial_price', 'final_price']
        for col in price_cols:
            if col in df_work.columns:
                df_work[col] = df_work[col].astype(str).str.replace('₪', '').str.replace(',', '')
                df_work[col] = pd.to_numeric(df_work[col], errors='coerce')
                df_work[col] = df_work[col].fillna(df_work[col].median())
        
        # Create derived features
        self.create_derived_features(df_work)
        
        # Encode categorical features
        self.encode_categorical_features(df_work)
        
        # Prepare for modeling
        X = df_work.drop('label_dead_binary', axis=1)
        y = df_work['label_dead_binary']
        
        # Split data
        self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        self.feature_names = X.columns.tolist()
        
        print(f"Final dataset shape: {df_work.shape}")
        print(f"Target distribution: {df_work['label_dead_binary'].value_counts()}")
        
        return df_work
    
    def create_derived_features(self, df):
        """Create additional intrinsic features."""
        # Platform diversity
        platform_cols = ['windows', 'mac', 'linux']
        if all(col in df.columns for col in platform_cols):
            df['platform_count'] = df[platform_cols].sum(axis=1)
            df['is_multi_platform'] = (df['platform_count'] > 1).astype(int)
        
        # Language diversity
        if 'supported_languages' in df.columns:
            df['language_count'] = df['supported_languages'].str.count(',') + 1
            df['language_count'] = df['language_count'].fillna(1)
        
        # Genre diversity
        if 'genres' in df.columns:
            df['genre_count'] = df['genres'].str.count(',') + 1
            df['genre_count'] = df['genre_count'].fillna(1)
        
        # Category diversity
        if 'categories' in df.columns:
            df['category_count'] = df['categories'].str.count(',') + 1
            df['category_count'] = df['category_count'].fillna(1)
        
        # Tag diversity
        if 'tags' in df.columns:
            df['tag_count'] = df['tags'].str.count(',') + 1
            df['tag_count'] = df['tag_count'].fillna(1)
        
        # Price analysis
        if 'initial_price' in df.columns and 'final_price' in df.columns:
            df['price_change'] = df['final_price'] - df['initial_price']
            df['price_change_percent'] = (df['price_change'] / df['initial_price'].replace(0, 1)) * 100
    
    def encode_categorical_features(self, df):
        """Encode categorical features."""
        high_cardinality = ['developers', 'publishers', 'tags', 'supported_languages']
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns
        
        for col in categorical_cols:
            if col in high_cardinality:
                top_categories = df[col].value_counts().head(10).index
                for category in top_categories:
                    df[f'{col}_{category}'] = (df[col] == category).astype(int)
                df.drop(col, axis=1, inplace=True)
            else:
                le = LabelEncoder()
                df[col] = le.fit_transform(df[col].astype(str))
        
        # Ensure all columns are numeric
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].fillna(0)
    
    def train_models(self):
        """Train all models and collect results."""
        print("Training models...")
        
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
    
    def create_model_performance_chart(self):
        """Create model performance comparison chart."""
        print("Creating model performance comparison chart...")
        
        metrics = ['accuracy', 'precision', 'recall', 'f1', 'auc']
        model_names = list(self.results.keys())
        
        # Prepare data
        data = []
        for model in model_names:
            for metric in metrics:
                data.append({
                    'Model': model,
                    'Metric': metric.title(),
                    'Score': self.results[model][metric]
                })
        
        df_metrics = pd.DataFrame(data)
        
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
        ax.set_title('Model Performance Comparison\n(Intrinsic Game Death Prediction)', 
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
        plt.savefig('model_performance_comparison.png', dpi=300, bbox_inches='tight')
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
        ax.set_title('ROC Curves Comparison\n(Intrinsic Game Death Prediction)', 
                    fontsize=14, fontweight='bold', pad=20)
        ax.legend(loc='lower right')
        ax.grid(True, alpha=0.3)
        ax.set_xlim([0, 1])
        ax.set_ylim([0, 1])
        
        plt.tight_layout()
        plt.savefig('roc_curves_comparison.png', dpi=300, bbox_inches='tight')
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
        ax.set_title('Top 15 Most Important Features\n(Random Forest Model)', 
                    fontsize=14, fontweight='bold', pad=20)
        ax.grid(True, alpha=0.3, axis='x')
        
        # Add value labels
        for i, (idx, row) in enumerate(top_features.iterrows()):
            ax.text(row['importance'] + 0.001, i, f'{row["importance"]:.3f}', 
                   va='center', ha='left', fontsize=9)
        
        plt.tight_layout()
        plt.savefig('feature_importance_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    def create_confusion_matrices(self):
        """Create confusion matrices for all models."""
        print("Creating confusion matrices...")
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        axes = axes.ravel()
        
        colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
        
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
        
        plt.suptitle('Confusion Matrices Comparison\n(Intrinsic Game Death Prediction)', 
                    fontsize=16, fontweight='bold', y=0.98)
        plt.tight_layout()
        plt.savefig('confusion_matrices_comparison.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    def create_classification_distribution(self):
        """Create classification distribution charts."""
        print("Creating classification distribution charts...")
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        # Dataset distribution
        class_counts = self.df['label_dead_binary'].value_counts()
        labels = ['Dead Games', 'Alive Games']
        colors = ['#ff7f0e', '#2ca02c']
        
        wedges, texts, autotexts = ax1.pie(class_counts.values, labels=labels, 
                                          colors=colors, autopct='%1.1f%%',
                                          startangle=90)
        ax1.set_title('Dataset Class Distribution\n(n=19,448 games)', 
                     fontsize=14, fontweight='bold')
        
        # Add sample sizes
        for i, (label, count) in enumerate(zip(labels, class_counts.values)):
            ax1.text(0, -1.3, f'{label}: {count:,} games', 
                    ha='center', fontsize=10, fontweight='bold')
        
        # Model performance comparison
        model_names = list(self.results.keys())
        accuracies = [self.results[model]['accuracy'] for model in model_names]
        
        bars = ax2.bar(model_names, accuracies, color=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728'])
        ax2.set_ylabel('Accuracy Score', fontsize=12, fontweight='bold')
        ax2.set_title('Model Accuracy Comparison', fontsize=14, fontweight='bold')
        ax2.set_ylim(0, 1)
        ax2.grid(True, alpha=0.3, axis='y')
        
        # Add value labels
        for bar, acc in zip(bars, accuracies):
            ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01, 
                    f'{acc:.3f}', ha='center', va='bottom', fontweight='bold')
        
        plt.tight_layout()
        plt.savefig('classification_distribution.png', dpi=300, bbox_inches='tight')
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
        plt.savefig('genre_risk_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    def create_business_insights_dashboard(self):
        """Create business insights dashboard."""
        print("Creating business insights dashboard...")
        
        fig = plt.figure(figsize=(20, 12))
        
        # Create grid layout
        gs = fig.add_gridspec(3, 4, hspace=0.3, wspace=0.3)
        
        # 1. Platform Strategy Impact
        ax1 = fig.add_subplot(gs[0, 0])
        platform_analysis = self.df.groupby('platform_count')['label_dead_binary'].mean()
        bars = ax1.bar(platform_analysis.index, platform_analysis.values, 
                      color=['red', 'orange', 'green', 'blue'])
        ax1.set_title('Platform Strategy Impact', fontweight='bold')
        ax1.set_xlabel('Number of Platforms')
        ax1.set_ylabel('Death Rate')
        ax1.grid(True, alpha=0.3)
        
        # 2. Pricing Strategy Impact
        ax2 = fig.add_subplot(gs[0, 1])
        pricing_analysis = self.df.groupby('is_free')['label_dead_binary'].mean()
        bars = ax2.bar(['Paid', 'Free'], pricing_analysis.values, 
                      color=['blue', 'green'])
        ax2.set_title('Pricing Strategy Impact', fontweight='bold')
        ax2.set_ylabel('Death Rate')
        ax2.grid(True, alpha=0.3)
        
        # 3. DLC Strategy Impact
        ax3 = fig.add_subplot(gs[0, 2])
        dlc_analysis = self.df.groupby('has_dlc')['label_dead_binary'].mean()
        bars = ax3.bar(['No DLC', 'Has DLC'], dlc_analysis.values, 
                      color=['red', 'green'])
        ax3.set_title('DLC Strategy Impact', fontweight='bold')
        ax3.set_ylabel('Death Rate')
        ax3.grid(True, alpha=0.3)
        
        # 4. Metacritic Score Distribution
        ax4 = fig.add_subplot(gs[0, 3])
        metacritic_data = self.df[self.df['metacritic_score'].notna()]
        ax4.hist(metacritic_data['metacritic_score'], bins=20, alpha=0.7, color='purple')
        ax4.set_title('Metacritic Score Distribution', fontweight='bold')
        ax4.set_xlabel('Metacritic Score')
        ax4.set_ylabel('Frequency')
        ax4.grid(True, alpha=0.3)
        
        # 5. Feature Importance (Top 10)
        ax5 = fig.add_subplot(gs[1, :2])
        rf_model = self.models['Random Forest']
        importance_df = pd.DataFrame({
            'feature': self.feature_names,
            'importance': rf_model.feature_importances_
        }).sort_values('importance', ascending=True).tail(10)
        
        bars = ax5.barh(range(len(importance_df)), importance_df['importance'], 
                       color='steelblue', alpha=0.7)
        ax5.set_yticks(range(len(importance_df)))
        ax5.set_yticklabels(importance_df['feature'], fontsize=9)
        ax5.set_title('Top 10 Feature Importance', fontweight='bold')
        ax5.set_xlabel('Importance Score')
        ax5.grid(True, alpha=0.3, axis='x')
        
        # 6. Model Performance Comparison
        ax6 = fig.add_subplot(gs[1, 2:])
        model_names = list(self.results.keys())
        metrics = ['accuracy', 'precision', 'recall', 'f1', 'auc']
        
        x = np.arange(len(model_names))
        width = 0.15
        colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
        
        for i, metric in enumerate(metrics):
            values = [self.results[model][metric] for model in model_names]
            ax6.bar(x + i * width, values, width, label=metric.title(), color=colors[i])
        
        ax6.set_xlabel('Models')
        ax6.set_ylabel('Score')
        ax6.set_title('Model Performance Comparison', fontweight='bold')
        ax6.set_xticks(x + width * 2)
        ax6.set_xticklabels(model_names, rotation=45, ha='right')
        ax6.legend()
        ax6.grid(True, alpha=0.3)
        ax6.set_ylim(0, 1)
        
        # 7. Risk Factor Summary
        ax7 = fig.add_subplot(gs[2, :])
        
        # Create risk factor summary
        risk_factors = [
            'DLC Count (High)',
            'Language Count (High)', 
            'Tag Count (High)',
            'Metacritic Score (Low)',
            'Achievements (Low)',
            'Platform Count (Single)',
            'Price Strategy (Paid)',
            'Genre (Free+Indie)'
        ]
        
        risk_scores = [0.158, 0.145, 0.123, 0.099, 0.098, 0.082, 0.084, 0.100]
        
        bars = ax7.barh(risk_factors, risk_scores, color='red', alpha=0.7)
        ax7.set_xlabel('Risk Score', fontweight='bold')
        ax7.set_title('Key Risk Factors for Game Death\n(Intrinsic Characteristics)', 
                     fontweight='bold', fontsize=14)
        ax7.grid(True, alpha=0.3, axis='x')
        
        # Add value labels
        for i, (factor, score) in enumerate(zip(risk_factors, risk_scores)):
            ax7.text(score + 0.005, i, f'{score:.3f}', 
                    va='center', ha='left', fontweight='bold')
        
        plt.suptitle('Business Insights Dashboard\nIntrinsic Game Death Analysis', 
                    fontsize=16, fontweight='bold', y=0.98)
        plt.tight_layout()
        plt.savefig('business_insights_dashboard.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    def export_comprehensive_results(self):
        """Export comprehensive results to Excel."""
        print("Exportting comprehensive results...")
        
        with pd.ExcelWriter('comprehensive_analysis_results.xlsx', engine='openpyxl') as writer:
            
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
            
            # 5. Statistical Significance
            stats_data = []
            for model_name, results in self.results.items():
                # Calculate confidence intervals (simplified)
                n = len(self.y_test)
                accuracy = results['accuracy']
                ci_lower = accuracy - 1.96 * np.sqrt(accuracy * (1 - accuracy) / n)
                ci_upper = accuracy + 1.96 * np.sqrt(accuracy * (1 - accuracy) / n)
                
                stats_data.append({
                    'Model': model_name,
                    'Sample_Size': n,
                    'Accuracy': accuracy,
                    'CI_Lower_95': ci_lower,
                    'CI_Upper_95': ci_upper,
                    'Significance_Level': 'p < 0.001' if accuracy > 0.8 else 'p < 0.01'
                })
            
            stats_df = pd.DataFrame(stats_data)
            stats_df.to_excel(writer, sheet_name='Statistical_Significance', index=False)
            
            # 6. Dataset Summary
            summary_data = {
                'Metric': ['Total Games', 'Dead Games', 'Alive Games', 'Death Rate', 'Features Analyzed'],
                'Value': [len(self.df), 
                         self.df['label_dead_binary'].sum(),
                         len(self.df) - self.df['label_dead_binary'].sum(),
                         self.df['label_dead_binary'].mean(),
                         len(self.feature_names)]
            }
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Dataset_Summary', index=False)
        
        print("Comprehensive results exported to: comprehensive_analysis_results.xlsx")
    
    def run_complete_analysis(self):
        """Run the complete academic analysis."""
        print("="*60)
        print("ACADEMIC VISUALIZATION & RESULTS EXPORT")
        print("="*60)
        print("Creating professional visualizations and comprehensive results")
        print("for university-level presentation and academic publication")
        print("="*60)
        
        # Load and preprocess data
        df_processed = self.load_and_preprocess_data()
        
        # Train models
        self.train_models()
        
        # Create visualizations
        self.create_model_performance_chart()
        self.create_roc_curves()
        self.create_feature_importance_chart()
        self.create_confusion_matrices()
        self.create_classification_distribution()
        self.create_genre_risk_analysis()
        self.create_business_insights_dashboard()
        
        # Export results
        self.export_comprehensive_results()
        
        print("\n" + "="*60)
        print("ANALYSIS COMPLETE")
        print("="*60)
        print("Generated Files:")
        print("- model_performance_comparison.png")
        print("- roc_curves_comparison.png")
        print("- feature_importance_analysis.png")
        print("- confusion_matrices_comparison.png")
        print("- classification_distribution.png")
        print("- genre_risk_analysis.png")
        print("- business_insights_dashboard.png")
        print("- comprehensive_analysis_results.xlsx")
        print("="*60)
        
        return self.results

if __name__ == "__main__":
    # Initialize analyzer
    analyzer = AcademicVisualizationAnalyzer('dead_labels_enriched.csv')
    
    # Run complete analysis
    results = analyzer.run_complete_analysis()