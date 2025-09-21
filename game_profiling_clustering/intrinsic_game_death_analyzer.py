#!/usr/bin/env python3
"""
Intrinsic Game Death Analyzer - Corrected Analysis
=================================================

This script analyzes game death patterns based on INTRINSIC game characteristics only,
excluding all engagement/player-related metrics that were used to define "dead games".

CRITICAL: No data leakage - we only use features that describe the game itself,
not its performance or player engagement metrics.

Intrinsic Features Used:
- Game metadata: type, genres, categories, tags, developers, publishers
- Technical specs: windows, mac, linux, pc_min_requirements, controller_support
- Content: required_age, supported_languages, has_dlc, dlc_count, achievements_total
- Business model: is_free, initial_price, final_price, discount_percent
- Quality indicators: metacritic_score (if available)
- Temporal: release_date, time since release

EXCLUDED Features (engagement metrics):
- avg_players_median_6m, months_used, recommendations_total
- Any player count or engagement metrics
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
from sklearn.feature_selection import SelectKBest, f_classif
import warnings
warnings.filterwarnings('ignore')

class IntrinsicGameDeathAnalyzer:
    def __init__(self, csv_path):
        """Initialize the analyzer with the dead games dataset."""
        self.csv_path = csv_path
        self.df = None
        self.intrinsic_features = None
        self.model = None
        self.feature_importance = None
        self.results = {}
        
    def load_data(self):
        """Load and examine the dataset."""
        print("Loading dead games dataset...")
        self.df = pd.read_csv(self.csv_path)
        print(f"Dataset loaded: {len(self.df)} games")
        print(f"Columns: {list(self.df.columns)}")
        
        # Check for missing values
        print("\nMissing values per column:")
        missing = self.df.isnull().sum()
        print(missing[missing > 0])
        
        return self.df
    
    def identify_intrinsic_features(self):
        """Identify intrinsic game characteristics (exclude engagement metrics)."""
        
        # EXCLUDED features (engagement/performance metrics)
        excluded_features = [
            'avg_players_median_6m',  # Player engagement metric
            'months_used',            # Usage metric
            'recommendations_total',  # Engagement metric
            'min_months_required',    # Used in death definition
            'min_months_ok',         # Used in death definition
            'first_month_in_window',  # Temporal window metric
            'last_month',            # Temporal window metric
            'crawl_timestamp',       # Technical metadata
            'crawl_status'           # Technical metadata
        ]
        
        # INTRINSIC features (game characteristics)
        intrinsic_features = [
            # Game metadata
            'type', 'genres', 'categories', 'tags', 'developers', 'publishers',
            
            # Technical specs
            'windows', 'mac', 'linux', 'pc_min_requirements', 'controller_support',
            
            # Content characteristics
            'required_age', 'supported_languages', 'has_dlc', 'dlc_count', 
            'achievements_total',
            
            # Business model
            'is_free', 'initial_price', 'final_price', 'discount_percent',
            
            # Quality indicators
            'metacritic_score',
            
            # Temporal (release info)
            'release_date', 'coming_soon'
        ]
        
        # Target variable
        target = 'label_dead_binary'
        
        # Verify all intrinsic features exist in dataset
        available_features = [f for f in intrinsic_features if f in self.df.columns]
        missing_features = [f for f in intrinsic_features if f not in self.df.columns]
        
        print(f"\nIntrinsic features available: {len(available_features)}")
        print(f"Available: {available_features}")
        if missing_features:
            print(f"Missing: {missing_features}")
        
        self.intrinsic_features = available_features + [target]
        print(f"\nFeatures to analyze: {self.intrinsic_features}")
        
        return available_features, target
    
    def preprocess_data(self):
        """Preprocess the data for intrinsic features only."""
        print("\nPreprocessing data...")
        
        # Select only intrinsic features
        df_work = self.df[self.intrinsic_features].copy()
        
        # Handle missing values
        print("Handling missing values...")
        
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
        
        # Create additional intrinsic features
        self.create_derived_features(df_work)
        
        # Encode categorical variables
        self.encode_categorical_features(df_work)
        
        print(f"Final dataset shape: {df_work.shape}")
        print(f"Target distribution: {df_work['label_dead_binary'].value_counts()}")
        
        return df_work
    
    def create_derived_features(self, df):
        """Create additional intrinsic features from existing ones."""
        print("Creating derived intrinsic features...")
        
        # Platform diversity
        platform_cols = ['windows', 'mac', 'linux']
        if all(col in df.columns for col in platform_cols):
            # Convert platform columns to boolean first
            for col in platform_cols:
                df[col] = df[col].astype(str).str.lower().isin(['true', '1', 'yes'])
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
        
        # Age rating categories
        if 'required_age' in df.columns:
            df['age_rating'] = pd.cut(df['required_age'], 
                                    bins=[-1, 0, 12, 16, 18, 100], 
                                    labels=['All', '12+', '16+', '18+', 'Unknown'])
        
        # Release era
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
    
    def train_model(self, df):
        """Train multiple models and select the best one."""
        print("\nTraining models...")
        
        # Prepare features and target
        X = df.drop('label_dead_binary', axis=1)
        y = df['label_dead_binary']
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        # Scale features
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        
        # Define models
        models = {
            'Random Forest': RandomForestClassifier(n_estimators=100, random_state=42),
            'Gradient Boosting': GradientBoostingClassifier(random_state=42),
            'Logistic Regression': LogisticRegression(random_state=42, max_iter=1000)
        }
        
        # Train and evaluate models
        results = {}
        for name, model in models.items():
            print(f"Training {name}...")
            
            if name == 'Logistic Regression':
                model.fit(X_train_scaled, y_train)
                y_pred = model.predict(X_test_scaled)
                y_pred_proba = model.predict_proba(X_test_scaled)[:, 1]
            else:
                model.fit(X_train, y_train)
                y_pred = model.predict(X_test)
                y_pred_proba = model.predict_proba(X_test)[:, 1]
            
            # Calculate metrics
            accuracy = (y_pred == y_test).mean()
            auc = roc_auc_score(y_test, y_pred_proba)
            
            results[name] = {
                'model': model,
                'accuracy': accuracy,
                'auc': auc,
                'predictions': y_pred,
                'probabilities': y_pred_proba
            }
            
            print(f"{name} - Accuracy: {accuracy:.3f}, AUC: {auc:.3f}")
        
        # Select best model based on AUC
        best_model_name = max(results.keys(), key=lambda k: results[k]['auc'])
        self.model = results[best_model_name]['model']
        self.results = results
        
        print(f"\nBest model: {best_model_name}")
        print(f"Best accuracy: {results[best_model_name]['accuracy']:.3f}")
        print(f"Best AUC: {results[best_model_name]['auc']:.3f}")
        
        # Check for data leakage (accuracy too high)
        if results[best_model_name]['accuracy'] > 0.95:
            print("⚠️  WARNING: Accuracy > 95% suggests possible data leakage!")
            print("Please review feature selection.")
        
        return results[best_model_name], X.columns
    
    def analyze_feature_importance(self, model, feature_names):
        """Analyze feature importance."""
        print("\nAnalyzing feature importance...")
        
        if hasattr(model, 'feature_importances_'):
            importance_df = pd.DataFrame({
                'feature': feature_names,
                'importance': model.feature_importances_
            }).sort_values('importance', ascending=False)
            
            self.feature_importance = importance_df
            
            print("Top 20 most important features:")
            print(importance_df.head(20))
            
            return importance_df
        else:
            print("Model does not support feature importance analysis.")
            return None
    
    def generate_insights(self, df, feature_importance):
        """Generate actionable insights for game developers."""
        print("\nGenerating actionable insights...")
        
        insights = {}
        
        # 1. Genre Analysis
        if 'genres' in df.columns:
            genre_death_rates = df.groupby('genres')['label_dead_binary'].mean().sort_values(ascending=False)
            insights['high_risk_genres'] = genre_death_rates.head(10)
            insights['low_risk_genres'] = genre_death_rates.tail(10)
        
        # 2. Platform Strategy
        platform_cols = ['windows', 'mac', 'linux']
        if all(col in df.columns for col in platform_cols):
            platform_analysis = df.groupby('platform_count')['label_dead_binary'].mean()
            insights['platform_strategy'] = platform_analysis
        
        # 3. Pricing Strategy
        if 'is_free' in df.columns:
            pricing_analysis = df.groupby('is_free')['label_dead_binary'].mean()
            insights['pricing_strategy'] = pricing_analysis
        
        # 4. Content Strategy
        if 'has_dlc' in df.columns:
            dlc_analysis = df.groupby('has_dlc')['label_dead_binary'].mean()
            insights['dlc_strategy'] = dlc_analysis
        
        # 5. Age Rating Analysis
        if 'required_age' in df.columns:
            age_analysis = df.groupby('required_age')['label_dead_binary'].mean()
            insights['age_rating'] = age_analysis
        
        # 6. Developer/Publisher Analysis
        if 'developers' in df.columns:
            dev_analysis = df.groupby('developers')['label_dead_binary'].mean()
            insights['developer_patterns'] = dev_analysis.sort_values(ascending=False).head(10)
        
        return insights
    
    def create_visualizations(self, df, feature_importance, insights):
        """Create visualizations for the analysis."""
        print("\nCreating visualizations...")
        
        plt.style.use('default')
        fig, axes = plt.subplots(2, 3, figsize=(18, 12))
        fig.suptitle('Intrinsic Game Death Analysis - Corrected', fontsize=16, fontweight='bold')
        
        # 1. Feature Importance
        if feature_importance is not None:
            top_features = feature_importance.head(15)
            axes[0, 0].barh(range(len(top_features)), top_features['importance'])
            axes[0, 0].set_yticks(range(len(top_features)))
            axes[0, 0].set_yticklabels(top_features['feature'])
            axes[0, 0].set_title('Top Feature Importance')
            axes[0, 0].set_xlabel('Importance')
        
        # 2. Genre Risk Analysis
        if 'high_risk_genres' in insights:
            axes[0, 1].bar(range(len(insights['high_risk_genres'])), insights['high_risk_genres'].values)
            axes[0, 1].set_xticks(range(len(insights['high_risk_genres'])))
            axes[0, 1].set_xticklabels(insights['high_risk_genres'].index, rotation=45, ha='right')
            axes[0, 1].set_title('High Risk Genres')
            axes[0, 1].set_ylabel('Death Rate')
        
        # 3. Platform Strategy
        if 'platform_strategy' in insights:
            axes[0, 2].bar(insights['platform_strategy'].index, insights['platform_strategy'].values)
            axes[0, 2].set_title('Platform Strategy Impact')
            axes[0, 2].set_xlabel('Number of Platforms')
            axes[0, 2].set_ylabel('Death Rate')
        
        # 4. Pricing Strategy
        if 'pricing_strategy' in insights:
            axes[1, 0].bar(['Paid', 'Free'], insights['pricing_strategy'].values)
            axes[1, 0].set_title('Pricing Strategy Impact')
            axes[1, 0].set_ylabel('Death Rate')
        
        # 5. DLC Strategy
        if 'dlc_strategy' in insights:
            axes[1, 1].bar(['No DLC', 'Has DLC'], insights['dlc_strategy'].values)
            axes[1, 1].set_title('DLC Strategy Impact')
            axes[1, 1].set_ylabel('Death Rate')
        
        # 6. Age Rating Analysis
        if 'age_rating' in insights:
            axes[1, 2].bar(insights['age_rating'].index, insights['age_rating'].values)
            axes[1, 2].set_title('Age Rating Impact')
            axes[1, 2].set_xlabel('Required Age')
            axes[1, 2].set_ylabel('Death Rate')
        
        plt.tight_layout()
        plt.savefig('intrinsic_game_death_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    def generate_business_recommendations(self, insights, feature_importance):
        """Generate specific business recommendations."""
        print("\nGenerating business recommendations...")
        
        recommendations = []
        
        # Genre recommendations
        if 'high_risk_genres' in insights:
            high_risk = insights['high_risk_genres'].head(3)
            high_risk_names = [str(x) for x in high_risk.index]
            high_risk_rates = [f'{rate:.1%}' for rate in high_risk.values]
            recommendations.append(f"AVOID high-risk genres: {', '.join(high_risk_names)} (death rates: {', '.join(high_risk_rates)})")
            
            low_risk = insights['low_risk_genres'].tail(3)
            low_risk_names = [str(x) for x in low_risk.index]
            low_risk_rates = [f'{rate:.1%}' for rate in low_risk.values]
            recommendations.append(f"CONSIDER low-risk genres: {', '.join(low_risk_names)} (death rates: {', '.join(low_risk_rates)})")
        
        # Platform recommendations
        if 'platform_strategy' in insights:
            platform_rates = insights['platform_strategy']
            best_platform_count = platform_rates.idxmin()
            recommendations.append(f"OPTIMAL platform strategy: {best_platform_count} platforms (lowest death rate: {platform_rates[best_platform_count]:.1%})")
        
        # Pricing recommendations
        if 'pricing_strategy' in insights:
            free_rate = insights['pricing_strategy'].get(True, 0)
            paid_rate = insights['pricing_strategy'].get(False, 0)
            if free_rate < paid_rate:
                recommendations.append(f"CONSIDER free-to-play model (death rate: {free_rate:.1%} vs paid: {paid_rate:.1%})")
            else:
                recommendations.append(f"CONSIDER paid model (death rate: {paid_rate:.1%} vs free: {free_rate:.1%})")
        
        # DLC recommendations
        if 'dlc_strategy' in insights:
            no_dlc_rate = insights['dlc_strategy'].get(False, 0)
            has_dlc_rate = insights['dlc_strategy'].get(True, 0)
            if has_dlc_rate < no_dlc_rate:
                recommendations.append(f"INCLUDE DLC content (death rate: {has_dlc_rate:.1%} vs no DLC: {no_dlc_rate:.1%})")
            else:
                recommendations.append(f"AVOID DLC complexity (death rate: {no_dlc_rate:.1%} vs with DLC: {has_dlc_rate:.1%})")
        
        return recommendations
    
    def run_complete_analysis(self):
        """Run the complete analysis pipeline."""
        print("="*60)
        print("INTRINSIC GAME DEATH ANALYSIS - CORRECTED")
        print("="*60)
        print("Analyzing game death patterns based on intrinsic characteristics only")
        print("EXCLUDING all engagement/player metrics to prevent data leakage")
        print("="*60)
        
        # Load data
        self.load_data()
        
        # Identify intrinsic features
        intrinsic_features, target = self.identify_intrinsic_features()
        
        # Preprocess data
        df_processed = self.preprocess_data()
        
        # Train model
        best_result, feature_names = self.train_model(df_processed)
        
        # Analyze feature importance
        feature_importance = self.analyze_feature_importance(best_result['model'], feature_names)
        
        # Generate insights
        insights = self.generate_insights(df_processed, feature_importance)
        
        # Create visualizations
        self.create_visualizations(df_processed, feature_importance, insights)
        
        # Generate business recommendations
        recommendations = self.generate_business_recommendations(insights, feature_importance)
        
        # Print summary
        print("\n" + "="*60)
        print("ANALYSIS SUMMARY")
        print("="*60)
        print(f"Dataset size: {len(df_processed)} games")
        print(f"Features analyzed: {len(feature_names)} intrinsic characteristics")
        print(f"Model accuracy: {best_result['accuracy']:.3f}")
        print(f"Model AUC: {best_result['auc']:.3f}")
        
        print("\nBUSINESS RECOMMENDATIONS:")
        for i, rec in enumerate(recommendations, 1):
            print(f"{i}. {rec}")
        
        # Save results
        self.save_results(df_processed, feature_importance, insights, recommendations)
        
        return {
            'processed_data': df_processed,
            'feature_importance': feature_importance,
            'insights': insights,
            'recommendations': recommendations,
            'model_results': best_result
        }
    
    def save_results(self, df, feature_importance, insights, recommendations):
        """Save analysis results to files."""
        print("\nSaving results...")
        
        # Save processed data
        df.to_csv('processed_intrinsic_data.csv', index=False)
        
        # Save feature importance
        if feature_importance is not None:
            feature_importance.to_csv('feature_importance.csv', index=False)
        
        # Save insights
        insights_df = pd.DataFrame(insights)
        insights_df.to_csv('game_death_insights.csv')
        
        # Save recommendations
        with open('business_recommendations.txt', 'w') as f:
            f.write("GAME DEATH PREVENTION RECOMMENDATIONS\n")
            f.write("="*50 + "\n\n")
            for i, rec in enumerate(recommendations, 1):
                f.write(f"{i}. {rec}\n")
        
        print("Results saved to:")
        print("- processed_intrinsic_data.csv")
        print("- feature_importance.csv")
        print("- game_death_insights.csv")
        print("- business_recommendations.txt")
        print("- intrinsic_game_death_analysis.png")

if __name__ == "__main__":
    # Initialize analyzer with balanced dataset (dead + alive games)
    analyzer = IntrinsicGameDeathAnalyzer('dead_labels_enriched.csv')
    
    # Run complete analysis
    results = analyzer.run_complete_analysis()