"""
Data loading and preprocessing utilities for Steam Communities Visualization Suite.

This module provides comprehensive data loading, cleaning, and preprocessing 
functionality for community analysis data.
"""

import pandas as pd
import numpy as np
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
import warnings

try:
    from .config import DEFAULT_DATA_PATHS, COMMUNITY_NAMES, COMMUNITY_DESCRIPTIONS
except ImportError:
    from config import DEFAULT_DATA_PATHS, COMMUNITY_NAMES, COMMUNITY_DESCRIPTIONS

class CommunityDataLoader:
    """
    Comprehensive data loader for Steam community analysis data.
    
    Handles loading, preprocessing, and feature extraction from various
    data sources including CSV files and detailed JSON profiles.
    """
    
    def __init__(self, data_dir: str = None):
        """
        Initialize the data loader.
        
        Args:
            data_dir (str, optional): Base directory containing data files.
                                    Defaults to parent directory of this script.
        """
        if data_dir is None:
            # Default to parent directory of this script
            self.data_dir = Path(__file__).parent.parent
        else:
            self.data_dir = Path(data_dir)
        
        self.community_profiles = None
        self.detailed_profiles = None
        self.community_summary = None
        self.overall_profile = None
        
        # Cached processed data
        self._genre_matrix = None
        self._feature_matrix = None
        self._similarity_matrix = None
    
    def load_all_data(self) -> None:
        """Load all available data sources."""
        try:
            self.load_community_profiles()
            self.load_detailed_profiles()
            self.load_community_summary()
            self.load_overall_profile()
            print("✅ All data loaded successfully!")
        except Exception as e:
            print(f"⚠️ Warning: Could not load all data sources: {e}")
    
    def load_community_profiles(self) -> pd.DataFrame:
        """
        Load community average profiles data.
        
        Returns:
            pd.DataFrame: Community profiles with computed metrics
        """
        file_path = self.data_dir / DEFAULT_DATA_PATHS['community_profiles']
        
        if not file_path.exists():
            raise FileNotFoundError(f"Community profiles file not found: {file_path}")
        
        self.community_profiles = pd.read_csv(file_path)
        
        # Add community names and descriptions
        self.community_profiles['community_name'] = self.community_profiles['community_id'].map(COMMUNITY_NAMES)
        self.community_profiles['community_description'] = self.community_profiles['community_id'].map(COMMUNITY_DESCRIPTIONS)
        
        # Convert percentage columns to numeric
        percentage_cols = [col for col in self.community_profiles.columns if 'percentage' in col]
        for col in percentage_cols:
            self.community_profiles[col] = pd.to_numeric(self.community_profiles[col], errors='coerce')
        
        # Calculate additional metrics
        self.community_profiles = self._calculate_additional_metrics(self.community_profiles)
        
        return self.community_profiles
    
    def load_detailed_profiles(self) -> Dict:
        """
        Load detailed community profiles from JSON.
        
        Returns:
            Dict: Detailed profiles with hierarchical structure
        """
        file_path = self.data_dir / DEFAULT_DATA_PATHS['detailed_profiles']
        
        if not file_path.exists():
            raise FileNotFoundError(f"Detailed profiles file not found: {file_path}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            self.detailed_profiles = json.load(f)
        
        return self.detailed_profiles
    
    def load_community_summary(self) -> pd.DataFrame:
        """
        Load community summary data.
        
        Returns:
            pd.DataFrame: High-level community summary
        """
        file_path = self.data_dir / DEFAULT_DATA_PATHS['community_summary']
        
        if not file_path.exists():
            warnings.warn(f"Community summary file not found: {file_path}")
            return None
        
        self.community_summary = pd.read_csv(file_path)
        
        # Add community names
        self.community_summary['community_name'] = self.community_summary['community_id'].map(COMMUNITY_NAMES)
        
        return self.community_summary
    
    def load_overall_profile(self) -> pd.DataFrame:
        """
        Load overall dataset profile for comparison.
        
        Returns:
            pd.DataFrame: Overall dataset statistics
        """
        file_path = self.data_dir / DEFAULT_DATA_PATHS['overall_profile']
        
        if not file_path.exists():
            warnings.warn(f"Overall profile file not found: {file_path}")
            return None
        
        self.overall_profile = pd.read_csv(file_path)
        return self.overall_profile
    
    def _calculate_additional_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate additional derived metrics.
        
        Args:
            df (pd.DataFrame): Community profiles dataframe
            
        Returns:
            pd.DataFrame: Enhanced dataframe with additional metrics
        """
        df = df.copy()
        
        # Price metrics
        if 'average_price' in df.columns and 'median_price' in df.columns:
            df['price_skew'] = (df['average_price'] - df['median_price']) / (df['average_price'] + 1e-6)
            df['is_budget_friendly'] = (df['median_price'] < 20) & (df['average_price'] < 30)
        
        # Platform diversity
        platform_cols = ['windows_true_percentage', 'mac_true_percentage', 'linux_true_percentage']
        if all(col in df.columns for col in platform_cols):
            df['platform_diversity'] = df[platform_cols].apply(lambda x: (x > 10).sum(), axis=1)
            df['cross_platform_friendly'] = df['platform_diversity'] >= 2
        
        # Content richness
        if 'achievements_total_mean' in df.columns and 'dlc_count_mean' in df.columns:
            df['content_richness'] = (
                (df['achievements_total_mean'] > df['achievements_total_mean'].median()) * 1 +
                (df['dlc_count_mean'] > df['dlc_count_mean'].median()) * 1 +
                (df['has_dlc_true_percentage'] > 20) * 1
            )
        
        # Quality indicators
        if 'metacritic_score_mean' in df.columns and 'recommendations_total_mean' in df.columns:
            df['high_quality'] = (
                (df['metacritic_score_mean'] > 75) & 
                (df['recommendations_total_mean'] > df['recommendations_total_mean'].median())
            )
        
        return df
    
    def get_genre_matrix(self, top_n: int = 10) -> pd.DataFrame:
        """
        Create genre distribution matrix across communities.
        
        Args:
            top_n (int): Number of top genres to include
            
        Returns:
            pd.DataFrame: Communities x genres matrix with percentages
        """
        if self._genre_matrix is not None:
            return self._genre_matrix
        
        if self.detailed_profiles is None:
            self.load_detailed_profiles()
        
        # Extract genre data for all communities
        genre_data = []
        for comm_id, profile in self.detailed_profiles['community_profiles'].items():
            genres = profile['categorical_features']['genres']['top_values']
            for genre_info in genres:
                genre_data.append({
                    'community_id': int(comm_id),
                    'genre': genre_info['value'],
                    'percentage': genre_info['percentage']
                })
        
        genre_df = pd.DataFrame(genre_data)
        
        # Get top genres across all communities
        top_genres = (genre_df.groupby('genre')['percentage']
                     .mean()
                     .sort_values(ascending=False)
                     .head(top_n)
                     .index.tolist())
        
        # Create pivot table
        self._genre_matrix = (genre_df[genre_df['genre'].isin(top_genres)]
                             .pivot(index='community_id', columns='genre', values='percentage')
                             .fillna(0))
        
        # Add community names as index
        self._genre_matrix['community_name'] = self._genre_matrix.index.map(COMMUNITY_NAMES)
        self._genre_matrix = self._genre_matrix.set_index('community_name')
        
        return self._genre_matrix
    
    def get_feature_matrix(self) -> pd.DataFrame:
        """
        Create comprehensive feature matrix for similarity analysis.
        
        Returns:
            pd.DataFrame: Normalized feature matrix for communities
        """
        if self._feature_matrix is not None:
            return self._feature_matrix
        
        if self.community_profiles is None:
            self.load_community_profiles()
        
        # Select numerical features for analysis
        feature_cols = [
            'required_age_mean', 'metacritic_score_mean', 'recommendations_total_mean',
            'achievements_total_mean', 'dlc_count_mean', 'release_year_mean',
            'is_free_true_percentage', 'windows_true_percentage', 'mac_true_percentage',
            'linux_true_percentage', 'has_dlc_true_percentage', 'average_price'
        ]
        
        # Add genre percentages if available
        if self.detailed_profiles is not None:
            genre_matrix = self.get_genre_matrix(top_n=5)
            feature_matrix = self.community_profiles.set_index('community_id')[feature_cols].copy()
            
            # Merge with genre matrix
            genre_matrix_reset = genre_matrix.reset_index()
            genre_matrix_reset['community_id'] = range(len(genre_matrix_reset))
            genre_matrix_reset = genre_matrix_reset.set_index('community_id')
            
            self._feature_matrix = pd.concat([feature_matrix, genre_matrix_reset.drop('community_name', axis=1)], axis=1)
        else:
            self._feature_matrix = self.community_profiles.set_index('community_id')[feature_cols].copy()
        
        # Fill missing values with median
        for col in self._feature_matrix.columns:
            if self._feature_matrix[col].dtype in ['float64', 'int64']:
                self._feature_matrix[col] = self._feature_matrix[col].fillna(self._feature_matrix[col].median())
        
        # Normalize features (0-1 scaling)
        from sklearn.preprocessing import MinMaxScaler
        scaler = MinMaxScaler()
        normalized_data = scaler.fit_transform(self._feature_matrix)
        self._feature_matrix = pd.DataFrame(
            normalized_data, 
            index=self._feature_matrix.index,
            columns=self._feature_matrix.columns
        )
        
        return self._feature_matrix
    
    def get_similarity_matrix(self, method: str = 'cosine') -> pd.DataFrame:
        """
        Calculate similarity matrix between communities.
        
        Args:
            method (str): Similarity method ('cosine', 'euclidean', 'correlation')
            
        Returns:
            pd.DataFrame: Similarity matrix
        """
        if self._similarity_matrix is not None and hasattr(self, '_similarity_method') and self._similarity_method == method:
            return self._similarity_matrix
        
        feature_matrix = self.get_feature_matrix()
        
        if method == 'cosine':
            from sklearn.metrics.pairwise import cosine_similarity
            similarity_array = cosine_similarity(feature_matrix)
        elif method == 'euclidean':
            from sklearn.metrics.pairwise import euclidean_distances
            distances = euclidean_distances(feature_matrix)
            similarity_array = 1 / (1 + distances)  # Convert distance to similarity
        elif method == 'correlation':
            similarity_array = np.corrcoef(feature_matrix)
        else:
            raise ValueError(f"Unknown similarity method: {method}")
        
        # Create DataFrame with community names
        community_names = [COMMUNITY_NAMES[i] for i in feature_matrix.index]
        self._similarity_matrix = pd.DataFrame(
            similarity_array,
            index=community_names,
            columns=community_names
        )
        self._similarity_method = method
        
        return self._similarity_matrix
    
    def get_publisher_data(self, top_n: int = 10) -> pd.DataFrame:
        """
        Extract top publishers data across communities.
        
        Args:
            top_n (int): Number of top publishers per community
            
        Returns:
            pd.DataFrame: Publisher data with community associations
        """
        if self.detailed_profiles is None:
            self.load_detailed_profiles()
        
        publisher_data = []
        for comm_id, profile in self.detailed_profiles['community_profiles'].items():
            publishers = profile['categorical_features']['publishers']['top_values'][:top_n]
            for pub_info in publishers:
                publisher_data.append({
                    'community_id': int(comm_id),
                    'community_name': COMMUNITY_NAMES[int(comm_id)],
                    'publisher': pub_info['value'],
                    'count': pub_info['count'],
                    'percentage': pub_info['percentage']
                })
        
        return pd.DataFrame(publisher_data)
    
    def get_temporal_data(self) -> pd.DataFrame:
        """
        Extract temporal release patterns for communities.
        
        Returns:
            pd.DataFrame: Release year statistics by community
        """
        if self.community_profiles is None:
            self.load_community_profiles()
        
        temporal_cols = ['community_id', 'community_name', 'release_year_mean', 
                        'release_year_median', 'release_year_coverage']
        
        return self.community_profiles[temporal_cols].copy()
    
    def get_language_data(self) -> pd.DataFrame:
        """
        Extract language support data across communities.
        
        Returns:
            pd.DataFrame: Language support statistics
        """
        if self.detailed_profiles is None:
            self.load_detailed_profiles()
        
        language_data = []
        for comm_id, profile in self.detailed_profiles['community_profiles'].items():
            languages = profile['categorical_features']['supported_languages']['top_values'][:5]
            for lang_info in languages:
                language_data.append({
                    'community_id': int(comm_id),
                    'community_name': COMMUNITY_NAMES[int(comm_id)],
                    'language': lang_info['value'],
                    'percentage': lang_info['percentage']
                })
        
        return pd.DataFrame(language_data)
    
    def export_processed_data(self, output_dir: str) -> None:
        """
        Export all processed data to CSV files.
        
        Args:
            output_dir (str): Directory to save exported data
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Export main datasets
        if self.community_profiles is not None:
            self.community_profiles.to_csv(output_path / 'community_profiles_enhanced.csv', index=False)
        
        # Export matrices
        genre_matrix = self.get_genre_matrix()
        genre_matrix.to_csv(output_path / 'genre_distribution_matrix.csv')
        
        feature_matrix = self.get_feature_matrix()
        feature_matrix.to_csv(output_path / 'feature_matrix_normalized.csv')
        
        similarity_matrix = self.get_similarity_matrix()
        similarity_matrix.to_csv(output_path / 'community_similarity_matrix.csv')
        
        # Export specialized datasets
        publisher_data = self.get_publisher_data()
        publisher_data.to_csv(output_path / 'publisher_community_data.csv', index=False)
        
        temporal_data = self.get_temporal_data()
        temporal_data.to_csv(output_path / 'temporal_release_patterns.csv', index=False)
        
        language_data = self.get_language_data()
        language_data.to_csv(output_path / 'language_support_data.csv', index=False)
        
        print(f"✅ Processed data exported to {output_path}")
    
    def validate_data(self) -> Dict[str, bool]:
        """
        Validate loaded data for completeness and consistency.
        
        Returns:
            Dict[str, bool]: Validation results
        """
        validation = {
            'community_profiles_loaded': self.community_profiles is not None,
            'detailed_profiles_loaded': self.detailed_profiles is not None,
            'community_summary_loaded': self.community_summary is not None,
            'overall_profile_loaded': self.overall_profile is not None,
        }
        
        if self.community_profiles is not None:
            validation['all_communities_present'] = len(self.community_profiles) == 14
            validation['no_missing_community_ids'] = not self.community_profiles['community_id'].isna().any()
            validation['reasonable_sizes'] = (self.community_profiles['size'] > 0).all()
        
        if self.detailed_profiles is not None:
            validation['detailed_profiles_complete'] = len(self.detailed_profiles['community_profiles']) == 14
        
        validation['overall_valid'] = all(validation.values())
        
        return validation

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def load_data(data_dir: str = None) -> CommunityDataLoader:
    """
    Convenience function to load all community data.
    
    Args:
        data_dir (str, optional): Data directory path
        
    Returns:
        CommunityDataLoader: Loaded data loader instance
    """
    loader = CommunityDataLoader(data_dir)
    loader.load_all_data()
    return loader

def validate_input_files(data_dir: str = None) -> Dict[str, bool]:
    """
    Validate that all required input files exist.
    
    Args:
        data_dir (str, optional): Data directory path
        
    Returns:
        Dict[str, bool]: File existence validation
    """
    if data_dir is None:
        data_dir = Path(__file__).parent.parent
    else:
        data_dir = Path(data_dir)
    
    validation = {}
    for key, path in DEFAULT_DATA_PATHS.items():
        full_path = data_dir / path
        validation[key] = full_path.exists()
    
    return validation

# Example usage and testing
if __name__ == "__main__":
    # Test data loading
    print("Testing Community Data Loader...")
    
    # Validate files
    file_validation = validate_input_files()
    print("File validation:", file_validation)
    
    if any(file_validation.values()):
        # Load data
        loader = load_data()
        
        # Validate data
        data_validation = loader.validate_data()
        print("Data validation:", data_validation)
        
        # Print summary
        if loader.community_profiles is not None:
            print(f"\nLoaded {len(loader.community_profiles)} communities")
            print("Community sizes:", loader.community_profiles['size'].describe())
            
        # Test matrix generation
        try:
            genre_matrix = loader.get_genre_matrix()
            print(f"\nGenre matrix shape: {genre_matrix.shape}")
            
            similarity_matrix = loader.get_similarity_matrix()
            print(f"Similarity matrix shape: {similarity_matrix.shape}")
        except Exception as e:
            print(f"Matrix generation failed: {e}")
    else:
        print("❌ No data files found. Please check data directory path.")