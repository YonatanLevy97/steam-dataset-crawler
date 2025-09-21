#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
community_profile_generator.py

Generate comprehensive average profiles for each community and an overall average profile.
Creates both CSV and markdown outputs with detailed statistics for categorical and numerical fields.

Usage:
    python community_profile_generator.py --community-assignments out/louvain_dead_games_communities/community_assignments.csv --games-metadata data/games_metadata_merged.csv --output-dir ./community_profiles_analysis
    
    # Or use defaults:
    python community_profile_generator.py
"""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path
from collections import Counter, defaultdict
import json
from typing import Dict, List, Any, Tuple, Union
import re
from datetime import datetime


class CommunityProfileGenerator:
    """Generate comprehensive profiles for game communities."""
    
    def __init__(self, community_assignments_path: str, games_metadata_path: str, output_dir: str):
        self.community_assignments_path = Path(community_assignments_path)
        self.games_metadata_path = Path(games_metadata_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Configuration for field types and processing
        self.categorical_fields = [
            'type', 'developers', 'publishers', 'categories', 'genres', 'tags',
            'supported_languages', 'controller_support', 'coming_soon'
        ]
        
        self.numerical_fields = [
            'required_age', 'metacritic_score', 'recommendations_total', 
            'achievements_total', 'dlc_count', 'discount_percent'
        ]
        
        self.boolean_fields = [
            'is_free', 'windows', 'mac', 'linux', 'has_dlc'
        ]
        
        self.price_fields = [
            'initial_price', 'final_price'
        ]
        
        # Data storage
        self.assignments_df = None
        self.metadata_df = None
        self.community_profiles = {}
        self.overall_profile = {}
    
    def clean_and_convert_price(self, price_value) -> Union[float, None]:
        """Clean price data and convert to numeric."""
        if pd.isna(price_value):
            return None
        
        price_str = str(price_value).strip()
        
        # Handle special cases
        if any(term in price_str.lower() for term in ['free to play', 'free', 'demo']):
            return 0.0
        
        # Remove currency symbols and extract numeric value
        numeric_match = re.search(r'([0-9]+(?:[.,][0-9]{1,2})?)', price_str)
        if numeric_match:
            price_num_str = numeric_match.group(1)
            price_num_str = price_num_str.replace(',', '.')
            try:
                return float(price_num_str)
            except ValueError:
                pass
        
        return None
    
    def safe_parse_list_field(self, field_value) -> List[str]:
        """Safely parse comma-separated fields."""
        if pd.isna(field_value) or field_value == '' or str(field_value).lower() in ['nan', 'none']:
            return []
        
        field_str = str(field_value)
        items = [item.strip().strip('"\'') for item in field_str.split(',') if item.strip()]
        return [item for item in items if item and item.lower() not in ['nan', 'none', '']]
    
    def parse_release_date(self, date_str) -> Union[int, None]:
        """Extract year from release date string."""
        if pd.isna(date_str) or str(date_str).lower() in ['nan', 'none', '']:
            return None
        
        date_str = str(date_str).strip()
        # Try to extract year using various patterns
        year_patterns = [
            r'\b(19|20)\d{2}\b',  # 4-digit year
            r'\b(\d{4})\b'        # Any 4-digit number
        ]
        
        for pattern in year_patterns:
            match = re.search(pattern, date_str)
            if match:
                year = int(match.group())
                if 1970 <= year <= 2030:  # Reasonable year range for games
                    return year
        
        return None
    
    def load_data(self):
        """Load and preprocess all required data."""
        print(f"📊 Loading community assignments from {self.community_assignments_path}")
        self.assignments_df = pd.read_csv(self.community_assignments_path)
        self.assignments_df['node_id'] = self.assignments_df['node_id'].astype(str)
        
        print(f"📥 Loading games metadata from {self.games_metadata_path}")
        self.metadata_df = pd.read_csv(self.games_metadata_path)
        self.metadata_df['appid'] = self.metadata_df['appid'].astype(str)
        
        # Clean price data
        print("💰 Processing price data...")
        self.metadata_df['initial_price_clean'] = self.metadata_df['initial_price'].apply(self.clean_and_convert_price)
        self.metadata_df['final_price_clean'] = self.metadata_df['final_price'].apply(self.clean_and_convert_price)
        self.metadata_df['price'] = self.metadata_df['final_price_clean'].fillna(self.metadata_df['initial_price_clean'])
        
        # Parse release dates
        print("📅 Processing release dates...")
        self.metadata_df['release_year'] = self.metadata_df['release_date'].apply(self.parse_release_date)
        
        # Join data
        print("🔗 Joining community assignments with game metadata...")
        self.joined_df = self.assignments_df.merge(
            self.metadata_df, left_on='node_id', right_on='appid', how='left'
        )
        
        print(f"✅ Data loaded: {len(self.joined_df):,} games across {self.joined_df['community_id'].nunique()} communities")
    
    def calculate_categorical_profile(self, series: pd.Series, field_name: str, top_n: int = 5, 
                                    exclude_values: List[str] = None) -> Dict[str, Any]:
        """Calculate profile statistics for categorical fields."""
        if exclude_values is None:
            exclude_values = []
            
        if field_name in ['developers', 'publishers', 'genres', 'tags', 'categories', 'supported_languages']:
            # Handle comma-separated fields
            all_items = []
            for value in series.dropna():
                items = self.safe_parse_list_field(value)
                all_items.extend(items)
            
            if not all_items:
                return {
                    'most_common': 'N/A',
                    'top_values': [],
                    'unique_count': 0,
                    'total_instances': 0
                }
            
            counter = Counter(all_items)
            total_instances = len(all_items)
            
            # Filter out excluded values for most_common selection
            filtered_counter = Counter({k: v for k, v in counter.items() 
                                      if k.lower() not in [ex.lower() for ex in exclude_values]})
            
            # Get most common value (excluding filtered ones)
            if filtered_counter:
                most_common = filtered_counter.most_common(1)[0][0]
            else:
                # Fallback to original if all are filtered
                most_common = counter.most_common(1)[0][0] if counter else 'N/A'
            
            top_values = []
            for item, count in counter.most_common(top_n):
                percentage = (count / len(series.dropna())) * 100
                top_values.append({
                    'value': item,
                    'count': count,
                    'percentage': round(percentage, 2)
                })
            
            return {
                'most_common': most_common,
                'top_values': top_values,
                'unique_count': len(counter),
                'total_instances': total_instances
            }
        else:
            # Handle simple categorical fields
            value_counts = series.value_counts()
            if value_counts.empty:
                return {
                    'most_common': 'N/A',
                    'top_values': [],
                    'unique_count': 0,
                    'total_instances': 0
                }
            
            total_valid = len(series.dropna())
            top_values = []
            for value, count in value_counts.head(top_n).items():
                percentage = (count / total_valid) * 100
                top_values.append({
                    'value': str(value),
                    'count': count,
                    'percentage': round(percentage, 2)
                })
            
            return {
                'most_common': str(value_counts.index[0]),
                'top_values': top_values,
                'unique_count': len(value_counts),
                'total_instances': total_valid
            }
    
    def calculate_numerical_profile(self, series: pd.Series) -> Dict[str, Any]:
        """Calculate profile statistics for numerical fields."""
        clean_series = pd.to_numeric(series, errors='coerce').dropna()
        
        if clean_series.empty:
            return {
                'mean': None,
                'median': None,
                'std': None,
                'min': None,
                'max': None,
                'count': 0,
                'coverage_percent': 0.0
            }
        
        coverage_percent = (len(clean_series) / len(series)) * 100
        
        return {
            'mean': round(clean_series.mean(), 2),
            'median': round(clean_series.median(), 2),
            'std': round(clean_series.std(), 2),
            'min': round(clean_series.min(), 2),
            'max': round(clean_series.max(), 2),
            'count': len(clean_series),
            'coverage_percent': round(coverage_percent, 1)
        }
    
    def calculate_boolean_profile(self, series: pd.Series) -> Dict[str, Any]:
        """Calculate profile statistics for boolean fields."""
        # Convert to boolean, handling various representations
        bool_series = series.fillna(False)
        
        # Handle string representations
        if bool_series.dtype == 'object':
            bool_series = bool_series.str.lower().isin(['true', '1', 'yes', 'y'])
        else:
            bool_series = bool_series.astype(bool)
        
        total_count = len(bool_series)
        true_count = bool_series.sum()
        true_percentage = (true_count / total_count) * 100 if total_count > 0 else 0
        
        return {
            'true_count': int(true_count),
            'false_count': int(total_count - true_count),
            'true_percentage': round(true_percentage, 1),
            'total_count': total_count
        }
    
    def generate_community_profile(self, community_id: int) -> Dict[str, Any]:
        """Generate comprehensive profile for a single community."""
        community_data = self.joined_df[self.joined_df['community_id'] == community_id]
        
        if community_data.empty:
            return {'error': f'No data found for community {community_id}'}
        
        profile = {
            'community_id': community_id,
            'size': len(community_data),
            'categorical_features': {},
            'numerical_features': {},
            'boolean_features': {},
            'price_features': {}
        }
        
        # Process categorical fields with smart tag exclusion
        exclusion_values = []
        
        # First, process genres and categories to build exclusion list
        for field in ['genres', 'categories']:
            if field in community_data.columns:
                profile['categorical_features'][field] = self.calculate_categorical_profile(
                    community_data[field], field
                )
                # Add the most common genre/category to exclusion list for tags
                most_common = profile['categorical_features'][field]['most_common']
                if most_common != 'N/A':
                    exclusion_values.append(most_common)
        
        # Process other categorical fields
        for field in self.categorical_fields:
            if field in community_data.columns and field not in ['genres', 'categories']:
                # For tags, exclude values that are already most common in genres/categories
                if field == 'tags':
                    profile['categorical_features'][field] = self.calculate_categorical_profile(
                        community_data[field], field, exclude_values=exclusion_values
                    )
                else:
                    profile['categorical_features'][field] = self.calculate_categorical_profile(
                        community_data[field], field
                    )
        
        # Process numerical fields
        for field in self.numerical_fields:
            if field in community_data.columns:
                profile['numerical_features'][field] = self.calculate_numerical_profile(
                    community_data[field]
                )
        
        # Process boolean fields
        for field in self.boolean_fields:
            if field in community_data.columns:
                profile['boolean_features'][field] = self.calculate_boolean_profile(
                    community_data[field]
                )
        
        # Process price fields
        price_series = community_data['price'].dropna()
        if not price_series.empty:
            profile['price_features'] = {
                'average_price': round(price_series.mean(), 2),
                'median_price': round(price_series.median(), 2),
                'min_price': round(price_series.min(), 2),
                'max_price': round(price_series.max(), 2),
                'std_price': round(price_series.std(), 2),
                'games_with_prices': len(price_series),
                'price_coverage_percent': round((len(price_series) / len(community_data)) * 100, 1)
            }
        else:
            profile['price_features'] = {
                'average_price': None,
                'median_price': None,
                'min_price': None,
                'max_price': None,
                'std_price': None,
                'games_with_prices': 0,
                'price_coverage_percent': 0.0
            }
        
        # Add release year information
        if 'release_year' in community_data.columns:
            profile['numerical_features']['release_year'] = self.calculate_numerical_profile(
                community_data['release_year']
            )
        
        return profile
    
    def generate_all_community_profiles(self):
        """Generate profiles for all communities."""
        print("🏗️ Generating individual community profiles...")
        
        community_ids = sorted(self.joined_df['community_id'].unique())
        
        for community_id in community_ids:
            print(f"   Processing Community {community_id}...")
            profile = self.generate_community_profile(community_id)
            self.community_profiles[community_id] = profile
        
        print(f"✅ Generated profiles for {len(self.community_profiles)} communities")
    
    def generate_overall_profile(self):
        """Generate overall average profile from all community profiles."""
        print("🌍 Generating overall average profile from communities...")
        
        if not self.community_profiles:
            raise ValueError("No community profiles available. Generate community profiles first.")
        
        # Initialize overall profile structure
        self.overall_profile = {
            'total_communities': len(self.community_profiles),
            'total_games': sum(profile['size'] for profile in self.community_profiles.values()),
            'categorical_features': {},
            'numerical_features': {},
            'boolean_features': {},
            'price_features': {}
        }
        
        # Aggregate categorical features
        for field in self.categorical_fields:
            all_top_values = []
            for profile in self.community_profiles.values():
                if field in profile['categorical_features']:
                    cat_data = profile['categorical_features'][field]
                    if cat_data['top_values']:
                        all_top_values.append(cat_data['most_common'])
            
            if all_top_values:
                most_common_overall = Counter(all_top_values).most_common(1)[0][0]
                self.overall_profile['categorical_features'][field] = {
                    'most_common_across_communities': most_common_overall,
                    'appears_in_communities': len([v for v in all_top_values if v == most_common_overall])
                }
        
        # Aggregate numerical features
        for field in self.numerical_fields + ['release_year']:
            values = []
            for profile in self.community_profiles.values():
                if field in profile['numerical_features']:
                    num_data = profile['numerical_features'][field]
                    if num_data['mean'] is not None:
                        values.append(num_data['mean'])
            
            if values:
                self.overall_profile['numerical_features'][field] = {
                    'average_across_communities': round(np.mean(values), 2),
                    'min_community_avg': round(min(values), 2),
                    'max_community_avg': round(max(values), 2),
                    'std_across_communities': round(np.std(values), 2),
                    'communities_with_data': len(values)
                }
        
        # Aggregate boolean features
        for field in self.boolean_fields:
            percentages = []
            for profile in self.community_profiles.values():
                if field in profile['boolean_features']:
                    bool_data = profile['boolean_features'][field]
                    percentages.append(bool_data['true_percentage'])
            
            if percentages:
                self.overall_profile['boolean_features'][field] = {
                    'average_true_percentage': round(np.mean(percentages), 1),
                    'min_community_percentage': round(min(percentages), 1),
                    'max_community_percentage': round(max(percentages), 1),
                    'communities_with_data': len(percentages)
                }
        
        # Aggregate price features
        price_values = []
        for profile in self.community_profiles.values():
            price_data = profile['price_features']
            if price_data['average_price'] is not None:
                price_values.append(price_data['average_price'])
        
        if price_values:
            self.overall_profile['price_features'] = {
                'average_price_across_communities': round(np.mean(price_values), 2),
                'min_community_avg_price': round(min(price_values), 2),
                'max_community_avg_price': round(max(price_values), 2),
                'std_price_across_communities': round(np.std(price_values), 2),
                'communities_with_price_data': len(price_values)
            }
        
        print("✅ Generated overall profile from community averages")
    
    def save_profiles_to_csv(self):
        """Save community profiles to CSV format."""
        print("💾 Saving profiles to CSV...")
        
        # Prepare data for CSV
        csv_data = []
        
        for community_id, profile in self.community_profiles.items():
            row = {'community_id': community_id, 'size': profile['size']}
            
            # Add categorical features (most common values)
            for field, data in profile['categorical_features'].items():
                row[f'{field}_most_common'] = data['most_common']
                if data['top_values']:
                    row[f'{field}_top_percentage'] = data['top_values'][0]['percentage']
            
            # Add numerical features
            for field, data in profile['numerical_features'].items():
                if data['mean'] is not None:
                    row[f'{field}_mean'] = data['mean']
                    row[f'{field}_median'] = data['median']
                    row[f'{field}_coverage'] = data['coverage_percent']
            
            # Add boolean features
            for field, data in profile['boolean_features'].items():
                row[f'{field}_true_percentage'] = data['true_percentage']
            
            # Add price features
            price_data = profile['price_features']
            row['average_price'] = price_data['average_price']
            row['median_price'] = price_data['median_price']
            row['price_coverage'] = price_data['price_coverage_percent']
            
            csv_data.append(row)
        
        # Create DataFrame and save
        csv_df = pd.DataFrame(csv_data)
        csv_path = self.output_dir / 'community_average_profiles.csv'
        csv_df.to_csv(csv_path, index=False)
        
        # Also save overall profile as CSV
        overall_csv_data = []
        for feature_type, features in self.overall_profile.items():
            if isinstance(features, dict):
                for field, data in features.items():
                    if isinstance(data, dict):
                        for metric, value in data.items():
                            overall_csv_data.append({
                                'feature_type': feature_type,
                                'field': field,
                                'metric': metric,
                                'value': value
                            })
        
        overall_csv_df = pd.DataFrame(overall_csv_data)
        overall_csv_path = self.output_dir / 'overall_average_profile.csv'
        overall_csv_df.to_csv(overall_csv_path, index=False)
        
        print(f"   Saved community profiles to {csv_path}")
        print(f"   Saved overall profile to {overall_csv_path}")
    
    def save_profiles_to_markdown(self):
        """Save profiles to markdown format."""
        print("📄 Generating markdown report...")
        
        md_content = []
        md_content.append("# Community Average Profiles Analysis Report")
        md_content.append("")
        md_content.append(f"**Generated on:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        md_content.append("")
        md_content.append("## Overview")
        md_content.append(f"This report presents average profiles for **{len(self.community_profiles)}** communities,")
        md_content.append(f"covering a total of **{self.overall_profile['total_games']:,}** games.")
        md_content.append("")
        
        # Table of Contents
        md_content.append("## Table of Contents")
        md_content.append("")
        md_content.append("1. [Overall Average Profile](#overall-average-profile)")
        md_content.append("2. [Individual Community Profiles](#individual-community-profiles)")
        for community_id in sorted(self.community_profiles.keys()):
            size = self.community_profiles[community_id]['size']
            md_content.append(f"   - [Community {community_id}](#community-{community_id}) ({size:,} games)")
        md_content.append("")
        
        # Overall Profile Section
        md_content.append("## Overall Average Profile")
        md_content.append("")
        md_content.append("This section shows the average characteristics across all communities.")
        md_content.append("")
        
        # Overall categorical features
        if self.overall_profile['categorical_features']:
            md_content.append("### 🏷️ Most Common Categorical Values Across Communities")
            md_content.append("")
            for field, data in self.overall_profile['categorical_features'].items():
                md_content.append(f"- **{field.replace('_', ' ').title()}:** {data['most_common_across_communities']} "
                                f"(appears as top in {data['appears_in_communities']} communities)")
            md_content.append("")
        
        # Overall numerical features
        if self.overall_profile['numerical_features']:
            md_content.append("### 📊 Average Numerical Values Across Communities")
            md_content.append("")
            for field, data in self.overall_profile['numerical_features'].items():
                field_name = field.replace('_', ' ').title()
                md_content.append(f"- **{field_name}:** {data['average_across_communities']} average "
                                f"(range: {data['min_community_avg']} - {data['max_community_avg']})")
            md_content.append("")
        
        # Overall boolean features
        if self.overall_profile['boolean_features']:
            md_content.append("### ✅ Average Boolean Values Across Communities")
            md_content.append("")
            for field, data in self.overall_profile['boolean_features'].items():
                field_name = field.replace('_', ' ').title()
                md_content.append(f"- **{field_name}:** {data['average_true_percentage']}% average "
                                f"(range: {data['min_community_percentage']}% - {data['max_community_percentage']}%)")
            md_content.append("")
        
        # Overall price features
        if self.overall_profile['price_features']:
            md_content.append("### 💰 Average Price Information Across Communities")
            md_content.append("")
            price_data = self.overall_profile['price_features']
            md_content.append(f"- **Average Price:** ${price_data['average_price_across_communities']}")
            md_content.append(f"- **Price Range:** ${price_data['min_community_avg_price']} - ${price_data['max_community_avg_price']}")
            md_content.append(f"- **Communities with Price Data:** {price_data['communities_with_price_data']}")
            md_content.append("")
        
        md_content.append("---")
        md_content.append("")
        
        # Individual Community Profiles
        md_content.append("## Individual Community Profiles")
        md_content.append("")
        
        for community_id in sorted(self.community_profiles.keys()):
            profile = self.community_profiles[community_id]
            
            md_content.append(f"### Community {community_id}")
            md_content.append(f"**Size:** {profile['size']:,} games")
            md_content.append("")
            
            # Categorical features
            if profile['categorical_features']:
                md_content.append("#### 🏷️ Most Common Categorical Values")
                md_content.append("")
                for field, data in profile['categorical_features'].items():
                    if data['most_common'] != 'N/A':
                        field_name = field.replace('_', ' ').title()
                        # Use most_common (which has smart selection) instead of top_values[0]
                        most_common_value = data['most_common']
                        
                        # Find the percentage for this value
                        percentage = 0.0
                        for top_val in data['top_values']:
                            if top_val['value'] == most_common_value:
                                percentage = top_val['percentage']
                                break
                        
                        md_content.append(f"- **{field_name}:** {most_common_value} ({percentage:.1f}%)")
                md_content.append("")
            
            # Numerical features
            if profile['numerical_features']:
                md_content.append("#### 📊 Numerical Averages")
                md_content.append("")
                for field, data in profile['numerical_features'].items():
                    if data['mean'] is not None:
                        field_name = field.replace('_', ' ').title()
                        md_content.append(f"- **{field_name}:** {data['mean']} (median: {data['median']}, coverage: {data['coverage_percent']:.1f}%)")
                md_content.append("")
            
            # Boolean features
            if profile['boolean_features']:
                md_content.append("#### ✅ Boolean Values")
                md_content.append("")
                for field, data in profile['boolean_features'].items():
                    field_name = field.replace('_', ' ').title()
                    md_content.append(f"- **{field_name}:** {data['true_percentage']:.1f}% ({data['true_count']:,}/{data['total_count']:,} games)")
                md_content.append("")
            
            # Price features
            price_data = profile['price_features']
            if price_data['average_price'] is not None:
                md_content.append("#### 💰 Price Information")
                md_content.append("")
                md_content.append(f"- **Average Price:** ${price_data['average_price']}")
                md_content.append(f"- **Median Price:** ${price_data['median_price']}")
                md_content.append(f"- **Price Range:** ${price_data['min_price']} - ${price_data['max_price']}")
                md_content.append(f"- **Price Coverage:** {price_data['price_coverage_percent']:.1f}%")
                md_content.append("")
            
            md_content.append("---")
            md_content.append("")
        
        # Save markdown file
        md_path = self.output_dir / 'community_average_profiles_report.md'
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(md_content))
        
        print(f"   Saved markdown report to {md_path}")
    
    def run_analysis(self):
        """Run the complete analysis pipeline."""
        print("🚀 Starting Community Profile Generation...\n")
        
        # Load data
        self.load_data()
        
        # Generate individual community profiles
        self.generate_all_community_profiles()
        
        # Generate overall profile
        self.generate_overall_profile()
        
        # Save outputs
        self.save_profiles_to_csv()
        self.save_profiles_to_markdown()
        
        # Save detailed JSON for reference
        profiles_json_path = self.output_dir / 'detailed_community_profiles.json'
        
        # Convert numpy/pandas types to native Python types
        def convert_types(obj):
            if hasattr(obj, 'item'):  # numpy scalars
                return obj.item()
            elif isinstance(obj, dict):
                return {str(k): convert_types(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_types(v) for v in obj]
            else:
                return obj
        
        converted_profiles = convert_types({
            'community_profiles': self.community_profiles,
            'overall_profile': self.overall_profile
        })
        
        with open(profiles_json_path, 'w', encoding='utf-8') as f:
            json.dump(converted_profiles, f, indent=2, default=str)
        
        print(f"💾 Saved detailed profiles to {profiles_json_path}")
        print(f"\n✅ Analysis complete! Results saved to: {self.output_dir}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate comprehensive average profiles for game communities"
    )
    parser.add_argument(
        '--community-assignments',
        default='out/louvain_dead_games_communities/community_assignments.csv',
        help='Path to community assignments CSV file'
    )
    parser.add_argument(
        '--games-metadata',
        default='data/games_metadata_merged.csv',
        help='Path to games metadata CSV file'
    )
    parser.add_argument(
        '--output-dir',
        default='./community_profiles_analysis',
        help='Output directory for generated files'
    )
    
    args = parser.parse_args()
    
    try:
        generator = CommunityProfileGenerator(
            args.community_assignments,
            args.games_metadata,
            args.output_dir
        )
        generator.run_analysis()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise


if __name__ == "__main__":
    main()