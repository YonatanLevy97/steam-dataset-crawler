"""
Technical Features Analysis Visualizations

Creates comprehensive visualizations analyzing technical features including
language support, controller support, DLC patterns, achievements, and
platform-specific characteristics across Steam game communities.
"""

import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from collections import Counter
import warnings

try:
    from .config import (
        COMMUNITY_COLORS, COMMUNITY_NAMES, FIGURE_SIZES, DPI_SETTINGS,
        get_community_color, get_plotly_layout, HEATMAP_PARAMS
    )
    from .data_loader import CommunityDataLoader
except ImportError:
    from config import (
        COMMUNITY_COLORS, COMMUNITY_NAMES, FIGURE_SIZES, DPI_SETTINGS,
        get_community_color, get_plotly_layout, HEATMAP_PARAMS
    )
    from data_loader import CommunityDataLoader

class TechnicalFeaturesAnalyzer:
    """
    Creates technical features analysis visualizations for Steam game communities.
    """
    
    def __init__(self, data_loader: CommunityDataLoader, output_dir: str = None):
        """
        Initialize the analyzer.
        
        Args:
            data_loader (CommunityDataLoader): Loaded community data
            output_dir (str, optional): Output directory for plots
        """
        self.data_loader = data_loader
        self.output_dir = Path(output_dir) if output_dir else Path('outputs/static_plots/technical_features')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load required data
        if data_loader.community_profiles is None:
            data_loader.load_community_profiles()
        if data_loader.detailed_profiles is None:
            data_loader.load_detailed_profiles()
    
    def create_language_support_analysis(self, top_n_languages: int = 8, save_plots: bool = True) -> Dict:
        """
        Create language support analysis visualizations.
        
        Args:
            top_n_languages (int): Number of top languages to analyze
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing matplotlib and plotly figures
        """
        # Get language data
        language_data = self.data_loader.get_language_data()
        
        # Get top languages across all communities
        top_languages = (language_data.groupby('language')['percentage']
                        .mean()
                        .sort_values(ascending=False)
                        .head(top_n_languages)
                        .index.tolist())
        
        # Create language matrix
        language_matrix = (language_data[language_data['language'].isin(top_languages)]
                          .pivot(index='community_name', columns='language', values='percentage')
                          .fillna(0))
        
        figures = {}
        
        # Matplotlib version
        fig_mpl, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=FIGURE_SIZES['large'])
        
        # Language support heatmap
        sns.heatmap(language_matrix, annot=True, fmt='.1f', cmap='Blues',
                   ax=ax1, cbar_kws={'label': 'Support Percentage'})
        ax1.set_title(f'Top {top_n_languages} Language Support Across Communities', fontweight='bold')
        ax1.set_xlabel('Language')
        ax1.set_ylabel('Community')
        
        # Language diversity by community
        language_diversity = language_data.groupby('community_name').agg({
            'language': 'count',
            'percentage': 'mean'
        }).rename(columns={'language': 'num_languages', 'percentage': 'avg_support'})
        
        colors = [get_community_color(list(COMMUNITY_NAMES.values()).index(name)) 
                 for name in language_diversity.index]
        
        bars2 = ax2.bar(language_diversity.index, language_diversity['num_languages'],
                       color=colors, alpha=0.8, edgecolor='black', linewidth=0.5)
        ax2.set_xlabel('Community')
        ax2.set_ylabel('Number of Supported Languages')
        ax2.set_title('Language Diversity by Community', fontweight='bold')
        ax2.tick_params(axis='x', rotation=45)
        
        # Add value labels
        for bar, count in zip(bars2, language_diversity['num_languages']):
            ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                    f'{count}', ha='center', va='bottom', fontsize=9)
        
        # English dominance analysis
        english_data = language_data[language_data['language'] == 'English']
        if not english_data.empty:
            english_sorted = english_data.sort_values('percentage', ascending=True)
            colors_eng = [get_community_color(list(COMMUNITY_NAMES.values()).index(name)) 
                         for name in english_sorted['community_name']]
            
            bars3 = ax3.barh(english_sorted['community_name'], english_sorted['percentage'],
                            color=colors_eng, alpha=0.8, edgecolor='black', linewidth=0.5)
            ax3.set_xlabel('English Support Percentage')
            ax3.set_title('English Language Support by Community', fontweight='bold')
            
            # Add percentage labels
            for bar, pct in zip(bars3, english_sorted['percentage']):
                ax3.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2,
                        f'{pct:.1f}%', va='center', ha='left', fontsize=8)
        
        # International appeal (non-English languages)
        non_english_data = language_data[language_data['language'] != 'English']
        international_support = non_english_data.groupby('community_name')['percentage'].mean().sort_values(ascending=True)
        
        colors_intl = [get_community_color(list(COMMUNITY_NAMES.values()).index(name)) 
                      for name in international_support.index]
        
        bars4 = ax4.barh(international_support.index, international_support.values,
                        color=colors_intl, alpha=0.8, edgecolor='black', linewidth=0.5)
        ax4.set_xlabel('Average Non-English Support Percentage')
        ax4.set_title('International Language Appeal', fontweight='bold')
        
        plt.tight_layout()
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / 'language_support_analysis.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
        
        # Plotly version - Interactive heatmap
        fig_plotly = go.Figure(data=go.Heatmap(
            z=language_matrix.values,
            x=language_matrix.columns,
            y=language_matrix.index,
            colorscale='Blues',
            hoverongaps=False,
            hovertemplate='Community: %{y}<br>Language: %{x}<br>Support: %{z:.1f}%<extra></extra>',
            text=language_matrix.values.round(1),
            texttemplate='%{text}%',
            textfont={"size": 9}
        ))
        
        fig_plotly.update_layout(
            **get_plotly_layout(
                title='Interactive Language Support Matrix',
                xaxis_title='Language',
                yaxis_title='Community',
                height=600
            )
        )
        
        figures['plotly'] = fig_plotly
        
        if save_plots:
            fig_plotly.write_html(self.output_dir / 'language_support_interactive.html')
        
        return figures
    
    def create_controller_support_analysis(self, save_plots: bool = True) -> Dict:
        """
        Create controller support analysis visualizations.
        
        Args:
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing matplotlib and plotly figures
        """
        df = self.data_loader.community_profiles.copy()
        
        # Extract controller support data from detailed profiles
        controller_data = []
        
        for comm_id, comm_name in COMMUNITY_NAMES.items():
            if str(comm_id) not in self.data_loader.detailed_profiles['community_profiles']:
                continue
            
            profile = self.data_loader.detailed_profiles['community_profiles'][str(comm_id)]
            controller_support = profile['categorical_features'].get('controller_support', {})
            
            if 'top_values' in controller_support:
                for ctrl_info in controller_support['top_values']:
                    controller_data.append({
                        'community_id': comm_id,
                        'community_name': comm_name,
                        'controller_type': ctrl_info['value'],
                        'percentage': ctrl_info['percentage'],
                        'count': ctrl_info['count']
                    })
        
        controller_df = pd.DataFrame(controller_data)
        
        figures = {}
        
        # Matplotlib version
        fig_mpl, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=FIGURE_SIZES['large'])
        
        # Controller support types distribution
        if not controller_df.empty:
            controller_types = controller_df['controller_type'].unique()
            controller_matrix = controller_df.pivot_table(
                index='community_name',
                columns='controller_type',
                values='percentage',
                fill_value=0
            )
            
            # Stacked bar chart
            controller_matrix.plot(kind='bar', stacked=True, ax=ax1, 
                                 colormap='Set3', alpha=0.8)
            ax1.set_title('Controller Support Types by Community', fontweight='bold')
            ax1.set_xlabel('Community')
            ax1.set_ylabel('Percentage')
            ax1.tick_params(axis='x', rotation=45)
            ax1.legend(title='Controller Support', bbox_to_anchor=(1.05, 1), loc='upper left')
            
            # Full vs Partial controller support
            full_support = controller_df[controller_df['controller_type'].str.contains('Full', na=False)]
            partial_support = controller_df[controller_df['controller_type'].str.contains('Partial', na=False)]
            
            if not full_support.empty and not partial_support.empty:
                full_avg = full_support.groupby('community_name')['percentage'].sum()
                partial_avg = partial_support.groupby('community_name')['percentage'].sum()
                
                communities = list(set(full_avg.index) | set(partial_avg.index))
                full_values = [full_avg.get(comm, 0) for comm in communities]
                partial_values = [partial_avg.get(comm, 0) for comm in communities]
                
                x_pos = np.arange(len(communities))
                width = 0.35
                
                ax2.bar(x_pos - width/2, full_values, width, label='Full Support', 
                       alpha=0.8, color='darkgreen')
                ax2.bar(x_pos + width/2, partial_values, width, label='Partial Support', 
                       alpha=0.8, color='orange')
                
                ax2.set_xlabel('Community')
                ax2.set_ylabel('Support Percentage')
                ax2.set_title('Full vs Partial Controller Support', fontweight='bold')
                ax2.set_xticks(x_pos)
                ax2.set_xticklabels([comm[:8] + '...' if len(comm) > 8 else comm 
                                   for comm in communities], rotation=45)
                ax2.legend()
        
        # Platform support vs controller support correlation
        colors = [get_community_color(i) for i in df['community_id']]
        
        # Assuming we have controller support percentage in main df
        if 'controller_support_most_common' in df.columns:
            # Calculate overall controller support score
            df['controller_support_score'] = df['controller_support_most_common'].map({
                'Full Xbox Controller Support': 100,
                'Partial Xbox Controller Support': 50,
                'No controller support': 0
            }).fillna(0)
            
            scatter3 = ax3.scatter(df['windows_true_percentage'], df['controller_support_score'],
                                  c=colors, s=df['size']/15, alpha=0.7, edgecolors='black')
            ax3.set_xlabel('Windows Support Percentage')
            ax3.set_ylabel('Controller Support Score')
            ax3.set_title('Platform vs Controller Support\n(Bubble size = Community size)', 
                         fontweight='bold')
            ax3.grid(True, alpha=0.3)
            
            # Mac vs Controller support
            scatter4 = ax4.scatter(df['mac_true_percentage'], df['controller_support_score'],
                                  c=colors, s=80, alpha=0.7, edgecolors='black')
            ax4.set_xlabel('Mac Support Percentage')
            ax4.set_ylabel('Controller Support Score')
            ax4.set_title('Mac vs Controller Support', fontweight='bold')
            ax4.grid(True, alpha=0.3)
        else:
            # Fallback visualization
            ax3.text(0.5, 0.5, 'Controller Support\nData Not Available', 
                    ha='center', va='center', transform=ax3.transAxes,
                    fontsize=12, bbox=dict(boxstyle="round,pad=0.3", facecolor="lightgray"))
            ax4.text(0.5, 0.5, 'Controller Support\nData Not Available', 
                    ha='center', va='center', transform=ax4.transAxes,
                    fontsize=12, bbox=dict(boxstyle="round,pad=0.3", facecolor="lightgray"))
        
        plt.tight_layout()
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / 'controller_support_analysis.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
        
        # Plotly version - Interactive controller support matrix
        if not controller_df.empty:
            fig_plotly = px.bar(
                controller_df,
                x='community_name',
                y='percentage',
                color='controller_type',
                title='Controller Support Types Across Communities - Interactive View',
                labels={'percentage': 'Support Percentage', 'community_name': 'Community'},
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            
            fig_plotly.update_layout(
                **get_plotly_layout(
                    height=600,
                    xaxis_tickangle=-45
                )
            )
            
            figures['plotly'] = fig_plotly
            
            if save_plots:
                fig_plotly.write_html(self.output_dir / 'controller_support_interactive.html')
        
        return figures
    
    def create_dlc_achievements_analysis(self, save_plots: bool = True) -> Dict:
        """
        Create DLC and achievements patterns analysis.
        
        Args:
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing matplotlib and plotly figures
        """
        df = self.data_loader.community_profiles.copy()
        
        # Calculate content richness metrics
        df['content_richness_score'] = (
            (df['dlc_count_mean'] / df['dlc_count_mean'].max() * 50) +
            (df['achievements_total_mean'] / df['achievements_total_mean'].max() * 50)
        )
        
        df['high_dlc'] = df['has_dlc_true_percentage'] > df['has_dlc_true_percentage'].median()
        df['high_achievements'] = df['achievements_total_mean'] > df['achievements_total_mean'].median()
        
        figures = {}
        
        # Matplotlib version
        fig_mpl, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=FIGURE_SIZES['large'])
        
        colors = [get_community_color(i) for i in df['community_id']]
        
        # DLC vs Achievements scatter
        scatter1 = ax1.scatter(df['dlc_count_mean'], df['achievements_total_mean'],
                              c=colors, s=df['size']/15, alpha=0.7, edgecolors='black')
        ax1.set_xlabel('Average DLC Count')
        ax1.set_ylabel('Average Achievements Count')
        ax1.set_title('DLC vs Achievements Relationship\n(Bubble size = Community size)', 
                     fontweight='bold')
        ax1.grid(True, alpha=0.3)
        
        # Add correlation coefficient
        corr_dlc_ach = df['dlc_count_mean'].corr(df['achievements_total_mean'])
        ax1.text(0.05, 0.95, f'Correlation: {corr_dlc_ach:.3f}', transform=ax1.transAxes,
                fontsize=10, fontweight='bold', bbox=dict(boxstyle="round,pad=0.3", 
                facecolor="white", alpha=0.8))
        
        # DLC percentage distribution
        df_dlc_sorted = df.sort_values('has_dlc_true_percentage', ascending=True)
        bars2 = ax2.barh(df_dlc_sorted['community_name'], 
                        df_dlc_sorted['has_dlc_true_percentage'],
                        color=[get_community_color(i) for i in df_dlc_sorted['community_id']],
                        alpha=0.8, edgecolor='black', linewidth=0.5)
        
        ax2.set_xlabel('Games with DLC (%)')
        ax2.set_title('DLC Adoption by Community', fontweight='bold')
        ax2.axvline(x=df['has_dlc_true_percentage'].median(), color='red', 
                   linestyle='--', alpha=0.7, label='Median')
        ax2.legend()
        
        # Content richness categories
        content_categories = pd.cut(df['content_richness_score'],
                                   bins=[0, 25, 50, 75, 100],
                                   labels=['Low', 'Medium', 'High', 'Very High'])
        
        if not content_categories.isna().all():
            content_counts = content_categories.value_counts()
            colors_content = plt.cm.viridis(np.linspace(0, 1, len(content_counts)))
            
            wedges, texts, autotexts = ax3.pie(content_counts.values, labels=content_counts.index,
                                              colors=colors_content, autopct='%1.1f%%',
                                              startangle=90)
            ax3.set_title('Content Richness Distribution', fontweight='bold')
            
            # Improve pie chart text
            for autotext in autotexts:
                autotext.set_color('white')
                autotext.set_fontweight('bold')
                autotext.set_fontsize(8)
        
        # Achievements vs Community Size
        scatter4 = ax4.scatter(df['size'], df['achievements_total_mean'],
                              c=colors, s=80, alpha=0.7, edgecolors='black')
        ax4.set_xlabel('Community Size')
        ax4.set_ylabel('Average Achievements Count')
        ax4.set_title('Community Size vs Achievement Complexity', fontweight='bold')
        ax4.grid(True, alpha=0.3)
        
        # Add trend line
        z = np.polyfit(df['size'], df['achievements_total_mean'], 1)
        p = np.poly1d(z)
        ax4.plot(df['size'], p(df['size']), "r--", alpha=0.8, linewidth=2)
        
        plt.tight_layout()
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / 'dlc_achievements_analysis.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
        
        # Plotly version - Interactive content analysis
        fig_plotly = go.Figure()
        
        fig_plotly.add_trace(go.Scatter(
            x=df['dlc_count_mean'],
            y=df['achievements_total_mean'],
            mode='markers+text',
            marker=dict(
                size=df['size']/10,
                color=[get_community_color(i) for i in df['community_id']],
                opacity=0.7,
                line=dict(width=1, color='black'),
                sizemode='diameter'
            ),
            text=df['community_name'],
            textposition='top center',
            hovertemplate=
                '<b>%{text}</b><br>' +
                'Avg DLC Count: %{x:.2f}<br>' +
                'Avg Achievements: %{y:.1f}<br>' +
                'DLC Adoption: %{customdata[0]:.1f}%<br>' +
                'Content Richness: %{customdata[1]:.1f}<br>' +
                'Community Size: %{marker.size}<br>' +
                '<extra></extra>',
            customdata=np.column_stack((
                df['has_dlc_true_percentage'],
                df['content_richness_score']
            )),
            name='Communities'
        ))
        
        fig_plotly.update_layout(
            **get_plotly_layout(
                title='DLC vs Achievements Analysis - Interactive View<br><sub>Bubble size represents community size</sub>',
                xaxis_title='Average DLC Count',
                yaxis_title='Average Achievements Count',
                height=600
            )
        )
        
        figures['plotly'] = fig_plotly
        
        if save_plots:
            fig_plotly.write_html(self.output_dir / 'dlc_achievements_interactive.html')
        
        return figures
    
    def create_free_vs_paid_analysis(self, save_plots: bool = True) -> Dict:
        """
        Create free vs paid games analysis.
        
        Args:
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing matplotlib and plotly figures
        """
        df = self.data_loader.community_profiles.copy()
        
        # Calculate pricing categories
        df['pricing_model'] = 'Mixed'
        df.loc[df['is_free_true_percentage'] > 50, 'pricing_model'] = 'Mostly Free'
        df.loc[df['is_free_true_percentage'] < 10, 'pricing_model'] = 'Mostly Paid'
        
        df['price_tier'] = pd.cut(df['average_price'],
                                 bins=[0, 10, 25, 50, 100, float('inf')],
                                 labels=['Budget (<$10)', 'Affordable ($10-25)', 
                                        'Mid-range ($25-50)', 'Premium ($50-100)', 
                                        'Luxury (>$100)'])
        
        figures = {}
        
        # Matplotlib version
        fig_mpl, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=FIGURE_SIZES['large'])
        
        colors = [get_community_color(i) for i in df['community_id']]
        
        # Free games percentage by community
        df_free_sorted = df.sort_values('is_free_true_percentage', ascending=True)
        bars1 = ax1.barh(df_free_sorted['community_name'], 
                        df_free_sorted['is_free_true_percentage'],
                        color=[get_community_color(i) for i in df_free_sorted['community_id']],
                        alpha=0.8, edgecolor='black', linewidth=0.5)
        
        ax1.set_xlabel('Free Games Percentage')
        ax1.set_title('Free Games Distribution by Community', fontweight='bold')
        
        # Add percentage labels
        for bar, pct in zip(bars1, df_free_sorted['is_free_true_percentage']):
            ax1.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2,
                    f'{pct:.1f}%', va='center', ha='left', fontsize=8)
        
        # Pricing model distribution
        pricing_counts = df['pricing_model'].value_counts()
        colors_pricing = ['lightcoral', 'lightblue', 'lightgreen'][:len(pricing_counts)]
        
        wedges, texts, autotexts = ax2.pie(pricing_counts.values, labels=pricing_counts.index,
                                          colors=colors_pricing, autopct='%1.1f%%',
                                          startangle=90)
        ax2.set_title('Community Pricing Models', fontweight='bold')
        
        # Free percentage vs Average price
        scatter3 = ax3.scatter(df['is_free_true_percentage'], df['average_price'],
                              c=colors, s=df['size']/15, alpha=0.7, edgecolors='black')
        ax3.set_xlabel('Free Games Percentage')
        ax3.set_ylabel('Average Price ($)')
        ax3.set_title('Free Games vs Pricing\n(Bubble size = Community size)', fontweight='bold')
        ax3.grid(True, alpha=0.3)
        
        # Add community labels for extreme points
        for _, row in df.iterrows():
            if row['is_free_true_percentage'] > 30 or row['average_price'] > 50:
                ax3.annotate(row['community_name'], 
                           (row['is_free_true_percentage'], row['average_price']),
                           xytext=(5, 5), textcoords='offset points',
                           fontsize=8, alpha=0.8)
        
        # Price tier distribution
        if not df['price_tier'].isna().all():
            price_tier_counts = df['price_tier'].value_counts()
            bars4 = ax4.bar(range(len(price_tier_counts)), price_tier_counts.values,
                           color=plt.cm.viridis(np.linspace(0, 1, len(price_tier_counts))),
                           alpha=0.8, edgecolor='black', linewidth=0.5)
            ax4.set_xticks(range(len(price_tier_counts)))
            ax4.set_xticklabels(price_tier_counts.index, rotation=45)
            ax4.set_ylabel('Number of Communities')
            ax4.set_title('Communities by Price Tier', fontweight='bold')
            
            # Add value labels
            for bar, count in zip(bars4, price_tier_counts.values):
                ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                        f'{count}', ha='center', va='bottom', fontsize=9)
        
        plt.tight_layout()
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / 'free_vs_paid_analysis.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
        
        # Plotly version - Interactive pricing analysis
        fig_plotly = go.Figure()
        
        fig_plotly.add_trace(go.Scatter(
            x=df['is_free_true_percentage'],
            y=df['average_price'],
            mode='markers+text',
            marker=dict(
                size=df['size']/10,
                color=[get_community_color(i) for i in df['community_id']],
                opacity=0.7,
                line=dict(width=1, color='black'),
                sizemode='diameter'
            ),
            text=df['community_name'],
            textposition='top center',
            hovertemplate=
                '<b>%{text}</b><br>' +
                'Free Games: %{x:.1f}%<br>' +
                'Avg Price: $%{y:.2f}<br>' +
                'Median Price: $%{customdata[0]:.2f}<br>' +
                'Pricing Model: %{customdata[1]}<br>' +
                'Price Coverage: %{customdata[2]:.1f}%<br>' +
                'Community Size: %{marker.size}<br>' +
                '<extra></extra>',
            customdata=np.column_stack((
                df['median_price'],
                df['pricing_model'],
                df['price_coverage']
            )),
            name='Communities'
        ))
        
        fig_plotly.update_layout(
            **get_plotly_layout(
                title='Free Games vs Pricing Analysis - Interactive View<br><sub>Bubble size represents community size</sub>',
                xaxis_title='Free Games Percentage',
                yaxis_title='Average Price ($)',
                height=600
            )
        )
        
        figures['plotly'] = fig_plotly
        
        if save_plots:
            fig_plotly.write_html(self.output_dir / 'free_vs_paid_interactive.html')
        
        return figures
    
    def create_platform_compatibility_matrix(self, save_plots: bool = True) -> Dict:
        """
        Create comprehensive platform compatibility analysis.
        
        Args:
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing matplotlib and plotly figures
        """
        df = self.data_loader.community_profiles.copy()
        
        # Platform combination analysis
        df['windows_only'] = (df['windows_true_percentage'] > 90) & \
                            (df['mac_true_percentage'] < 10) & \
                            (df['linux_true_percentage'] < 10)
        
        df['cross_platform'] = (df['windows_true_percentage'] > 50) & \
                              (df['mac_true_percentage'] > 20) & \
                              (df['linux_true_percentage'] > 10)
        
        df['platform_category'] = 'Mixed'
        df.loc[df['windows_only'], 'platform_category'] = 'Windows Only'
        df.loc[df['cross_platform'], 'platform_category'] = 'Cross-Platform'
        
        # Calculate platform diversity score
        platform_cols = ['windows_true_percentage', 'mac_true_percentage', 'linux_true_percentage']
        df['platform_diversity_score'] = df[platform_cols].apply(
            lambda x: len([p for p in x if p > 20]), axis=1
        )
        
        figures = {}
        
        # Matplotlib version
        fig_mpl, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=FIGURE_SIZES['large'])
        
        # Platform support heatmap
        platform_data = df[['community_name'] + platform_cols].set_index('community_name')
        platform_data.columns = ['Windows', 'Mac', 'Linux']
        
        sns.heatmap(platform_data, annot=True, fmt='.1f', cmap='RdYlGn',
                   ax=ax1, cbar_kws={'label': 'Support Percentage'})
        ax1.set_title('Platform Support Matrix', fontweight='bold')
        ax1.set_xlabel('Platform')
        ax1.set_ylabel('Community')
        
        # Platform diversity score
        colors = [get_community_color(i) for i in df['community_id']]
        
        bars2 = ax2.bar(df['community_name'], df['platform_diversity_score'],
                       color=colors, alpha=0.8, edgecolor='black', linewidth=0.5)
        ax2.set_xlabel('Community')
        ax2.set_ylabel('Platform Diversity Score')
        ax2.set_title('Platform Diversity by Community', fontweight='bold')
        ax2.tick_params(axis='x', rotation=45)
        ax2.set_ylim(0, 3.5)
        
        # Add value labels
        for bar, score in zip(bars2, df['platform_diversity_score']):
            ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                    f'{score}', ha='center', va='bottom', fontsize=9)
        
        # Platform category distribution
        platform_cat_counts = df['platform_category'].value_counts()
        colors_cat = ['lightblue', 'lightgreen', 'lightcoral'][:len(platform_cat_counts)]
        
        wedges, texts, autotexts = ax3.pie(platform_cat_counts.values, 
                                          labels=platform_cat_counts.index,
                                          colors=colors_cat, autopct='%1.1f%%',
                                          startangle=90)
        ax3.set_title('Platform Compatibility Categories', fontweight='bold')
        
        # Mac vs Linux support correlation
        scatter4 = ax4.scatter(df['mac_true_percentage'], df['linux_true_percentage'],
                              c=colors, s=df['size']/15, alpha=0.7, edgecolors='black')
        ax4.set_xlabel('Mac Support Percentage')
        ax4.set_ylabel('Linux Support Percentage')
        ax4.set_title('Mac vs Linux Support\n(Bubble size = Community size)', fontweight='bold')
        ax4.grid(True, alpha=0.3)
        
        # Add correlation coefficient
        corr_mac_linux = df['mac_true_percentage'].corr(df['linux_true_percentage'])
        ax4.text(0.05, 0.95, f'Correlation: {corr_mac_linux:.3f}', transform=ax4.transAxes,
                fontsize=10, fontweight='bold', bbox=dict(boxstyle="round,pad=0.3", 
                facecolor="white", alpha=0.8))
        
        plt.tight_layout()
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / 'platform_compatibility_matrix.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
        
        # Plotly version - Interactive 3D platform analysis
        fig_plotly = go.Figure(data=go.Scatter3d(
            x=df['windows_true_percentage'],
            y=df['mac_true_percentage'],
            z=df['linux_true_percentage'],
            mode='markers+text',
            marker=dict(
                size=df['size']/30,
                color=[get_community_color(i) for i in df['community_id']],
                opacity=0.8,
                line=dict(width=1, color='black')
            ),
            text=df['community_name'],
            textposition='top center',
            hovertemplate=
                '<b>%{text}</b><br>' +
                'Windows: %{x:.1f}%<br>' +
                'Mac: %{y:.1f}%<br>' +
                'Linux: %{z:.1f}%<br>' +
                'Platform Category: %{customdata[0]}<br>' +
                'Diversity Score: %{customdata[1]}<br>' +
                '<extra></extra>',
            customdata=np.column_stack((
                df['platform_category'],
                df['platform_diversity_score']
            ))
        ))
        
        fig_plotly.update_layout(
            **get_plotly_layout(
                title='3D Platform Support Analysis - Interactive View',
                height=700
            ),
            scene=dict(
                xaxis_title='Windows Support (%)',
                yaxis_title='Mac Support (%)',
                zaxis_title='Linux Support (%)'
            )
        )
        
        figures['plotly'] = fig_plotly
        
        if save_plots:
            fig_plotly.write_html(self.output_dir / 'platform_compatibility_3d.html')
        
        return figures
    
    def generate_all_technical_features_plots(self, save_plots: bool = True) -> Dict:
        """
        Generate all technical features analysis visualizations.
        
        Args:
            save_plots (bool): Whether to save all plots
            
        Returns:
            Dict: All generated figures organized by category
        """
        print("🎨 Generating technical features analysis visualizations...")
        
        all_figures = {}
        
        # Generate each visualization
        print("  🌐 Creating language support analysis...")
        all_figures['language_support'] = self.create_language_support_analysis(save_plots=save_plots)
        
        print("  🎮 Creating controller support analysis...")
        all_figures['controller_support'] = self.create_controller_support_analysis(save_plots=save_plots)
        
        print("  📦 Creating DLC and achievements analysis...")
        all_figures['dlc_achievements'] = self.create_dlc_achievements_analysis(save_plots=save_plots)
        
        print("  💰 Creating free vs paid analysis...")
        all_figures['free_vs_paid'] = self.create_free_vs_paid_analysis(save_plots=save_plots)
        
        print("  💻 Creating platform compatibility matrix...")
        all_figures['platform_compatibility'] = self.create_platform_compatibility_matrix(save_plots=save_plots)
        
        print(f"✅ Technical features analysis complete! Saved to: {self.output_dir}")
        return all_figures

# =============================================================================
# COMMAND LINE INTERFACE
# =============================================================================

def main():
    """Command line interface for technical features analysis visualizations."""
    import argparse
    try:
        from .data_loader import load_data
    except ImportError:
        from data_loader import load_data
    
    parser = argparse.ArgumentParser(description='Generate technical features analysis visualizations')
    parser.add_argument('--data-dir', type=str, default=None,
                       help='Directory containing community data files')
    parser.add_argument('--output-dir', type=str, 
                       default='communities_visualizations/outputs/static_plots/technical_features',
                       help='Output directory for generated plots')
    parser.add_argument('--top-languages', type=int, default=8,
                       help='Number of top languages to analyze (default: 8)')
    parser.add_argument('--no-save', action='store_true',
                       help='Don\'t save plots to files (display only)')
    
    args = parser.parse_args()
    
    try:
        # Load data
        print("📥 Loading community data...")
        data_loader = load_data(args.data_dir)
        
        # Create analyzer
        analyzer = TechnicalFeaturesAnalyzer(data_loader, args.output_dir)
        
        # Generate all plots
        figures = analyzer.generate_all_technical_features_plots(save_plots=not args.no_save)
        
        print("🎉 Technical features analysis visualization generation complete!")
        
        # Show summary
        total_plots = sum(len(category_figs) for category_figs in figures.values())
        print(f"Generated {total_plots} visualizations across {len(figures)} categories")
        
    except Exception as e:
        print(f"❌ Error generating visualizations: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()