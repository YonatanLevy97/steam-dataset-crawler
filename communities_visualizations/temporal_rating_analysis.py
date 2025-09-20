"""
Temporal and Rating Analysis Visualizations

Creates comprehensive visualizations analyzing release patterns over time,
review scores, age ratings, and temporal trends across Steam game communities.
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
from datetime import datetime, timedelta
import warnings

try:
    from .config import (
        COMMUNITY_COLORS, COMMUNITY_NAMES, FIGURE_SIZES, DPI_SETTINGS,
        get_community_color, get_plotly_layout, BOXPLOT_PARAMS
    )
    from .data_loader import CommunityDataLoader
except ImportError:
    from config import (
        COMMUNITY_COLORS, COMMUNITY_NAMES, FIGURE_SIZES, DPI_SETTINGS,
        get_community_color, get_plotly_layout, BOXPLOT_PARAMS
    )
    from data_loader import CommunityDataLoader

class TemporalRatingAnalyzer:
    """
    Creates temporal and rating analysis visualizations for Steam game communities.
    """
    
    def __init__(self, data_loader: CommunityDataLoader, output_dir: str = None):
        """
        Initialize the analyzer.
        
        Args:
            data_loader (CommunityDataLoader): Loaded community data
            output_dir (str, optional): Output directory for plots
        """
        self.data_loader = data_loader
        self.output_dir = Path(output_dir) if output_dir else Path('outputs/static_plots/temporal_ratings')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load required data
        if data_loader.community_profiles is None:
            data_loader.load_community_profiles()
        if data_loader.detailed_profiles is None:
            data_loader.load_detailed_profiles()
    
    def create_release_timeline_analysis(self, save_plots: bool = True) -> Dict:
        """
        Create release timeline analysis visualizations.
        
        Args:
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing matplotlib and plotly figures
        """
        df = self.data_loader.community_profiles.copy()
        
        # Calculate additional temporal metrics
        current_year = datetime.now().year
        df['years_since_avg_release'] = current_year - df['release_year_mean']
        df['is_modern'] = df['release_year_mean'] >= 2015
        df['era'] = pd.cut(df['release_year_mean'], 
                          bins=[1990, 2005, 2010, 2015, 2020, current_year+1],
                          labels=['Retro (≤2005)', 'Classic (2006-2010)', 
                                 'Indie Boom (2011-2015)', 'Modern (2016-2020)', 
                                 'Current (2021+)'],
                          right=False)
        
        figures = {}
        
        # Matplotlib version
        fig_mpl, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=FIGURE_SIZES['large'])
        
        colors = [get_community_color(i) for i in df['community_id']]
        
        # Community formation timeline
        df_sorted = df.sort_values('release_year_mean')
        bars1 = ax1.barh(df_sorted['community_name'], df_sorted['release_year_mean'],
                        color=[get_community_color(i) for i in df_sorted['community_id']],
                        alpha=0.8, edgecolor='black', linewidth=0.5)
        
        ax1.set_xlabel('Average Release Year')
        ax1.set_title('Community Formation Timeline', fontweight='bold')
        ax1.axvline(x=2015, color='red', linestyle='--', alpha=0.7, label='Steam Boom')
        ax1.legend()
        
        # Add year labels
        for bar, year in zip(bars1, df_sorted['release_year_mean']):
            ax1.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2,
                    f'{year:.1f}', va='center', ha='left', fontsize=8)
        
        # Community size vs release year
        scatter2 = ax2.scatter(df['release_year_mean'], df['size'],
                              c=colors, s=80, alpha=0.7, edgecolors='black')
        ax2.set_xlabel('Average Release Year')
        ax2.set_ylabel('Community Size')
        ax2.set_title('Community Size vs Release Timeline', fontweight='bold')
        ax2.grid(True, alpha=0.3)
        
        # Add trend line
        z = np.polyfit(df['release_year_mean'], df['size'], 1)
        p = np.poly1d(z)
        ax2.plot(df['release_year_mean'], p(df['release_year_mean']), 
                "r--", alpha=0.8, linewidth=2)
        
        # Era distribution
        era_counts = df['era'].value_counts()
        colors_era = plt.cm.viridis(np.linspace(0, 1, len(era_counts)))
        
        wedges, texts, autotexts = ax3.pie(era_counts.values, labels=era_counts.index,
                                          colors=colors_era, autopct='%1.1f%%',
                                          startangle=90)
        ax3.set_title('Communities by Release Era', fontweight='bold')
        
        # Improve pie chart text
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
            autotext.set_fontsize(8)
        
        # Years since release distribution
        bars4 = ax4.bar(df['community_name'], df['years_since_avg_release'],
                       color=colors, alpha=0.8, edgecolor='black', linewidth=0.5)
        ax4.set_xlabel('Community')
        ax4.set_ylabel('Years Since Average Release')
        ax4.set_title('Community "Age" Analysis', fontweight='bold')
        ax4.tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / 'release_timeline_analysis.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
        
        # Plotly version - Interactive timeline
        fig_plotly = go.Figure()
        
        fig_plotly.add_trace(go.Scatter(
            x=df['release_year_mean'],
            y=df['size'],
            mode='markers+text',
            marker=dict(
                size=df['years_since_avg_release']*2,  # Size based on age
                color=[get_community_color(i) for i in df['community_id']],
                opacity=0.7,
                line=dict(width=1, color='black'),
                sizemode='diameter'
            ),
            text=df['community_name'],
            textposition='top center',
            hovertemplate=
                '<b>%{text}</b><br>' +
                'Avg Release Year: %{x:.1f}<br>' +
                'Community Size: %{y}<br>' +
                'Years Since Release: %{customdata[0]:.1f}<br>' +
                'Era: %{customdata[1]}<br>' +
                'Coverage: %{customdata[2]:.1f}%<br>' +
                '<extra></extra>',
            customdata=np.column_stack((
                df['years_since_avg_release'],
                df['era'].astype(str),
                df['release_year_coverage']
            )),
            name='Communities'
        ))
        
        # Add era boundaries
        era_boundaries = [2005, 2010, 2015, 2020]
        for year in era_boundaries:
            fig_plotly.add_vline(
                x=year,
                line=dict(color="rgba(128,128,128,0.3)", width=1, dash="dash"),
                annotation_text=f"{year}",
                annotation_position="top"
            )
        
        fig_plotly.update_layout(
            **get_plotly_layout(
                title='Community Formation Timeline - Interactive View<br><sub>Bubble size represents years since average release</sub>',
                xaxis_title='Average Release Year',
                yaxis_title='Community Size',
                height=600
            )
        )
        
        figures['plotly'] = fig_plotly
        
        if save_plots:
            fig_plotly.write_html(self.output_dir / 'release_timeline_interactive.html')
        
        return figures
    
    def create_metacritic_score_analysis(self, save_plots: bool = True) -> Dict:
        """
        Create Metacritic score distribution analysis.
        
        Args:
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing matplotlib and plotly figures
        """
        df = self.data_loader.community_profiles.copy()
        
        # Filter communities with reasonable Metacritic coverage
        df_filtered = df[df['metacritic_score_coverage'] >= 5].copy()  # At least 5% coverage
        
        # Quality categories
        df_filtered['quality_tier'] = pd.cut(df_filtered['metacritic_score_mean'],
                                           bins=[0, 60, 70, 80, 90, 100],
                                           labels=['Poor (≤60)', 'Fair (61-70)', 
                                                  'Good (71-80)', 'Excellent (81-90)', 
                                                  'Masterpiece (91+)'],
                                           right=False)
        
        figures = {}
        
        # Matplotlib version
        fig_mpl, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=FIGURE_SIZES['large'])
        
        colors = [get_community_color(i) for i in df_filtered['community_id']]
        
        # Metacritic scores by community
        df_score_sorted = df_filtered.sort_values('metacritic_score_mean', ascending=True)
        bars1 = ax1.barh(df_score_sorted['community_name'], 
                        df_score_sorted['metacritic_score_mean'],
                        color=[get_community_color(i) for i in df_score_sorted['community_id']],
                        alpha=0.8, edgecolor='black', linewidth=0.5)
        
        ax1.set_xlabel('Average Metacritic Score')
        ax1.set_title('Average Metacritic Scores by Community', fontweight='bold')
        ax1.axvline(x=75, color='green', linestyle='--', alpha=0.7, label='Good Threshold')
        ax1.legend()
        
        # Add score labels
        for bar, score in zip(bars1, df_score_sorted['metacritic_score_mean']):
            ax1.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2,
                    f'{score:.1f}', va='center', ha='left', fontsize=8)
        
        # Score vs Coverage scatter
        scatter2 = ax2.scatter(df_filtered['metacritic_score_coverage'], 
                              df_filtered['metacritic_score_mean'],
                              c=colors, s=df_filtered['size']/15, alpha=0.7, 
                              edgecolors='black')
        ax2.set_xlabel('Metacritic Coverage (%)')
        ax2.set_ylabel('Average Metacritic Score')
        ax2.set_title('Score vs Coverage\n(Bubble size = Community size)', fontweight='bold')
        ax2.grid(True, alpha=0.3)
        
        # Add community labels for interesting points
        for _, row in df_filtered.iterrows():
            if row['metacritic_score_coverage'] > 50 or row['metacritic_score_mean'] > 85:
                ax2.annotate(row['community_name'], 
                           (row['metacritic_score_coverage'], row['metacritic_score_mean']),
                           xytext=(5, 5), textcoords='offset points',
                           fontsize=8, alpha=0.8)
        
        # Quality tier distribution
        if not df_filtered['quality_tier'].isna().all():
            quality_counts = df_filtered['quality_tier'].value_counts()
            colors_quality = plt.cm.RdYlGn(np.linspace(0.2, 0.8, len(quality_counts)))
            
            bars3 = ax3.bar(range(len(quality_counts)), quality_counts.values,
                           color=colors_quality, alpha=0.8, edgecolor='black', linewidth=0.5)
            ax3.set_xticks(range(len(quality_counts)))
            ax3.set_xticklabels(quality_counts.index, rotation=45)
            ax3.set_ylabel('Number of Communities')
            ax3.set_title('Communities by Quality Tier', fontweight='bold')
            
            # Add value labels
            for bar, count in zip(bars3, quality_counts.values):
                ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                        f'{count}', ha='center', va='bottom', fontsize=9)
        
        # Score vs Recommendations correlation
        scatter4 = ax4.scatter(df_filtered['recommendations_total_mean'], 
                              df_filtered['metacritic_score_mean'],
                              c=colors, s=80, alpha=0.7, edgecolors='black')
        ax4.set_xlabel('Average Recommendations')
        ax4.set_ylabel('Average Metacritic Score')
        ax4.set_title('Quality vs Popularity Correlation', fontweight='bold')
        ax4.grid(True, alpha=0.3)
        
        # Add correlation coefficient
        corr = df_filtered['recommendations_total_mean'].corr(df_filtered['metacritic_score_mean'])
        ax4.text(0.05, 0.95, f'Correlation: {corr:.3f}', transform=ax4.transAxes,
                fontsize=10, fontweight='bold', bbox=dict(boxstyle="round,pad=0.3", 
                facecolor="white", alpha=0.8))
        
        plt.tight_layout()
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / 'metacritic_score_analysis.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
        
        # Plotly version - Interactive bubble chart
        fig_plotly = go.Figure()
        
        fig_plotly.add_trace(go.Scatter(
            x=df_filtered['metacritic_score_coverage'],
            y=df_filtered['metacritic_score_mean'],
            mode='markers+text',
            marker=dict(
                size=df_filtered['size']/10,
                color=[get_community_color(i) for i in df_filtered['community_id']],
                opacity=0.7,
                line=dict(width=1, color='black'),
                sizemode='diameter'
            ),
            text=df_filtered['community_name'],
            textposition='top center',
            hovertemplate=
                '<b>%{text}</b><br>' +
                'Avg Metacritic Score: %{y:.1f}<br>' +
                'Coverage: %{x:.1f}%<br>' +
                'Median Score: %{customdata[0]:.1f}<br>' +
                'Quality Tier: %{customdata[1]}<br>' +
                'Community Size: %{marker.size}<br>' +
                '<extra></extra>',
            customdata=np.column_stack((
                df_filtered['metacritic_score_median'],
                df_filtered['quality_tier'].astype(str)
            )),
            name='Communities'
        ))
        
        fig_plotly.update_layout(
            **get_plotly_layout(
                title='Metacritic Score Analysis - Interactive View<br><sub>Bubble size represents community size</sub>',
                xaxis_title='Metacritic Coverage (%)',
                yaxis_title='Average Metacritic Score',
                height=600
            )
        )
        
        figures['plotly'] = fig_plotly
        
        if save_plots:
            fig_plotly.write_html(self.output_dir / 'metacritic_analysis_interactive.html')
        
        return figures
    
    def create_age_rating_analysis(self, save_plots: bool = True) -> Dict:
        """
        Create age rating distribution analysis.
        
        Args:
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing matplotlib and plotly figures
        """
        df = self.data_loader.community_profiles.copy()
        
        # Age rating categories
        df['age_category'] = pd.cut(df['required_age_mean'],
                                   bins=[-1, 0, 13, 17, 18, 100],
                                   labels=['All Ages (0)', 'Teen (1-13)', 
                                          'Mature Teen (14-17)', 'Adult (18)', 
                                          'Mature Adult (18+)'],
                                   right=False)
        
        figures = {}
        
        # Matplotlib version
        fig_mpl, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=FIGURE_SIZES['large'])
        
        colors = [get_community_color(i) for i in df['community_id']]
        
        # Age requirements by community
        df_age_sorted = df.sort_values('required_age_mean', ascending=True)
        bars1 = ax1.barh(df_age_sorted['community_name'], 
                        df_age_sorted['required_age_mean'],
                        color=[get_community_color(i) for i in df_age_sorted['community_id']],
                        alpha=0.8, edgecolor='black', linewidth=0.5)
        
        ax1.set_xlabel('Average Required Age')
        ax1.set_title('Content Age Requirements by Community', fontweight='bold')
        ax1.axvline(x=18, color='red', linestyle='--', alpha=0.7, label='Adult Content')
        ax1.legend()
        
        # Age category distribution
        if not df['age_category'].isna().all():
            age_counts = df['age_category'].value_counts()
            colors_age = plt.cm.RdYlBu_r(np.linspace(0.2, 0.8, len(age_counts)))
            
            wedges, texts, autotexts = ax2.pie(age_counts.values, labels=age_counts.index,
                                              colors=colors_age, autopct='%1.1f%%',
                                              startangle=90)
            ax2.set_title('Communities by Age Category', fontweight='bold')
            
            # Improve pie chart text
            for autotext in autotexts:
                autotext.set_color('white')
                autotext.set_fontweight('bold')
                autotext.set_fontsize(8)
        
        # Age vs Quality relationship
        scatter3 = ax3.scatter(df['required_age_mean'], df['metacritic_score_mean'],
                              c=colors, s=df['size']/15, alpha=0.7, edgecolors='black')
        ax3.set_xlabel('Average Required Age')
        ax3.set_ylabel('Average Metacritic Score')
        ax3.set_title('Age Requirements vs Quality\n(Bubble size = Community size)', 
                     fontweight='bold')
        ax3.grid(True, alpha=0.3)
        
        # Age vs Price relationship
        scatter4 = ax4.scatter(df['required_age_mean'], df['average_price'],
                              c=colors, s=80, alpha=0.7, edgecolors='black')
        ax4.set_xlabel('Average Required Age')
        ax4.set_ylabel('Average Price ($)')
        ax4.set_title('Age Requirements vs Pricing', fontweight='bold')
        ax4.grid(True, alpha=0.3)
        
        # Add correlation coefficient
        corr_price = df['required_age_mean'].corr(df['average_price'])
        ax4.text(0.05, 0.95, f'Correlation: {corr_price:.3f}', transform=ax4.transAxes,
                fontsize=10, fontweight='bold', bbox=dict(boxstyle="round,pad=0.3", 
                facecolor="white", alpha=0.8))
        
        plt.tight_layout()
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / 'age_rating_analysis.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
        
        # Plotly version - Multi-dimensional analysis
        fig_plotly = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Age Requirements Distribution', 
                           'Age vs Quality vs Size',
                           'Age Category Breakdown',
                           'Age vs Price Relationship'),
            specs=[[{"type": "bar"}, {"type": "scatter"}],
                   [{"type": "pie"}, {"type": "scatter"}]]
        )
        
        # Age requirements bar chart
        fig_plotly.add_trace(
            go.Bar(
                y=df_age_sorted['community_name'],
                x=df_age_sorted['required_age_mean'],
                orientation='h',
                marker_color=[get_community_color(i) for i in df_age_sorted['community_id']],
                name='Age Requirements'
            ),
            row=1, col=1
        )
        
        # Age vs Quality scatter
        fig_plotly.add_trace(
            go.Scatter(
                x=df['required_age_mean'],
                y=df['metacritic_score_mean'],
                mode='markers',
                marker=dict(
                    size=df['size']/15,
                    color=[get_community_color(i) for i in df['community_id']],
                    opacity=0.7,
                    line=dict(width=1, color='black')
                ),
                text=df['community_name'],
                name='Age vs Quality'
            ),
            row=1, col=2
        )
        
        # Age category pie chart
        if not df['age_category'].isna().all():
            age_counts = df['age_category'].value_counts()
            fig_plotly.add_trace(
                go.Pie(
                    labels=age_counts.index,
                    values=age_counts.values,
                    name='Age Categories'
                ),
                row=2, col=1
            )
        
        # Age vs Price scatter
        fig_plotly.add_trace(
            go.Scatter(
                x=df['required_age_mean'],
                y=df['average_price'],
                mode='markers',
                marker=dict(
                    size=8,
                    color=[get_community_color(i) for i in df['community_id']],
                    opacity=0.7,
                    line=dict(width=1, color='black')
                ),
                text=df['community_name'],
                name='Age vs Price'
            ),
            row=2, col=2
        )
        
        fig_plotly.update_layout(
            **get_plotly_layout(
                title='Comprehensive Age Rating Analysis',
                height=800,
                showlegend=False
            )
        )
        
        figures['plotly'] = fig_plotly
        
        if save_plots:
            fig_plotly.write_html(self.output_dir / 'age_rating_comprehensive.html')
        
        return figures
    
    def create_review_coverage_analysis(self, save_plots: bool = True) -> Dict:
        """
        Create analysis of review coverage and recommendation patterns.
        
        Args:
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing matplotlib and plotly figures
        """
        df = self.data_loader.community_profiles.copy()
        
        # Calculate review engagement metrics
        df['high_engagement'] = (df['recommendations_total_mean'] > df['recommendations_total_mean'].median()) & \
                               (df['metacritic_score_coverage'] > 10)
        
        df['review_ratio'] = df['metacritic_score_coverage'] / 100  # Normalize to 0-1
        
        figures = {}
        
        # Matplotlib version
        fig_mpl, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=FIGURE_SIZES['large'])
        
        colors = [get_community_color(i) for i in df['community_id']]
        
        # Coverage vs Recommendations
        scatter1 = ax1.scatter(df['metacritic_score_coverage'], 
                              df['recommendations_total_mean'],
                              c=colors, s=df['size']/15, alpha=0.7, edgecolors='black')
        ax1.set_xlabel('Metacritic Coverage (%)')
        ax1.set_ylabel('Average Recommendations')
        ax1.set_title('Review Coverage vs Community Engagement\n(Bubble size = Community size)', 
                     fontweight='bold')
        ax1.grid(True, alpha=0.3)
        
        # Coverage distribution
        bars2 = ax2.bar(df['community_name'], df['metacritic_score_coverage'],
                       color=colors, alpha=0.8, edgecolor='black', linewidth=0.5)
        ax2.set_xlabel('Community')
        ax2.set_ylabel('Metacritic Coverage (%)')
        ax2.set_title('Review Coverage by Community', fontweight='bold')
        ax2.tick_params(axis='x', rotation=45)
        ax2.axhline(y=df['metacritic_score_coverage'].median(), color='red', 
                   linestyle='--', alpha=0.7, label='Median Coverage')
        ax2.legend()
        
        # Engagement categories
        engagement_counts = df['high_engagement'].value_counts()
        colors_engagement = ['lightcoral', 'lightblue']
        
        wedges, texts, autotexts = ax3.pie(engagement_counts.values, 
                                          labels=['Low Engagement', 'High Engagement'],
                                          colors=colors_engagement, autopct='%1.1f%%',
                                          startangle=90)
        ax3.set_title('Community Engagement Levels', fontweight='bold')
        
        # Recommendations vs Size relationship
        scatter4 = ax4.scatter(df['size'], df['recommendations_total_mean'],
                              c=colors, s=80, alpha=0.7, edgecolors='black')
        ax4.set_xlabel('Community Size')
        ax4.set_ylabel('Average Recommendations')
        ax4.set_title('Community Size vs Average Recommendations', fontweight='bold')
        ax4.grid(True, alpha=0.3)
        
        # Add trend line
        z = np.polyfit(df['size'], df['recommendations_total_mean'], 1)
        p = np.poly1d(z)
        ax4.plot(df['size'], p(df['size']), "r--", alpha=0.8, linewidth=2)
        
        # Add correlation coefficient
        corr_size = df['size'].corr(df['recommendations_total_mean'])
        ax4.text(0.05, 0.95, f'Correlation: {corr_size:.3f}', transform=ax4.transAxes,
                fontsize=10, fontweight='bold', bbox=dict(boxstyle="round,pad=0.3", 
                facecolor="white", alpha=0.8))
        
        plt.tight_layout()
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / 'review_coverage_analysis.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
        
        # Plotly version - Interactive engagement analysis
        fig_plotly = go.Figure()
        
        fig_plotly.add_trace(go.Scatter(
            x=df['metacritic_score_coverage'],
            y=df['recommendations_total_mean'],
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
                'Metacritic Coverage: %{x:.1f}%<br>' +
                'Avg Recommendations: %{y:.0f}<br>' +
                'Community Size: %{marker.size}<br>' +
                'High Engagement: %{customdata[0]}<br>' +
                'Median Recommendations: %{customdata[1]:.0f}<br>' +
                '<extra></extra>',
            customdata=np.column_stack((
                df['high_engagement'].map({True: 'Yes', False: 'No'}),
                df['recommendations_total_median']
            )),
            name='Communities'
        ))
        
        fig_plotly.update_layout(
            **get_plotly_layout(
                title='Review Coverage vs Community Engagement<br><sub>Bubble size represents community size</sub>',
                xaxis_title='Metacritic Coverage (%)',
                yaxis_title='Average Recommendations',
                height=600
            )
        )
        
        figures['plotly'] = fig_plotly
        
        if save_plots:
            fig_plotly.write_html(self.output_dir / 'review_engagement_interactive.html')
        
        return figures
    
    def create_temporal_quality_evolution(self, save_plots: bool = True) -> Dict:
        """
        Create visualization showing quality evolution over time.
        
        Args:
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing matplotlib and plotly figures
        """
        df = self.data_loader.community_profiles.copy()
        
        # Create time-quality matrix
        df['decade'] = (df['release_year_mean'] // 10) * 10
        df['quality_score'] = (df['metacritic_score_mean'] * df['metacritic_score_coverage'] / 100) + \
                             (df['recommendations_total_mean'] / df['recommendations_total_mean'].max() * 100)
        
        figures = {}
        
        # Matplotlib version
        fig_mpl, (ax1, ax2) = plt.subplots(2, 1, figsize=FIGURE_SIZES['medium'])
        
        colors = [get_community_color(i) for i in df['community_id']]
        
        # Quality vs Release Year
        scatter1 = ax1.scatter(df['release_year_mean'], df['metacritic_score_mean'],
                              c=colors, s=df['size']/15, alpha=0.7, edgecolors='black')
        ax1.set_xlabel('Average Release Year')
        ax1.set_ylabel('Average Metacritic Score')
        ax1.set_title('Quality Evolution Over Time\n(Bubble size = Community size)', 
                     fontweight='bold')
        ax1.grid(True, alpha=0.3)
        
        # Add trend line
        valid_mask = ~(np.isnan(df['release_year_mean']) | np.isnan(df['metacritic_score_mean']))
        if valid_mask.sum() > 1:
            z = np.polyfit(df.loc[valid_mask, 'release_year_mean'], 
                          df.loc[valid_mask, 'metacritic_score_mean'], 1)
            p = np.poly1d(z)
            x_trend = np.linspace(df['release_year_mean'].min(), df['release_year_mean'].max(), 100)
            ax1.plot(x_trend, p(x_trend), "r--", alpha=0.8, linewidth=2, label='Trend')
            ax1.legend()
        
        # Community labels for interesting points
        for _, row in df.iterrows():
            if row['metacritic_score_mean'] > 85 or row['release_year_mean'] < 2010:
                ax1.annotate(row['community_name'], 
                           (row['release_year_mean'], row['metacritic_score_mean']),
                           xytext=(5, 5), textcoords='offset points',
                           fontsize=8, alpha=0.8)
        
        # Decade-wise quality distribution
        decade_quality = df.groupby('decade').agg({
            'metacritic_score_mean': 'mean',
            'recommendations_total_mean': 'mean',
            'size': 'sum'
        }).reset_index()
        
        # Create bar chart
        x_pos = np.arange(len(decade_quality))
        bars = ax2.bar(x_pos, decade_quality['metacritic_score_mean'],
                      alpha=0.8, color='skyblue', edgecolor='black', linewidth=0.5)
        
        ax2.set_xlabel('Decade')
        ax2.set_ylabel('Average Metacritic Score')
        ax2.set_title('Average Quality by Release Decade', fontweight='bold')
        ax2.set_xticks(x_pos)
        ax2.set_xticklabels([f"{int(decade)}s" for decade in decade_quality['decade']])
        
        # Add value labels
        for bar, score in zip(bars, decade_quality['metacritic_score_mean']):
            ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                    f'{score:.1f}', ha='center', va='bottom', fontsize=9)
        
        plt.tight_layout()
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / 'temporal_quality_evolution.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
        
        # Plotly version - Animated time evolution
        fig_plotly = go.Figure()
        
        fig_plotly.add_trace(go.Scatter(
            x=df['release_year_mean'],
            y=df['metacritic_score_mean'],
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
                'Avg Release Year: %{x:.1f}<br>' +
                'Avg Metacritic Score: %{y:.1f}<br>' +
                'Decade: %{customdata[0]}s<br>' +
                'Community Size: %{marker.size}<br>' +
                'Coverage: %{customdata[1]:.1f}%<br>' +
                '<extra></extra>',
            customdata=np.column_stack((
                df['decade'].astype(int),
                df['metacritic_score_coverage']
            )),
            name='Communities'
        ))
        
        # Add trend line
        if valid_mask.sum() > 1:
            fig_plotly.add_trace(go.Scatter(
                x=x_trend,
                y=p(x_trend),
                mode='lines',
                line=dict(color='red', dash='dash'),
                name='Quality Trend',
                showlegend=True
            ))
        
        fig_plotly.update_layout(
            **get_plotly_layout(
                title='Quality Evolution Over Time - Interactive Analysis<br><sub>Bubble size represents community size</sub>',
                xaxis_title='Average Release Year',
                yaxis_title='Average Metacritic Score',
                height=600
            )
        )
        
        figures['plotly'] = fig_plotly
        
        if save_plots:
            fig_plotly.write_html(self.output_dir / 'temporal_quality_interactive.html')
        
        return figures
    
    def generate_all_temporal_rating_plots(self, save_plots: bool = True) -> Dict:
        """
        Generate all temporal and rating analysis visualizations.
        
        Args:
            save_plots (bool): Whether to save all plots
            
        Returns:
            Dict: All generated figures organized by category
        """
        print("🎨 Generating temporal and rating analysis visualizations...")
        
        all_figures = {}
        
        # Generate each visualization
        print("  📅 Creating release timeline analysis...")
        all_figures['release_timeline'] = self.create_release_timeline_analysis(save_plots=save_plots)
        
        print("  ⭐ Creating Metacritic score analysis...")
        all_figures['metacritic_analysis'] = self.create_metacritic_score_analysis(save_plots=save_plots)
        
        print("  🔞 Creating age rating analysis...")
        all_figures['age_rating'] = self.create_age_rating_analysis(save_plots=save_plots)
        
        print("  📝 Creating review coverage analysis...")
        all_figures['review_coverage'] = self.create_review_coverage_analysis(save_plots=save_plots)
        
        print("  📈 Creating temporal quality evolution...")
        all_figures['quality_evolution'] = self.create_temporal_quality_evolution(save_plots=save_plots)
        
        print(f"✅ Temporal/rating analysis complete! Saved to: {self.output_dir}")
        return all_figures

# =============================================================================
# COMMAND LINE INTERFACE
# =============================================================================

def main():
    """Command line interface for temporal and rating analysis visualizations."""
    import argparse
    try:
        from .data_loader import load_data
    except ImportError:
        from data_loader import load_data
    
    parser = argparse.ArgumentParser(description='Generate temporal and rating analysis visualizations')
    parser.add_argument('--data-dir', type=str, default=None,
                       help='Directory containing community data files')
    parser.add_argument('--output-dir', type=str, 
                       default='communities_visualizations/outputs/static_plots/temporal_ratings',
                       help='Output directory for generated plots')
    parser.add_argument('--no-save', action='store_true',
                       help='Don\'t save plots to files (display only)')
    
    args = parser.parse_args()
    
    try:
        # Load data
        print("📥 Loading community data...")
        data_loader = load_data(args.data_dir)
        
        # Create analyzer
        analyzer = TemporalRatingAnalyzer(data_loader, args.output_dir)
        
        # Generate all plots
        figures = analyzer.generate_all_temporal_rating_plots(save_plots=not args.no_save)
        
        print("🎉 Temporal and rating analysis visualization generation complete!")
        
        # Show summary
        total_plots = sum(len(category_figs) for category_figs in figures.values())
        print(f"Generated {total_plots} visualizations across {len(figures)} categories")
        
    except Exception as e:
        print(f"❌ Error generating visualizations: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()