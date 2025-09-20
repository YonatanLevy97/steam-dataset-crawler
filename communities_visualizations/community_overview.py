"""
Community Overview Visualizations

Creates comprehensive overview visualizations showing community sizes, 
composition, platform support, and price distributions.
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

try:
    from .config import (
        COMMUNITY_COLORS, COMMUNITY_NAMES, FIGURE_SIZES, DPI_SETTINGS,
        get_community_color, get_plotly_layout, PLOTLY_COLORS
    )
    from .data_loader import CommunityDataLoader
except ImportError:
    from config import (
        COMMUNITY_COLORS, COMMUNITY_NAMES, FIGURE_SIZES, DPI_SETTINGS,
        get_community_color, get_plotly_layout, PLOTLY_COLORS
    )
    from data_loader import CommunityDataLoader

class CommunityOverviewVisualizer:
    """
    Creates overview visualizations for Steam game communities.
    """
    
    def __init__(self, data_loader: CommunityDataLoader, output_dir: str = None):
        """
        Initialize the visualizer.
        
        Args:
            data_loader (CommunityDataLoader): Loaded community data
            output_dir (str, optional): Output directory for plots
        """
        self.data_loader = data_loader
        self.output_dir = Path(output_dir) if output_dir else Path('outputs/static_plots/community_overview')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load data if not already loaded
        if data_loader.community_profiles is None:
            data_loader.load_community_profiles()
    
    def create_community_size_distribution(self, save_plots: bool = True) -> Dict:
        """
        Create community size distribution visualizations.
        
        Args:
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing matplotlib and plotly figures
        """
        df = self.data_loader.community_profiles.copy()
        df = df.sort_values('size', ascending=True)
        
        figures = {}
        
        # Matplotlib version
        fig_mpl, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=FIGURE_SIZES['large'])
        
        # Main horizontal bar chart
        colors = [get_community_color(i) for i in df['community_id']]
        bars = ax1.barh(df['community_name'], df['size'], color=colors, alpha=0.8, edgecolor='black', linewidth=0.5)
        ax1.set_xlabel('Number of Games', fontsize=12)
        ax1.set_title('Community Sizes - Games per Community', fontsize=14, fontweight='bold')
        ax1.grid(axis='x', alpha=0.3)
        
        # Add value labels on bars
        for i, (bar, size) in enumerate(zip(bars, df['size'])):
            ax1.text(bar.get_width() + 10, bar.get_y() + bar.get_height()/2, 
                    f'{size:,}', va='center', ha='left', fontsize=9)
        
        # Cumulative distribution
        df_sorted_desc = df.sort_values('size', ascending=False)
        cumulative_sizes = df_sorted_desc['size'].cumsum()
        cumulative_pct = (cumulative_sizes / df_sorted_desc['size'].sum()) * 100
        
        bars2 = ax2.bar(range(len(df_sorted_desc)), cumulative_pct, 
                       color=[get_community_color(i) for i in df_sorted_desc['community_id']], 
                       alpha=0.8, edgecolor='black', linewidth=0.5)
        ax2.set_xlabel('Community Rank (by size)')
        ax2.set_ylabel('Cumulative Percentage')
        ax2.set_title('Cumulative Community Size Distribution', fontweight='bold')
        ax2.set_xticks(range(len(df_sorted_desc)))
        ax2.set_xticklabels([f'{i+1}' for i in range(len(df_sorted_desc))])
        ax2.grid(axis='y', alpha=0.3)
        
        # Add percentage labels for top communities
        for i, (bar, pct) in enumerate(zip(bars2[:5], cumulative_pct[:5])):
            ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                    f'{pct:.1f}%', ha='center', va='bottom', fontsize=8)
        
        # Size categories distribution
        df['size_category'] = pd.cut(df['size'], 
                                    bins=[0, 300, 600, 900, 1200, float('inf')],
                                    labels=['Small\n(<300)', 'Medium\n(300-600)', 
                                           'Large\n(600-900)', 'Very Large\n(900-1200)', 
                                           'Massive\n(1200+)'])
        
        size_category_counts = df['size_category'].value_counts()
        bars3 = ax3.bar(size_category_counts.index, size_category_counts.values,
                       color='steelblue', alpha=0.8, edgecolor='black', linewidth=0.5)
        ax3.set_ylabel('Number of Communities')
        ax3.set_title('Communities by Size Category', fontweight='bold')
        ax3.grid(axis='y', alpha=0.3)
        
        # Add count labels
        for bar, count in zip(bars3, size_category_counts.values):
            ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                    f'{count}', ha='center', va='bottom', fontsize=10, fontweight='bold')
        
        # Size vs rank scatter with annotations
        df_sorted_desc_reset = df_sorted_desc.reset_index(drop=True)
        scatter = ax4.scatter(range(1, len(df_sorted_desc_reset) + 1), df_sorted_desc_reset['size'],
                             c=[get_community_color(i) for i in df_sorted_desc_reset['community_id']],
                             s=100, alpha=0.8, edgecolors='black', linewidth=1)
        
        ax4.set_xlabel('Community Rank')
        ax4.set_ylabel('Community Size')
        ax4.set_title('Size Distribution by Rank', fontweight='bold')
        ax4.grid(True, alpha=0.3)
        
        # Add annotations for top 3 and bottom 3
        for i in range(min(3, len(df_sorted_desc_reset))):
            ax4.annotate(df_sorted_desc_reset.iloc[i]['community_name'], 
                        (i+1, df_sorted_desc_reset.iloc[i]['size']),
                        xytext=(5, 5), textcoords='offset points', fontsize=8)
        
        for i in range(max(0, len(df_sorted_desc_reset)-3), len(df_sorted_desc_reset)):
            ax4.annotate(df_sorted_desc_reset.iloc[i]['community_name'], 
                        (i+1, df_sorted_desc_reset.iloc[i]['size']),
                        xytext=(5, -15), textcoords='offset points', fontsize=8)
        
        plt.tight_layout()
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / 'community_size_distribution.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
            fig_mpl.savefig(self.output_dir / 'community_size_distribution.pdf', 
                           bbox_inches='tight')
        
        # Plotly version
        fig_plotly = make_subplots(
            rows=1, cols=2,
            subplot_titles=('Community Sizes', 'Size Distribution'),
            specs=[[{"type": "bar"}, {"type": "pie"}]]
        )
        
        # Bar chart
        fig_plotly.add_trace(
            go.Bar(
                y=df['community_name'],
                x=df['size'],
                orientation='h',
                marker_color=[get_community_color(i) for i in df['community_id']],
                text=[f'{size:,}' for size in df['size']],
                textposition='outside',
                name='Games Count'
            ),
            row=1, col=1
        )
        
        # Pie chart
        fig_plotly.add_trace(
            go.Pie(
                labels=df['community_name'],
                values=df['size'],
                marker_colors=[get_community_color(i) for i in df['community_id']],
                textinfo='label+percent',
                textposition='inside',
                name='Distribution'
            ),
            row=1, col=2
        )
        
        fig_plotly.update_layout(
            **get_plotly_layout(
                title='Steam Game Communities - Size Analysis',
                height=600,
                showlegend=False
            )
        )
        
        fig_plotly.update_xaxes(title_text="Number of Games", row=1, col=1)
        
        figures['plotly'] = fig_plotly
        
        if save_plots:
            fig_plotly.write_html(self.output_dir / 'community_size_distribution.html')
            try:
                fig_plotly.write_image(self.output_dir / 'community_size_distribution_plotly.png')
            except Exception as e:
                print(f"  ⚠️ Could not save PNG (using HTML only): {e}")
        
        return figures
    
    def create_genre_composition_charts(self, save_plots: bool = True) -> Dict:
        """
        Create genre composition visualizations for each community.
        
        Args:
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing matplotlib and plotly figures
        """
        # Get top genres for each community from detailed profiles
        if self.data_loader.detailed_profiles is None:
            self.data_loader.load_detailed_profiles()
        
        figures = {}
        
        # Create comprehensive genre analysis with multiple visualizations
        fig_mpl, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=FIGURE_SIZES['large'])
        
        # Collect genre data for all communities
        genre_data = []
        all_communities = []
        
        for comm_id, comm_name in COMMUNITY_NAMES.items():
            if str(comm_id) not in self.data_loader.detailed_profiles['community_profiles']:
                continue
                
            profile = self.data_loader.detailed_profiles['community_profiles'][str(comm_id)]
            genres = profile['categorical_features']['genres']['top_values'][:5]  # Top 5 genres
            
            for genre in genres:
                genre_data.append({
                    'community': comm_name,
                    'community_id': comm_id,
                    'genre': genre['value'],
                    'percentage': genre['percentage'],
                    'count': genre['count']
                })
            all_communities.append({
                'community': comm_name,
                'community_id': comm_id,
                'size': profile['size'],
                'top_genre': genres[0]['value'] if genres else 'Unknown',
                'top_genre_pct': genres[0]['percentage'] if genres else 0
            })
        
        genre_df = pd.DataFrame(genre_data)
        communities_df = pd.DataFrame(all_communities)
        
        # 1. Top genres across all communities (stacked horizontal bar)
        top_genres = genre_df.groupby('genre')['count'].sum().sort_values(ascending=False).head(8)
        
        genre_community_matrix = genre_df[genre_df['genre'].isin(top_genres.index)].pivot_table(
            index='genre', columns='community', values='count', fill_value=0
        )
        
        # Create stacked horizontal bar chart
        bottom_values = np.zeros(len(genre_community_matrix))
        colors = [get_community_color(list(COMMUNITY_NAMES.keys())[list(COMMUNITY_NAMES.values()).index(comm)]) 
                 for comm in genre_community_matrix.columns if comm in COMMUNITY_NAMES.values()]
        
        for i, community in enumerate(genre_community_matrix.columns):
            if community in COMMUNITY_NAMES.values():
                comm_id = list(COMMUNITY_NAMES.keys())[list(COMMUNITY_NAMES.values()).index(community)]
                color = get_community_color(comm_id)
                ax1.barh(genre_community_matrix.index, genre_community_matrix[community], 
                        left=bottom_values, color=color, alpha=0.8, 
                        label=community, edgecolor='white', linewidth=0.5)
                bottom_values += genre_community_matrix[community].values
        
        ax1.set_xlabel('Number of Games')
        ax1.set_title('Top Genres Distribution Across Communities', fontweight='bold')
        ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
        
        # 2. Genre diversity by community (horizontal bar chart)
        genre_diversity = genre_df.groupby('community').agg({
            'genre': 'nunique',
            'percentage': lambda x: 100 - max(x)  # 100 - top genre percentage = diversity score
        }).rename(columns={'genre': 'unique_genres', 'percentage': 'diversity_score'})
        
        genre_diversity = genre_diversity.sort_values('diversity_score', ascending=True)
        colors_div = [get_community_color(list(COMMUNITY_NAMES.keys())[list(COMMUNITY_NAMES.values()).index(comm)]) 
                     for comm in genre_diversity.index if comm in COMMUNITY_NAMES.values()]
        
        bars2 = ax2.barh(genre_diversity.index, genre_diversity['diversity_score'], 
                        color=colors_div, alpha=0.8, edgecolor='black', linewidth=0.5)
        ax2.set_xlabel('Genre Diversity Score (100 - Top Genre %)')
        ax2.set_title('Genre Diversity by Community', fontweight='bold')
        ax2.grid(axis='x', alpha=0.3)
        
        # Add value labels
        for bar, score in zip(bars2, genre_diversity['diversity_score']):
            ax2.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2,
                    f'{score:.1f}', va='center', ha='left', fontsize=8)
        
        # 3. Top genre dominance by community size
        communities_df = communities_df.sort_values('size')
        colors_size = [get_community_color(comm_id) for comm_id in communities_df['community_id']]
        
        scatter = ax3.scatter(communities_df['size'], communities_df['top_genre_pct'], 
                             c=colors_size, s=80, alpha=0.8, edgecolors='black')
        
        ax3.set_xlabel('Community Size')
        ax3.set_ylabel('Top Genre Dominance (%)')
        ax3.set_title('Genre Specialization vs Community Size', fontweight='bold')
        ax3.grid(True, alpha=0.3)
        
        # Add trend line
        z = np.polyfit(communities_df['size'], communities_df['top_genre_pct'], 1)
        p = np.poly1d(z)
        ax3.plot(communities_df['size'], p(communities_df['size']), "r--", alpha=0.8, linewidth=2)
        
        # Add labels for interesting points
        for _, row in communities_df.iterrows():
            if row['top_genre_pct'] > 80 or row['size'] > 1000:
                ax3.annotate(row['community'], (row['size'], row['top_genre_pct']),
                           xytext=(5, 5), textcoords='offset points', fontsize=8, alpha=0.8)
        
        # 4. Genre distribution comparison (top genres only)
        genre_counts = genre_df['genre'].value_counts().head(10)
        bars4 = ax4.bar(range(len(genre_counts)), genre_counts.values, 
                       color='steelblue', alpha=0.8, edgecolor='black', linewidth=0.5)
        
        ax4.set_xlabel('Genre')
        ax4.set_ylabel('Total Appearances Across Communities')
        ax4.set_title('Most Popular Genres (Cross-Community)', fontweight='bold')
        ax4.set_xticks(range(len(genre_counts)))
        ax4.set_xticklabels(genre_counts.index, rotation=45, ha='right')
        ax4.grid(axis='y', alpha=0.3)
        
        # Add count labels
        for bar, count in zip(bars4, genre_counts.values):
            ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height() + count*0.01,
                    f'{count}', ha='center', va='bottom', fontsize=9)
        
        plt.tight_layout()
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / 'genre_composition_by_community.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
            fig_mpl.savefig(self.output_dir / 'genre_composition_by_community.pdf', 
                           bbox_inches='tight')
        
        # Plotly version - interactive sunburst chart
        sunburst_data = []
        for comm_id, comm_name in COMMUNITY_NAMES.items():
            if str(comm_id) not in self.data_loader.detailed_profiles['community_profiles']:
                continue
                
            profile = self.data_loader.detailed_profiles['community_profiles'][str(comm_id)]
            genres = profile['categorical_features']['genres']['top_values'][:5]
            
            for genre in genres:
                sunburst_data.append({
                    'community': comm_name,
                    'genre': genre['value'],
                    'percentage': genre['percentage'],
                    'count': genre['count']
                })
        
        sunburst_df = pd.DataFrame(sunburst_data)
        
        fig_plotly = go.Figure(go.Sunburst(
            labels=sunburst_df['community'].tolist() + sunburst_df['genre'].tolist(),
            parents=[''] * len(sunburst_df['community'].unique()) + sunburst_df['community'].tolist(),
            values=[100] * len(sunburst_df['community'].unique()) + sunburst_df['percentage'].tolist(),
            branchvalues="total",
            hovertemplate='<b>%{label}</b><br>Percentage: %{value:.1f}%<extra></extra>',
            maxdepth=2
        ))
        
        fig_plotly.update_layout(
            **get_plotly_layout(
                title='Interactive Genre Composition - Communities and Genres',
                height=800
            )
        )
        
        figures['plotly'] = fig_plotly
        
        if save_plots:
            fig_plotly.write_html(self.output_dir / 'genre_composition_sunburst.html')
        
        return figures
    
    def create_platform_support_matrix(self, save_plots: bool = True) -> Dict:
        """
        Create platform support analysis visualizations.
        
        Args:
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing matplotlib and plotly figures
        """
        df = self.data_loader.community_profiles.copy()
        
        platform_cols = ['windows_true_percentage', 'mac_true_percentage', 'linux_true_percentage']
        platform_names = ['Windows', 'Mac', 'Linux']
        
        figures = {}
        
        # Matplotlib version - Heatmap and stacked bar
        fig_mpl, (ax1, ax2) = plt.subplots(2, 1, figsize=FIGURE_SIZES['medium'])
        
        # Platform support heatmap
        platform_data = df[['community_name'] + platform_cols].set_index('community_name')
        platform_data.columns = platform_names
        
        sns.heatmap(platform_data, annot=True, fmt='.1f', cmap='YlOrRd', 
                   ax=ax1, cbar_kws={'label': 'Support Percentage'})
        ax1.set_title('Platform Support Matrix by Community', fontsize=14, fontweight='bold')
        ax1.set_xlabel('')
        ax1.set_ylabel('Community')
        
        # Stacked bar chart
        platform_data_sorted = platform_data.sort_values('Windows', ascending=True)
        platform_data_sorted.plot(kind='barh', stacked=False, ax=ax2, 
                                 color=['#1f77b4', '#ff7f0e', '#2ca02c'], alpha=0.8)
        ax2.set_title('Platform Support Comparison', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Support Percentage')
        ax2.legend(title='Platform', bbox_to_anchor=(1.05, 1), loc='upper left')
        ax2.grid(axis='x', alpha=0.3)
        
        plt.tight_layout()
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / 'platform_support_matrix.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
        
        # Plotly version - Interactive heatmap
        fig_plotly = go.Figure(data=go.Heatmap(
            z=platform_data.values,
            x=platform_names,
            y=platform_data.index,
            colorscale='Viridis',
            hoverongaps=False,
            hovertemplate='Community: %{y}<br>Platform: %{x}<br>Support: %{z:.1f}%<extra></extra>',
            text=platform_data.values.round(1),
            texttemplate='%{text}%',
            textfont={"size": 10}
        ))
        
        fig_plotly.update_layout(
            **get_plotly_layout(
                title='Platform Support Matrix - Interactive Heatmap',
                xaxis_title='Platform',
                yaxis_title='Community',
                height=600
            )
        )
        
        figures['plotly'] = fig_plotly
        
        if save_plots:
            fig_plotly.write_html(self.output_dir / 'platform_support_heatmap.html')
        
        return figures
    
    def create_price_distribution_analysis(self, save_plots: bool = True) -> Dict:
        """
        Create price distribution visualizations across communities.
        
        Args:
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing matplotlib and plotly figures
        """
        df = self.data_loader.community_profiles.copy()
        df = df.sort_values('median_price', ascending=True)
        
        figures = {}
        
        # Matplotlib version - Box plots and scatter
        fig_mpl, (ax1, ax2) = plt.subplots(2, 1, figsize=FIGURE_SIZES['large'])
        
        # Box plot simulation using average and median prices
        price_data = []
        colors = []
        positions = []
        
        for i, (_, row) in enumerate(df.iterrows()):
            # Create synthetic price distribution for visualization
            avg_price = row['average_price']
            med_price = row['median_price']
            
            # Simple approximation of price distribution
            if avg_price > med_price:  # Right-skewed
                prices = np.random.gamma(2, med_price/2, 100)
                prices = prices[prices <= avg_price * 2]  # Cap outliers
            else:  # More uniform
                prices = np.random.normal(med_price, med_price * 0.3, 100)
                prices = prices[prices >= 0]  # No negative prices
            
            price_data.append(prices)
            colors.append(get_community_color(row['community_id']))
            positions.append(i)
        
        # Box plot
        box_plot = ax1.boxplot(price_data, positions=positions, patch_artist=True, 
                              labels=df['community_name'])
        
        for patch, color in zip(box_plot['boxes'], colors):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)
        
        ax1.set_title('Price Distribution by Community (Estimated)', fontsize=14, fontweight='bold')
        ax1.set_ylabel('Price ($)')
        ax1.tick_params(axis='x', rotation=45)
        ax1.grid(axis='y', alpha=0.3)
        
        # Average vs Median price scatter
        colors_scatter = [get_community_color(i) for i in df['community_id']]
        scatter = ax2.scatter(df['median_price'], df['average_price'], 
                            c=colors_scatter, s=df['size']/5, alpha=0.7, edgecolors='black')
        
        # Add diagonal line (median = average)
        max_price = max(df['average_price'].max(), df['median_price'].max())
        ax2.plot([0, max_price], [0, max_price], 'k--', alpha=0.5, label='Median = Average')
        
        ax2.set_xlabel('Median Price ($)')
        ax2.set_ylabel('Average Price ($)')
        ax2.set_title('Price Relationship: Average vs Median (Bubble size = Community size)', 
                     fontsize=14, fontweight='bold')
        ax2.grid(True, alpha=0.3)
        ax2.legend()
        
        # Add community labels for interesting points
        for _, row in df.iterrows():
            if abs(row['average_price'] - row['median_price']) > 10:  # Significant difference
                ax2.annotate(row['community_name'], 
                           (row['median_price'], row['average_price']),
                           xytext=(5, 5), textcoords='offset points',
                           fontsize=8, alpha=0.8)
        
        plt.tight_layout()
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / 'price_distribution_analysis.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
        
        # Plotly version - Interactive scatter with hover info
        fig_plotly = go.Figure()
        
        fig_plotly.add_trace(go.Scatter(
            x=df['median_price'],
            y=df['average_price'],
            mode='markers',
            marker=dict(
                size=df['size']/10,
                color=[get_community_color(i) for i in df['community_id']],
                opacity=0.7,
                line=dict(width=1, color='black')
            ),
            text=df['community_name'],
            hovertemplate=
                '<b>%{text}</b><br>' +
                'Median Price: $%{x:.2f}<br>' +
                'Average Price: $%{y:.2f}<br>' +
                'Community Size: %{marker.size}<br>' +
                '<extra></extra>',
            name='Communities'
        ))
        
        # Add diagonal line
        max_price = max(df['average_price'].max(), df['median_price'].max())
        fig_plotly.add_trace(go.Scatter(
            x=[0, max_price],
            y=[0, max_price],
            mode='lines',
            line=dict(color='black', dash='dash'),
            name='Median = Average',
            showlegend=True
        ))
        
        fig_plotly.update_layout(
            **get_plotly_layout(
                title='Price Analysis: Average vs Median by Community<br><sub>Bubble size represents community size</sub>',
                xaxis_title='Median Price ($)',
                yaxis_title='Average Price ($)',
                height=600
            )
        )
        
        figures['plotly'] = fig_plotly
        
        if save_plots:
            fig_plotly.write_html(self.output_dir / 'price_analysis_interactive.html')
        
        return figures
    
    def create_community_summary_dashboard(self, save_plots: bool = True) -> Dict:
        """
        Create a comprehensive overview dashboard.
        
        Args:
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing dashboard figures
        """
        df = self.data_loader.community_profiles.copy()
        
        figures = {}
        
        # Create matplotlib dashboard
        fig_mpl = plt.figure(figsize=FIGURE_SIZES['dashboard'])
        gs = fig_mpl.add_gridspec(3, 3, hspace=0.3, wspace=0.3)
        
        # Community sizes (top-left)
        ax1 = fig_mpl.add_subplot(gs[0, 0])
        colors = [get_community_color(i) for i in df['community_id']]
        ax1.bar(range(len(df)), df['size'], color=colors, alpha=0.8)
        ax1.set_title('Community Sizes', fontweight='bold')
        ax1.set_xlabel('Community ID')
        ax1.set_ylabel('Games Count')
        
        # Price distribution (top-center)
        ax2 = fig_mpl.add_subplot(gs[0, 1])
        ax2.scatter(df['median_price'], df['average_price'], 
                   c=colors, s=50, alpha=0.7, edgecolors='black')
        ax2.set_title('Price: Avg vs Median', fontweight='bold')
        ax2.set_xlabel('Median Price ($)')
        ax2.set_ylabel('Average Price ($)')
        
        # Platform support (top-right)
        ax3 = fig_mpl.add_subplot(gs[0, 2])
        platform_means = df[['windows_true_percentage', 'mac_true_percentage', 
                           'linux_true_percentage']].mean()
        ax3.bar(['Windows', 'Mac', 'Linux'], platform_means, 
               color=['#1f77b4', '#ff7f0e', '#2ca02c'], alpha=0.8)
        ax3.set_title('Avg Platform Support', fontweight='bold')
        ax3.set_ylabel('Support Percentage')
        
        # Quality metrics (middle-left)
        ax4 = fig_mpl.add_subplot(gs[1, 0])
        ax4.scatter(df['metacritic_score_mean'], df['recommendations_total_mean'], 
                   c=colors, s=50, alpha=0.7, edgecolors='black')
        ax4.set_title('Quality Metrics', fontweight='bold')
        ax4.set_xlabel('Metacritic Score')
        ax4.set_ylabel('Recommendations')
        
        # Release timeline (middle-center)
        ax5 = fig_mpl.add_subplot(gs[1, 1])
        ax5.scatter(df['release_year_mean'], df['size'], 
                   c=colors, s=50, alpha=0.7, edgecolors='black')
        ax5.set_title('Release Timeline', fontweight='bold')
        ax5.set_xlabel('Average Release Year')
        ax5.set_ylabel('Community Size')
        
        # Content richness (middle-right)
        ax6 = fig_mpl.add_subplot(gs[1, 2])
        ax6.scatter(df['achievements_total_mean'], df['dlc_count_mean'], 
                   c=colors, s=50, alpha=0.7, edgecolors='black')
        ax6.set_title('Content Richness', fontweight='bold')
        ax6.set_xlabel('Avg Achievements')
        ax6.set_ylabel('Avg DLC Count')
        
        # Community summary table (bottom)
        ax7 = fig_mpl.add_subplot(gs[2, :])
        ax7.axis('tight')
        ax7.axis('off')
        
        # Create summary table
        summary_data = []
        for _, row in df.head(5).iterrows():  # Show top 5 communities
            summary_data.append([
                row['community_name'][:15],  # Truncate long names
                f"{row['size']:,}",
                f"${row['median_price']:.1f}",
                f"{row['metacritic_score_mean']:.1f}",
                f"{row['windows_true_percentage']:.0f}%"
            ])
        
        table = ax7.table(cellText=summary_data,
                         colLabels=['Community', 'Size', 'Price', 'Score', 'Win%'],
                         cellLoc='center',
                         loc='center')
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1.2, 1.5)
        ax7.set_title('Top Communities Summary', fontweight='bold', y=0.8)
        
        plt.suptitle('Steam Communities Overview Dashboard', 
                    fontsize=16, fontweight='bold', y=0.95)
        
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / 'community_overview_dashboard.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
            fig_mpl.savefig(self.output_dir / 'community_overview_dashboard.pdf', 
                           bbox_inches='tight')
        
        return figures
    
    def create_quality_vs_popularity_analysis(self, save_plots: bool = True) -> Dict:
        """
        Create quality vs popularity analysis visualizations.
        
        Args:
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing matplotlib and plotly figures
        """
        df = self.data_loader.community_profiles.copy()
        
        figures = {}
        
        # Matplotlib version
        fig_mpl, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=FIGURE_SIZES['large'])
        
        colors = [get_community_color(i) for i in df['community_id']]
        
        # 1. Quality vs Recommendations scatter
        scatter1 = ax1.scatter(df['metacritic_score_mean'], df['recommendations_total_mean'],
                              c=colors, s=df['size']/10, alpha=0.8, edgecolors='black')
        ax1.set_xlabel('Average Metacritic Score')
        ax1.set_ylabel('Average Recommendations')
        ax1.set_title('Quality vs Popularity\n(Bubble size = Community size)', fontweight='bold')
        ax1.grid(True, alpha=0.3)
        
        # Add community labels for interesting points
        for _, row in df.iterrows():
            if row['metacritic_score_mean'] > 80 or row['recommendations_total_mean'] > 400:
                ax1.annotate(row['community_name'], 
                           (row['metacritic_score_mean'], row['recommendations_total_mean']),
                           xytext=(5, 5), textcoords='offset points', fontsize=8, alpha=0.8)
        
        # 2. Price vs Quality relationship
        scatter2 = ax2.scatter(df['average_price'], df['metacritic_score_mean'],
                              c=colors, s=80, alpha=0.8, edgecolors='black')
        ax2.set_xlabel('Average Price ($)')
        ax2.set_ylabel('Average Metacritic Score')
        ax2.set_title('Price vs Quality Relationship', fontweight='bold')
        ax2.grid(True, alpha=0.3)
        
        # Add trend line
        mask = ~(np.isnan(df['average_price']) | np.isnan(df['metacritic_score_mean']))
        if mask.sum() > 1:
            z = np.polyfit(df.loc[mask, 'average_price'], df.loc[mask, 'metacritic_score_mean'], 1)
            p = np.poly1d(z)
            x_range = np.linspace(df['average_price'].min(), df['average_price'].max(), 100)
            ax2.plot(x_range, p(x_range), "r--", alpha=0.8, linewidth=2)
        
        # 3. Quality distribution by community
        df_quality_sorted = df.sort_values('metacritic_score_mean', ascending=True)
        bars3 = ax3.barh(df_quality_sorted['community_name'], 
                        df_quality_sorted['metacritic_score_mean'],
                        color=[get_community_color(i) for i in df_quality_sorted['community_id']],
                        alpha=0.8, edgecolor='black', linewidth=0.5)
        
        ax3.set_xlabel('Average Metacritic Score')
        ax3.set_title('Quality Rankings by Community', fontweight='bold')
        ax3.axvline(x=75, color='green', linestyle='--', alpha=0.7, label='Good Threshold (75)')
        ax3.legend()
        ax3.grid(axis='x', alpha=0.3)
        
        # Add score labels
        for bar, score in zip(bars3, df_quality_sorted['metacritic_score_mean']):
            ax3.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2,
                    f'{score:.1f}', va='center', ha='left', fontsize=8)
        
        # 4. Quality vs Coverage analysis
        scatter4 = ax4.scatter(df['metacritic_score_coverage'], df['metacritic_score_mean'],
                              c=colors, s=df['size']/15, alpha=0.8, edgecolors='black')
        ax4.set_xlabel('Review Coverage (%)')
        ax4.set_ylabel('Average Metacritic Score')
        ax4.set_title('Review Coverage vs Quality\n(Bubble size = Community size)', fontweight='bold')
        ax4.grid(True, alpha=0.3)
        
        plt.tight_layout()
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / 'quality_popularity_analysis.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
            fig_mpl.savefig(self.output_dir / 'quality_popularity_analysis.pdf', 
                           bbox_inches='tight')
        
        return figures
    
    def create_content_monetization_analysis(self, save_plots: bool = True) -> Dict:
        """
        Create content monetization analysis visualizations.
        
        Args:
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing matplotlib and plotly figures
        """
        df = self.data_loader.community_profiles.copy()
        
        figures = {}
        
        # Matplotlib version
        fig_mpl, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=FIGURE_SIZES['large'])
        
        colors = [get_community_color(i) for i in df['community_id']]
        
        # 1. Free games percentage by community
        df_free_sorted = df.sort_values('is_free_true_percentage', ascending=True)
        bars1 = ax1.barh(df_free_sorted['community_name'], 
                        df_free_sorted['is_free_true_percentage'],
                        color=[get_community_color(i) for i in df_free_sorted['community_id']],
                        alpha=0.8, edgecolor='black', linewidth=0.5)
        
        ax1.set_xlabel('Free Games Percentage')
        ax1.set_title('Free-to-Play Adoption by Community', fontweight='bold')
        ax1.grid(axis='x', alpha=0.3)
        
        # Add percentage labels
        for bar, pct in zip(bars1, df_free_sorted['is_free_true_percentage']):
            ax1.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2,
                    f'{pct:.1f}%', va='center', ha='left', fontsize=8)
        
        # 2. DLC adoption vs community size
        scatter2 = ax2.scatter(df['size'], df['has_dlc_true_percentage'],
                              c=colors, s=80, alpha=0.8, edgecolors='black')
        ax2.set_xlabel('Community Size')
        ax2.set_ylabel('Games with DLC (%)')
        ax2.set_title('DLC Adoption vs Community Size', fontweight='bold')
        ax2.grid(True, alpha=0.3)
        
        # Add trend line
        z = np.polyfit(df['size'], df['has_dlc_true_percentage'], 1)
        p = np.poly1d(z)
        ax2.plot(df['size'], p(df['size']), "r--", alpha=0.8, linewidth=2)
        
        # 3. Price vs DLC relationship
        scatter3 = ax3.scatter(df['average_price'], df['has_dlc_true_percentage'],
                              c=colors, s=df['size']/15, alpha=0.8, edgecolors='black')
        ax3.set_xlabel('Average Price ($)')
        ax3.set_ylabel('Games with DLC (%)')
        ax3.set_title('Pricing vs DLC Strategy\n(Bubble size = Community size)', fontweight='bold')
        ax3.grid(True, alpha=0.3)
        
        # 4. Content richness comparison
        df['content_score'] = (
            (df['achievements_total_mean'] / df['achievements_total_mean'].max() * 50) +
            (df['dlc_count_mean'] / df['dlc_count_mean'].max() * 50)
        )
        
        df_content_sorted = df.sort_values('content_score', ascending=True)
        bars4 = ax4.barh(df_content_sorted['community_name'], 
                        df_content_sorted['content_score'],
                        color=[get_community_color(i) for i in df_content_sorted['community_id']],
                        alpha=0.8, edgecolor='black', linewidth=0.5)
        
        ax4.set_xlabel('Content Richness Score (0-100)')
        ax4.set_title('Content Richness by Community\n(Achievements + DLC)', fontweight='bold')
        ax4.grid(axis='x', alpha=0.3)
        
        # Add score labels
        for bar, score in zip(bars4, df_content_sorted['content_score']):
            ax4.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2,
                    f'{score:.0f}', va='center', ha='left', fontsize=8)
        
        plt.tight_layout()
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / 'content_monetization_analysis.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
            fig_mpl.savefig(self.output_dir / 'content_monetization_analysis.pdf', 
                           bbox_inches='tight')
        
        return figures
    
    def create_market_positioning_analysis(self, save_plots: bool = True) -> Dict:
        """
        Create market positioning analysis visualizations.
        
        Args:
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing matplotlib and plotly figures
        """
        df = self.data_loader.community_profiles.copy()
        
        # Calculate market positioning metrics
        df['value_score'] = df['metacritic_score_mean'] / (df['average_price'] + 1)  # Quality per dollar
        df['market_reach'] = df['size'] * (df['windows_true_percentage'] / 100)
        df['premium_indicator'] = df['average_price'] > df['average_price'].median()
        
        figures = {}
        
        # Matplotlib version
        fig_mpl, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=FIGURE_SIZES['large'])
        
        colors = [get_community_color(i) for i in df['community_id']]
        
        # 1. Market positioning quadrants (Price vs Quality)
        ax1.scatter(df['average_price'], df['metacritic_score_mean'],
                   c=colors, s=df['size']/10, alpha=0.8, edgecolors='black')
        
        # Add quadrant lines
        price_median = df['average_price'].median()
        quality_median = df['metacritic_score_mean'].median()
        
        ax1.axvline(x=price_median, color='gray', linestyle='--', alpha=0.5)
        ax1.axhline(y=quality_median, color='gray', linestyle='--', alpha=0.5)
        
        ax1.set_xlabel('Average Price ($)')
        ax1.set_ylabel('Average Metacritic Score')
        ax1.set_title('Market Positioning Quadrants\n(Bubble size = Community size)', fontweight='bold')
        ax1.grid(True, alpha=0.3)
        
        # Add quadrant labels
        ax1.text(ax1.get_xlim()[0]*1.1, ax1.get_ylim()[1]*0.95, 'Budget\nHigh Quality', 
                ha='left', va='top', fontweight='bold', color='green')
        ax1.text(ax1.get_xlim()[1]*0.9, ax1.get_ylim()[1]*0.95, 'Premium\nHigh Quality', 
                ha='right', va='top', fontweight='bold', color='blue')
        ax1.text(ax1.get_xlim()[0]*1.1, ax1.get_ylim()[0]*1.1, 'Budget\nLow Quality', 
                ha='left', va='bottom', fontweight='bold', color='red')
        ax1.text(ax1.get_xlim()[1]*0.9, ax1.get_ylim()[0]*1.1, 'Premium\nLow Quality', 
                ha='right', va='bottom', fontweight='bold', color='orange')
        
        # 2. Value proposition (Quality per dollar)
        df_value_sorted = df.sort_values('value_score', ascending=True)
        bars2 = ax2.barh(df_value_sorted['community_name'], 
                        df_value_sorted['value_score'],
                        color=[get_community_color(i) for i in df_value_sorted['community_id']],
                        alpha=0.8, edgecolor='black', linewidth=0.5)
        
        ax2.set_xlabel('Value Score (Quality / Price)')
        ax2.set_title('Value Proposition by Community', fontweight='bold')
        ax2.grid(axis='x', alpha=0.3)
        
        # Add value labels
        for bar, score in zip(bars2, df_value_sorted['value_score']):
            ax2.text(bar.get_width() + score*0.05, bar.get_y() + bar.get_height()/2,
                    f'{score:.1f}', va='center', ha='left', fontsize=8)
        
        # 3. Market reach analysis
        scatter3 = ax3.scatter(df['market_reach'], df['average_price'],
                              c=colors, s=80, alpha=0.8, edgecolors='black')
        ax3.set_xlabel('Market Reach (Size × Windows Support)')
        ax3.set_ylabel('Average Price ($)')
        ax3.set_title('Market Reach vs Pricing Strategy', fontweight='bold')
        ax3.grid(True, alpha=0.3)
        
        # Add community labels for outliers
        for _, row in df.iterrows():
            if row['market_reach'] > 800 or row['average_price'] > 100:
                ax3.annotate(row['community_name'], 
                           (row['market_reach'], row['average_price']),
                           xytext=(5, 5), textcoords='offset points', fontsize=8, alpha=0.8)
        
        # 4. Platform strategy comparison
        platform_score = (df['windows_true_percentage'] + 
                         df['mac_true_percentage'] + 
                         df['linux_true_percentage']) / 3
        
        scatter4 = ax4.scatter(platform_score, df['size'],
                              c=colors, s=df['average_price']*2, alpha=0.8, edgecolors='black')
        ax4.set_xlabel('Average Platform Support (%)')
        ax4.set_ylabel('Community Size')
        ax4.set_title('Platform Strategy vs Size\n(Bubble size = Average price)', fontweight='bold')
        ax4.grid(True, alpha=0.3)
        
        plt.tight_layout()
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / 'market_positioning_analysis.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
            fig_mpl.savefig(self.output_dir / 'market_positioning_analysis.pdf', 
                           bbox_inches='tight')
        
        return figures
    
    def generate_all_overview_plots(self, save_plots: bool = True) -> Dict:
        """
        Generate all overview visualizations.
        
        Args:
            save_plots (bool): Whether to save all plots
            
        Returns:
            Dict: All generated figures organized by category
        """
        print("🎨 Generating comprehensive community overview visualizations...")
        
        all_figures = {}
        
        # Generate each visualization
        print("  📊 Creating enhanced size distribution plots...")
        all_figures['size_distribution'] = self.create_community_size_distribution(save_plots)
        
        print("  🎯 Creating improved genre composition charts...")
        all_figures['genre_composition'] = self.create_genre_composition_charts(save_plots)
        
        print("  💻 Creating platform support matrix...")
        all_figures['platform_support'] = self.create_platform_support_matrix(save_plots)
        
        print("  💰 Creating price distribution analysis...")
        all_figures['price_analysis'] = self.create_price_distribution_analysis(save_plots)
        
        print("  ⭐ Creating quality vs popularity analysis...")
        all_figures['quality_popularity'] = self.create_quality_vs_popularity_analysis(save_plots)
        
        print("  💳 Creating content monetization analysis...")
        all_figures['monetization'] = self.create_content_monetization_analysis(save_plots)
        
        print("  🎯 Creating market positioning analysis...")
        all_figures['market_positioning'] = self.create_market_positioning_analysis(save_plots)
        
        print("  📋 Creating summary dashboard...")
        all_figures['summary_dashboard'] = self.create_community_summary_dashboard(save_plots)
        
        print(f"✅ Enhanced overview visualizations complete! Saved to: {self.output_dir}")
        return all_figures

# =============================================================================
# COMMAND LINE INTERFACE
# =============================================================================

def main():
    """Command line interface for community overview visualizations."""
    import argparse
    try:
        from .data_loader import load_data
    except ImportError:
        from data_loader import load_data
    
    parser = argparse.ArgumentParser(description='Generate community overview visualizations')
    parser.add_argument('--data-dir', type=str, default=None,
                       help='Directory containing community data files')
    parser.add_argument('--output-dir', type=str, 
                       default='communities_visualizations/outputs/static_plots/community_overview',
                       help='Output directory for generated plots')
    parser.add_argument('--no-save', action='store_true',
                       help='Don\'t save plots to files (display only)')
    
    args = parser.parse_args()
    
    try:
        # Load data
        print("📥 Loading community data...")
        data_loader = load_data(args.data_dir)
        
        # Create visualizer
        visualizer = CommunityOverviewVisualizer(data_loader, args.output_dir)
        
        # Generate all plots
        figures = visualizer.generate_all_overview_plots(save_plots=not args.no_save)
        
        print("🎉 Community overview visualization generation complete!")
        
        # Show summary
        total_plots = sum(len(category_figs) for category_figs in figures.values())
        print(f"Generated {total_plots} visualizations across {len(figures)} categories")
        
    except Exception as e:
        print(f"❌ Error generating visualizations: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()