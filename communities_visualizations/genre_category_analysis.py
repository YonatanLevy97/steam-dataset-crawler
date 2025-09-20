"""
Genre and Category Analysis Visualizations

Creates comprehensive visualizations analyzing genre distributions, diversity,
and category patterns across Steam game communities.
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
from scipy.cluster.hierarchy import dendrogram, linkage
from sklearn.cluster import KMeans
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

class GenreCategoryAnalyzer:
    """
    Creates genre and category analysis visualizations for Steam game communities.
    """
    
    def __init__(self, data_loader: CommunityDataLoader, output_dir: str = None):
        """
        Initialize the analyzer.
        
        Args:
            data_loader (CommunityDataLoader): Loaded community data
            output_dir (str, optional): Output directory for plots
        """
        self.data_loader = data_loader
        self.output_dir = Path(output_dir) if output_dir else Path('outputs/static_plots/genres_categories')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load required data
        if data_loader.community_profiles is None:
            data_loader.load_community_profiles()
        if data_loader.detailed_profiles is None:
            data_loader.load_detailed_profiles()
    
    def create_genre_distribution_heatmap(self, top_n: int = 12, save_plots: bool = True) -> Dict:
        """
        Create genre distribution heatmap across communities.
        
        Args:
            top_n (int): Number of top genres to include
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing matplotlib and plotly figures
        """
        # Get genre matrix
        genre_matrix = self.data_loader.get_genre_matrix(top_n)
        
        figures = {}
        
        # Matplotlib version
        fig_mpl, (ax1, ax2) = plt.subplots(2, 1, figsize=FIGURE_SIZES['large'])
        
        # Main heatmap
        heatmap_params = HEATMAP_PARAMS.copy()
        heatmap_params['fmt'] = '.1f'
        sns.heatmap(genre_matrix, ax=ax1, **heatmap_params)
        ax1.set_title('Genre Distribution Across Communities', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Genre')
        ax1.set_ylabel('Community')
        
        # Clustered heatmap
        genre_matrix_numeric = genre_matrix.select_dtypes(include=[np.number])
        
        # Perform hierarchical clustering on communities
        linkage_matrix = linkage(genre_matrix_numeric, method='ward')
        cluster_order = dendrogram(linkage_matrix, no_plot=True)['leaves']
        
        # Reorder matrix based on clustering
        clustered_matrix = genre_matrix_numeric.iloc[cluster_order]
        
        heatmap_params2 = HEATMAP_PARAMS.copy()
        heatmap_params2['cbar_kws'] = {'label': 'Percentage'}
        heatmap_params2['fmt'] = '.1f'
        sns.heatmap(clustered_matrix, ax=ax2, **heatmap_params2)
        ax2.set_title('Clustered Genre Distribution (Hierarchical Clustering)', 
                     fontsize=14, fontweight='bold')
        ax2.set_xlabel('Genre')
        ax2.set_ylabel('Community (Clustered)')
        
        plt.tight_layout()
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / 'genre_distribution_heatmap.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
            fig_mpl.savefig(self.output_dir / 'genre_distribution_heatmap.pdf', 
                           bbox_inches='tight')
        
        # Plotly version - Interactive heatmap
        fig_plotly = go.Figure(data=go.Heatmap(
            z=genre_matrix_numeric.values,
            x=genre_matrix_numeric.columns,
            y=genre_matrix_numeric.index,
            colorscale='Viridis',
            hoverongaps=False,
            hovertemplate='Community: %{y}<br>Genre: %{x}<br>Percentage: %{z:.1f}%<extra></extra>',
            text=genre_matrix_numeric.values.round(1),
            texttemplate='%{text}%',
            textfont={"size": 9}
        ))
        
        fig_plotly.update_layout(
            **get_plotly_layout(
                title='Interactive Genre Distribution Heatmap',
                xaxis_title='Genre',
                yaxis_title='Community',
                height=700
            )
        )
        
        figures['plotly'] = fig_plotly
        
        if save_plots:
            fig_plotly.write_html(self.output_dir / 'genre_distribution_interactive.html')
        
        return figures
    
    def create_genre_diversity_analysis(self, save_plots: bool = True) -> Dict:
        """
        Create genre diversity analysis visualizations.
        
        Args:
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing matplotlib and plotly figures
        """
        # Calculate diversity metrics for each community
        diversity_data = []
        
        for comm_id, comm_name in COMMUNITY_NAMES.items():
            if str(comm_id) not in self.data_loader.detailed_profiles['community_profiles']:
                continue
            
            profile = self.data_loader.detailed_profiles['community_profiles'][str(comm_id)]
            genres = profile['categorical_features']['genres']['top_values']
            
            # Calculate Shannon diversity index
            percentages = np.array([g['percentage'] for g in genres]) / 100.0
            shannon_diversity = -np.sum(percentages * np.log(percentages + 1e-10))
            
            # Calculate Simpson diversity index  
            simpson_diversity = 1 - np.sum(percentages ** 2)
            
            # Calculate genre count and dominant genre percentage
            genre_count = profile['categorical_features']['genres']['unique_count']
            dominant_percentage = genres[0]['percentage']
            
            diversity_data.append({
                'community_id': comm_id,
                'community_name': comm_name,
                'shannon_diversity': shannon_diversity,
                'simpson_diversity': simpson_diversity,
                'genre_count': genre_count,
                'dominant_percentage': dominant_percentage,
                'size': profile['size']
            })
        
        diversity_df = pd.DataFrame(diversity_data)
        
        figures = {}
        
        # Matplotlib version
        fig_mpl, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=FIGURE_SIZES['large'])
        
        colors = [get_community_color(i) for i in diversity_df['community_id']]
        
        # Shannon diversity vs community size
        scatter1 = ax1.scatter(diversity_df['size'], diversity_df['shannon_diversity'], 
                              c=colors, s=80, alpha=0.7, edgecolors='black')
        ax1.set_xlabel('Community Size')
        ax1.set_ylabel('Shannon Diversity Index')
        ax1.set_title('Genre Diversity vs Community Size', fontweight='bold')
        ax1.grid(True, alpha=0.3)
        
        # Add labels for extreme points
        for _, row in diversity_df.iterrows():
            if row['shannon_diversity'] > diversity_df['shannon_diversity'].quantile(0.8) or \
               row['shannon_diversity'] < diversity_df['shannon_diversity'].quantile(0.2):
                ax1.annotate(row['community_name'], 
                           (row['size'], row['shannon_diversity']),
                           xytext=(5, 5), textcoords='offset points',
                           fontsize=8, alpha=0.8)
        
        # Dominant genre percentage vs diversity
        ax2.scatter(diversity_df['dominant_percentage'], diversity_df['simpson_diversity'], 
                   c=colors, s=80, alpha=0.7, edgecolors='black')
        ax2.set_xlabel('Dominant Genre Percentage')
        ax2.set_ylabel('Simpson Diversity Index')
        ax2.set_title('Dominance vs Diversity', fontweight='bold')
        ax2.grid(True, alpha=0.3)
        
        # Genre count distribution
        bars = ax3.bar(diversity_df['community_name'], diversity_df['genre_count'], 
                      color=colors, alpha=0.8, edgecolor='black', linewidth=0.5)
        ax3.set_xlabel('Community')
        ax3.set_ylabel('Number of Unique Genres')
        ax3.set_title('Genre Count by Community', fontweight='bold')
        ax3.tick_params(axis='x', rotation=45)
        
        # Add value labels on bars
        for bar, count in zip(bars, diversity_df['genre_count']):
            ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                    f'{count}', ha='center', va='bottom', fontsize=9)
        
        # Diversity comparison radar-like plot (as scatter)
        ax4.scatter(diversity_df['shannon_diversity'], diversity_df['simpson_diversity'], 
                   c=colors, s=diversity_df['size']/10, alpha=0.7, edgecolors='black')
        ax4.set_xlabel('Shannon Diversity')
        ax4.set_ylabel('Simpson Diversity')
        ax4.set_title('Diversity Comparison\n(Bubble size = Community size)', fontweight='bold')
        ax4.grid(True, alpha=0.3)
        
        plt.tight_layout()
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / 'genre_diversity_analysis.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
        
        # Plotly version - Interactive bubble chart
        fig_plotly = go.Figure()
        
        fig_plotly.add_trace(go.Scatter(
            x=diversity_df['shannon_diversity'],
            y=diversity_df['simpson_diversity'],
            mode='markers',
            marker=dict(
                size=diversity_df['size']/15,
                color=[get_community_color(i) for i in diversity_df['community_id']],
                opacity=0.7,
                line=dict(width=1, color='black'),
                sizemode='diameter'
            ),
            text=diversity_df['community_name'],
            hovertemplate=
                '<b>%{text}</b><br>' +
                'Shannon Diversity: %{x:.3f}<br>' +
                'Simpson Diversity: %{y:.3f}<br>' +
                'Community Size: %{marker.size}<br>' +
                'Genre Count: %{customdata[0]}<br>' +
                'Dominant Genre %: %{customdata[1]:.1f}%<br>' +
                '<extra></extra>',
            customdata=np.column_stack((diversity_df['genre_count'], 
                                       diversity_df['dominant_percentage'])),
            name='Communities'
        ))
        
        fig_plotly.update_layout(
            **get_plotly_layout(
                title='Genre Diversity Analysis - Interactive Comparison<br><sub>Bubble size represents community size</sub>',
                xaxis_title='Shannon Diversity Index',
                yaxis_title='Simpson Diversity Index',
                height=600
            )
        )
        
        figures['plotly'] = fig_plotly
        
        if save_plots:
            fig_plotly.write_html(self.output_dir / 'genre_diversity_interactive.html')
        
        return figures
    
    def create_top_genres_comparison(self, top_n: int = 5, save_plots: bool = True) -> Dict:
        """
        Create top genres comparison across communities.
        
        Args:
            top_n (int): Number of top genres to show per community
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing matplotlib and plotly figures
        """
        # Extract top genres for each community
        community_genres = {}
        
        for comm_id, comm_name in COMMUNITY_NAMES.items():
            if str(comm_id) not in self.data_loader.detailed_profiles['community_profiles']:
                continue
            
            profile = self.data_loader.detailed_profiles['community_profiles'][str(comm_id)]
            genres = profile['categorical_features']['genres']['top_values'][:top_n]
            community_genres[comm_name] = [(g['value'], g['percentage']) for g in genres]
        
        figures = {}
        
        # Matplotlib version - Horizontal bar charts grid
        n_communities = len(community_genres)
        n_cols = 3
        n_rows = (n_communities + n_cols - 1) // n_cols
        
        fig_mpl, axes = plt.subplots(n_rows, n_cols, figsize=FIGURE_SIZES['dashboard'])
        axes = axes.flatten()
        
        for i, (comm_name, genres) in enumerate(community_genres.items()):
            if i >= len(axes):
                break
            
            genre_names = [g[0] for g in genres]
            genre_percentages = [g[1] for g in genres]
            
            # Create color gradient for this community
            base_color = get_community_color(list(COMMUNITY_NAMES.values()).index(comm_name))
            colors = plt.cm.Blues(np.linspace(0.4, 0.9, len(genre_names)))
            
            bars = axes[i].barh(genre_names, genre_percentages, color=colors, 
                               alpha=0.8, edgecolor='black', linewidth=0.5)
            axes[i].set_title(comm_name, fontweight='bold', fontsize=11)
            axes[i].set_xlabel('Percentage')
            
            # Add percentage labels
            for bar, pct in zip(bars, genre_percentages):
                axes[i].text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2,
                           f'{pct:.1f}%', va='center', ha='left', fontsize=8)
        
        # Hide unused subplots
        for i in range(len(community_genres), len(axes)):
            axes[i].axis('off')
        
        plt.suptitle(f'Top {top_n} Genres by Community', fontsize=16, fontweight='bold', y=0.98)
        plt.tight_layout()
        
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / f'top_{top_n}_genres_comparison.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
        
        # Plotly version - Stacked bar chart
        genres_all = set()
        for genres in community_genres.values():
            genres_all.update([g[0] for g in genres])
        
        # Create data matrix
        data_matrix = []
        communities = list(community_genres.keys())
        
        for genre in sorted(genres_all):
            genre_data = []
            for comm_name in communities:
                genres = dict(community_genres[comm_name])
                genre_data.append(genres.get(genre, 0))
            data_matrix.append(genre_data)
        
        fig_plotly = go.Figure()
        
        colors_plotly = px.colors.qualitative.Set3[:len(genres_all)]
        
        for i, (genre, data) in enumerate(zip(sorted(genres_all), data_matrix)):
            fig_plotly.add_trace(go.Bar(
                name=genre,
                x=communities,
                y=data,
                marker_color=colors_plotly[i % len(colors_plotly)],
                hovertemplate='Community: %{x}<br>Genre: %{fullData.name}<br>Percentage: %{y:.1f}%<extra></extra>'
            ))
        
        fig_plotly.update_layout(
            **get_plotly_layout(
                title=f'Top {top_n} Genres Distribution Across Communities',
                xaxis_title='Community',
                yaxis_title='Percentage',
                barmode='group',
                height=600
            )
        )
        
        figures['plotly'] = fig_plotly
        
        if save_plots:
            fig_plotly.write_html(self.output_dir / f'top_{top_n}_genres_interactive.html')
        
        return figures
    
    def create_category_clustering_analysis(self, save_plots: bool = True) -> Dict:
        """
        Create category clustering visualization using genre similarity.
        
        Args:
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing matplotlib and plotly figures
        """
        # Get genre matrix for clustering
        genre_matrix = self.data_loader.get_genre_matrix(top_n=10)
        genre_matrix_numeric = genre_matrix.select_dtypes(include=[np.number])
        
        figures = {}
        
        # Matplotlib version - Dendrogram and clustered heatmap
        fig_mpl, (ax1, ax2) = plt.subplots(1, 2, figsize=FIGURE_SIZES['wide'])
        
        # Hierarchical clustering dendrogram
        linkage_matrix = linkage(genre_matrix_numeric, method='ward')
        dendro = dendrogram(linkage_matrix, ax=ax1, labels=genre_matrix_numeric.index, 
                           orientation='left', leaf_font_size=10)
        ax1.set_title('Community Clustering by Genre Similarity', fontweight='bold')
        ax1.set_xlabel('Distance')
        
        # K-means clustering visualization
        n_clusters = 4
        kmeans = KMeans(n_clusters=n_clusters, random_state=42)
        cluster_labels = kmeans.fit_predict(genre_matrix_numeric)
        
        # Create cluster colors
        cluster_colors = plt.cm.Set1(np.linspace(0, 1, n_clusters))
        community_colors = [cluster_colors[label] for label in cluster_labels]
        
        # PCA for 2D visualization
        from sklearn.decomposition import PCA
        pca = PCA(n_components=2)
        genre_2d = pca.fit_transform(genre_matrix_numeric)
        
        scatter = ax2.scatter(genre_2d[:, 0], genre_2d[:, 1], 
                             c=community_colors, s=100, alpha=0.7, edgecolors='black')
        ax2.set_xlabel(f'PC1 ({pca.explained_variance_ratio_[0]:.1%} variance)')
        ax2.set_ylabel(f'PC2 ({pca.explained_variance_ratio_[1]:.1%} variance)')
        ax2.set_title(f'Communities in Genre Space (K-means, k={n_clusters})', fontweight='bold')
        
        # Add community labels
        for i, comm_name in enumerate(genre_matrix_numeric.index):
            ax2.annotate(comm_name, (genre_2d[i, 0], genre_2d[i, 1]),
                        xytext=(5, 5), textcoords='offset points',
                        fontsize=8, alpha=0.8)
        
        plt.tight_layout()
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / 'category_clustering_analysis.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
        
        # Plotly version - Interactive PCA plot with clustering
        cluster_names = [f'Cluster {i+1}' for i in range(n_clusters)]
        
        fig_plotly = go.Figure()
        
        for cluster_id in range(n_clusters):
            cluster_mask = cluster_labels == cluster_id
            cluster_communities = genre_matrix_numeric.index[cluster_mask]
            cluster_coords = genre_2d[cluster_mask]
            
            fig_plotly.add_trace(go.Scatter(
                x=cluster_coords[:, 0],
                y=cluster_coords[:, 1],
                mode='markers+text',
                marker=dict(
                    size=12,
                    color=cluster_colors[cluster_id],
                    opacity=0.7,
                    line=dict(width=1, color='black')
                ),
                text=cluster_communities,
                textposition='top center',
                name=f'Cluster {cluster_id + 1}',
                hovertemplate='Community: %{text}<br>PC1: %{x:.3f}<br>PC2: %{y:.3f}<extra></extra>'
            ))
        
        fig_plotly.update_layout(
            **get_plotly_layout(
                title=f'Communities Clustered by Genre Patterns<br><sub>PCA projection, K-means clustering (k={n_clusters})</sub>',
                xaxis_title=f'PC1 ({pca.explained_variance_ratio_[0]:.1%} variance)',
                yaxis_title=f'PC2 ({pca.explained_variance_ratio_[1]:.1%} variance)',
                height=600
            )
        )
        
        figures['plotly'] = fig_plotly
        
        if save_plots:
            fig_plotly.write_html(self.output_dir / 'category_clustering_interactive.html')
        
        return figures
    
    def create_genre_evolution_timeline(self, save_plots: bool = True) -> Dict:
        """
        Create genre evolution timeline based on release years.
        
        Args:
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing matplotlib and plotly figures
        """
        # Get community temporal data
        temporal_data = self.data_loader.get_temporal_data()
        
        # Get top genres for each community
        community_genre_timeline = []
        
        for _, row in temporal_data.iterrows():
            comm_id = row['community_id']
            if str(comm_id) not in self.data_loader.detailed_profiles['community_profiles']:
                continue
            
            profile = self.data_loader.detailed_profiles['community_profiles'][str(comm_id)]
            top_genre = profile['categorical_features']['genres']['top_values'][0]
            
            community_genre_timeline.append({
                'community_id': comm_id,
                'community_name': row['community_name'],
                'release_year_mean': row['release_year_mean'],
                'top_genre': top_genre['value'],
                'top_genre_percentage': top_genre['percentage'],
                'size': profile['size']
            })
        
        timeline_df = pd.DataFrame(community_genre_timeline)
        
        figures = {}
        
        # Matplotlib version
        fig_mpl, (ax1, ax2) = plt.subplots(2, 1, figsize=FIGURE_SIZES['medium'])
        
        # Timeline scatter plot
        colors = [get_community_color(i) for i in timeline_df['community_id']]
        
        scatter1 = ax1.scatter(timeline_df['release_year_mean'], timeline_df['top_genre_percentage'],
                              c=colors, s=timeline_df['size']/10, alpha=0.7, edgecolors='black')
        ax1.set_xlabel('Average Release Year')
        ax1.set_ylabel('Top Genre Dominance (%)')
        ax1.set_title('Genre Dominance vs Release Timeline', fontweight='bold')
        ax1.grid(True, alpha=0.3)
        
        # Add community labels
        for _, row in timeline_df.iterrows():
            ax1.annotate(row['community_name'], 
                        (row['release_year_mean'], row['top_genre_percentage']),
                        xytext=(5, 5), textcoords='offset points',
                        fontsize=8, alpha=0.8)
        
        # Genre distribution over time
        genre_counts = timeline_df['top_genre'].value_counts()
        top_genres_time = genre_counts.head(8).index
        
        genre_timeline_data = []
        for genre in top_genres_time:
            genre_data = timeline_df[timeline_df['top_genre'] == genre]
            for _, row in genre_data.iterrows():
                genre_timeline_data.append({
                    'genre': genre,
                    'release_year': row['release_year_mean'],
                    'size': row['size']
                })
        
        genre_timeline_df = pd.DataFrame(genre_timeline_data)
        
        # Box plot of release years by genre
        genre_groups = [genre_timeline_df[genre_timeline_df['genre'] == genre]['release_year'].values 
                       for genre in top_genres_time]
        
        box_plot = ax2.boxplot(genre_groups, labels=top_genres_time, patch_artist=True)
        
        colors_genres = plt.cm.Set3(np.linspace(0, 1, len(top_genres_time)))
        for patch, color in zip(box_plot['boxes'], colors_genres):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)
        
        ax2.set_xlabel('Top Genre')
        ax2.set_ylabel('Release Year')
        ax2.set_title('Release Year Distribution by Dominant Genre', fontweight='bold')
        ax2.tick_params(axis='x', rotation=45)
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / 'genre_evolution_timeline.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
        
        # Plotly version - Interactive timeline
        fig_plotly = go.Figure()
        
        fig_plotly.add_trace(go.Scatter(
            x=timeline_df['release_year_mean'],
            y=timeline_df['top_genre_percentage'],
            mode='markers+text',
            marker=dict(
                size=timeline_df['size']/15,
                color=[get_community_color(i) for i in timeline_df['community_id']],
                opacity=0.7,
                line=dict(width=1, color='black'),
                sizemode='diameter'
            ),
            text=timeline_df['community_name'],
            textposition='top center',
            hovertemplate=
                '<b>%{text}</b><br>' +
                'Avg Release Year: %{x:.1f}<br>' +
                'Top Genre Dominance: %{y:.1f}%<br>' +
                'Top Genre: %{customdata[0]}<br>' +
                'Community Size: %{marker.size}<br>' +
                '<extra></extra>',
            customdata=np.column_stack([timeline_df['top_genre']]),
            name='Communities'
        ))
        
        fig_plotly.update_layout(
            **get_plotly_layout(
                title='Genre Evolution Timeline - Community Characteristics<br><sub>Bubble size represents community size</sub>',
                xaxis_title='Average Release Year',
                yaxis_title='Top Genre Dominance (%)',
                height=600
            )
        )
        
        figures['plotly'] = fig_plotly
        
        if save_plots:
            fig_plotly.write_html(self.output_dir / 'genre_evolution_interactive.html')
        
        return figures
    
    def generate_all_genre_category_plots(self, save_plots: bool = True) -> Dict:
        """
        Generate all genre and category analysis visualizations.
        
        Args:
            save_plots (bool): Whether to save all plots
            
        Returns:
            Dict: All generated figures organized by category
        """
        print("🎨 Generating genre and category analysis visualizations...")
        
        all_figures = {}
        
        # Generate each visualization
        print("  🔥 Creating genre distribution heatmap...")
        all_figures['genre_heatmap'] = self.create_genre_distribution_heatmap(save_plots=save_plots)
        
        print("  📊 Creating genre diversity analysis...")
        all_figures['genre_diversity'] = self.create_genre_diversity_analysis(save_plots=save_plots)
        
        print("  🏆 Creating top genres comparison...")
        all_figures['top_genres'] = self.create_top_genres_comparison(save_plots=save_plots)
        
        print("  🔍 Creating category clustering analysis...")
        all_figures['clustering'] = self.create_category_clustering_analysis(save_plots=save_plots)
        
        print("  ⏰ Creating genre evolution timeline...")
        all_figures['timeline'] = self.create_genre_evolution_timeline(save_plots=save_plots)
        
        print(f"✅ Genre/category analysis complete! Saved to: {self.output_dir}")
        return all_figures

# =============================================================================
# COMMAND LINE INTERFACE
# =============================================================================

def main():
    """Command line interface for genre and category analysis visualizations."""
    import argparse
    try:
        from .data_loader import load_data
    except ImportError:
        from data_loader import load_data
    
    parser = argparse.ArgumentParser(description='Generate genre and category analysis visualizations')
    parser.add_argument('--data-dir', type=str, default=None,
                       help='Directory containing community data files')
    parser.add_argument('--output-dir', type=str, 
                       default='communities_visualizations/outputs/static_plots/genres_categories',
                       help='Output directory for generated plots')
    parser.add_argument('--top-genres', type=int, default=12,
                       help='Number of top genres to analyze (default: 12)')
    parser.add_argument('--no-save', action='store_true',
                       help='Don\'t save plots to files (display only)')
    
    args = parser.parse_args()
    
    try:
        # Load data
        print("📥 Loading community data...")
        data_loader = load_data(args.data_dir)
        
        # Create analyzer
        analyzer = GenreCategoryAnalyzer(data_loader, args.output_dir)
        
        # Generate all plots
        figures = analyzer.generate_all_genre_category_plots(save_plots=not args.no_save)
        
        print("🎉 Genre and category analysis visualization generation complete!")
        
        # Show summary
        total_plots = sum(len(category_figs) for category_figs in figures.values())
        print(f"Generated {total_plots} visualizations across {len(figures)} categories")
        
    except Exception as e:
        print(f"❌ Error generating visualizations: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()