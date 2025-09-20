"""
Community Similarity Analysis Visualizations

Creates comprehensive visualizations analyzing similarity between communities
using distance matrices, hierarchical clustering, multidimensional scaling,
and feature importance analysis.
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
import warnings

# Advanced analytics imports
from scipy.cluster.hierarchy import dendrogram, linkage, fcluster
from scipy.spatial.distance import pdist, squareform
from sklearn.manifold import MDS, TSNE
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans, DBSCAN
from sklearn.preprocessing import StandardScaler
from sklearn.metrics.pairwise import cosine_similarity, euclidean_distances
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_selection import SelectKBest, f_classif

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

class SimilarityAnalyzer:
    """
    Creates community similarity analysis visualizations for Steam game communities.
    """
    
    def __init__(self, data_loader: CommunityDataLoader, output_dir: str = None):
        """
        Initialize the analyzer.
        
        Args:
            data_loader (CommunityDataLoader): Loaded community data
            output_dir (str, optional): Output directory for plots
        """
        self.data_loader = data_loader
        self.output_dir = Path(output_dir) if output_dir else Path('outputs/static_plots/similarity_analysis')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load required data
        if data_loader.community_profiles is None:
            data_loader.load_community_profiles()
        if data_loader.detailed_profiles is None:
            data_loader.load_detailed_profiles()
    
    def create_distance_matrix_analysis(self, method: str = 'cosine', save_plots: bool = True) -> Dict:
        """
        Create distance/similarity matrix visualizations.
        
        Args:
            method (str): Similarity method ('cosine', 'euclidean', 'correlation')
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing matplotlib and plotly figures
        """
        # Get similarity matrix
        similarity_matrix = self.data_loader.get_similarity_matrix(method=method)
        
        # Convert similarity to distance for some analyses
        if method == 'cosine':
            distance_matrix = 1 - similarity_matrix
        elif method == 'euclidean':
            # For euclidean, similarity_matrix is actually distance-based
            distance_matrix = similarity_matrix
        else:  # correlation
            distance_matrix = 1 - similarity_matrix
        
        figures = {}
        
        # Matplotlib version
        fig_mpl, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=FIGURE_SIZES['large'])
        
        # Similarity heatmap
        mask = np.triu(np.ones_like(similarity_matrix, dtype=bool), k=1)
        sns.heatmap(similarity_matrix, mask=mask, annot=True, fmt='.3f', 
                   cmap='RdBu_r', center=0, square=True, ax=ax1,
                   cbar_kws={'label': f'{method.title()} Similarity'})
        ax1.set_title(f'Community Similarity Matrix ({method.title()})', fontweight='bold')
        
        # Distance heatmap (full matrix)
        sns.heatmap(distance_matrix, annot=True, fmt='.3f', 
                   cmap='viridis', square=True, ax=ax2,
                   cbar_kws={'label': f'{method.title()} Distance'})
        ax2.set_title(f'Community Distance Matrix ({method.title()})', fontweight='bold')
        
        # Similarity distribution
        # Get upper triangle values (excluding diagonal)
        upper_triangle = similarity_matrix.values[np.triu_indices_from(similarity_matrix.values, k=1)]
        
        ax3.hist(upper_triangle, bins=20, alpha=0.7, color='skyblue', edgecolor='black')
        ax3.axvline(np.mean(upper_triangle), color='red', linestyle='--', 
                   label=f'Mean: {np.mean(upper_triangle):.3f}')
        ax3.axvline(np.median(upper_triangle), color='orange', linestyle='--', 
                   label=f'Median: {np.median(upper_triangle):.3f}')
        ax3.set_xlabel(f'{method.title()} Similarity')
        ax3.set_ylabel('Frequency')
        ax3.set_title('Similarity Distribution', fontweight='bold')
        ax3.legend()
        ax3.grid(True, alpha=0.3)
        
        # Most/Least similar pairs
        # Find most and least similar pairs
        similarity_pairs = []
        for i in range(len(similarity_matrix)):
            for j in range(i+1, len(similarity_matrix)):
                similarity_pairs.append({
                    'community1': similarity_matrix.index[i],
                    'community2': similarity_matrix.index[j],
                    'similarity': similarity_matrix.iloc[i, j]
                })
        
        pairs_df = pd.DataFrame(similarity_pairs).sort_values('similarity')
        
        # Plot top 5 most and least similar pairs
        top_pairs = pairs_df.tail(5)
        bottom_pairs = pairs_df.head(5)
        
        y_pos = np.arange(10)
        similarities = list(bottom_pairs['similarity']) + list(top_pairs['similarity'])
        pair_labels = [f"{row['community1'][:8]} - {row['community2'][:8]}" 
                      for _, row in bottom_pairs.iterrows()] + \
                     [f"{row['community1'][:8]} - {row['community2'][:8]}" 
                      for _, row in top_pairs.iterrows()]
        
        colors = ['red'] * 5 + ['green'] * 5
        bars = ax4.barh(y_pos, similarities, color=colors, alpha=0.7, edgecolor='black')
        ax4.set_yticks(y_pos)
        ax4.set_yticklabels(pair_labels, fontsize=8)
        ax4.set_xlabel(f'{method.title()} Similarity')
        ax4.set_title('Most and Least Similar Community Pairs', fontweight='bold')
        ax4.axvline(x=np.mean(upper_triangle), color='black', linestyle='--', alpha=0.5)
        
        # Add value labels
        for bar, sim in zip(bars, similarities):
            ax4.text(bar.get_width() + 0.01, bar.get_y() + bar.get_height()/2,
                    f'{sim:.3f}', va='center', ha='left', fontsize=8)
        
        plt.tight_layout()
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / f'distance_matrix_{method}.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
        
        # Plotly version - Interactive heatmap
        fig_plotly = go.Figure(data=go.Heatmap(
            z=similarity_matrix.values,
            x=similarity_matrix.columns,
            y=similarity_matrix.index,
            colorscale='RdBu',
            zmid=0,
            hoverongaps=False,
            hovertemplate='%{y} vs %{x}<br>Similarity: %{z:.3f}<extra></extra>',
            text=np.round(similarity_matrix.values, 3),
            texttemplate='%{text}',
            textfont={"size": 9}
        ))
        
        fig_plotly.update_layout(
            **get_plotly_layout(
                title=f'Interactive Community Similarity Matrix ({method.title()})',
                xaxis_title='Community',
                yaxis_title='Community',
                height=600
            )
        )
        
        figures['plotly'] = fig_plotly
        
        if save_plots:
            fig_plotly.write_html(self.output_dir / f'similarity_matrix_{method}_interactive.html')
        
        return figures
    
    def create_hierarchical_clustering_analysis(self, save_plots: bool = True) -> Dict:
        """
        Create hierarchical clustering analysis visualizations.
        
        Args:
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing matplotlib and plotly figures
        """
        # Get feature matrix for clustering
        feature_matrix = self.data_loader.get_feature_matrix()
        
        figures = {}
        
        # Matplotlib version
        fig_mpl, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=FIGURE_SIZES['large'])
        
        # Hierarchical clustering dendrogram
        linkage_matrix = linkage(feature_matrix, method='ward')
        
        # Create dendrogram
        dendro = dendrogram(linkage_matrix, ax=ax1, 
                           labels=[COMMUNITY_NAMES[i] for i in feature_matrix.index],
                           orientation='left', leaf_font_size=10)
        ax1.set_title('Hierarchical Clustering Dendrogram', fontweight='bold')
        ax1.set_xlabel('Distance')
        
        # Clustered heatmap
        # Get cluster order from dendrogram
        cluster_order = dendro['leaves']
        clustered_matrix = feature_matrix.iloc[cluster_order]
        
        # Select top features for visualization
        top_features = feature_matrix.var().sort_values(ascending=False).head(10).index
        clustered_subset = clustered_matrix[top_features]
        
        sns.heatmap(clustered_subset.T, cmap='viridis', ax=ax2, 
                   cbar_kws={'label': 'Normalized Value'},
                   xticklabels=[COMMUNITY_NAMES[i] for i in clustered_subset.index],
                   yticklabels=True)
        ax2.set_title('Clustered Feature Heatmap', fontweight='bold')
        ax2.set_xlabel('Community (Clustered Order)')
        ax2.set_ylabel('Features')
        
        # Different numbers of clusters
        n_clusters_range = range(2, 8)
        cluster_results = {}
        
        for n_clusters in n_clusters_range:
            clusters = fcluster(linkage_matrix, n_clusters, criterion='maxclust')
            cluster_results[n_clusters] = clusters
            
        # Plot cluster assignments for different k values
        cluster_df = pd.DataFrame(cluster_results, 
                                 index=[COMMUNITY_NAMES[i] for i in feature_matrix.index])
        
        sns.heatmap(cluster_df.T, cmap='tab10', ax=ax3, 
                   cbar_kws={'label': 'Cluster ID'}, annot=True, fmt='d')
        ax3.set_title('Cluster Assignments for Different k Values', fontweight='bold')
        ax3.set_xlabel('Community')
        ax3.set_ylabel('Number of Clusters')
        
        # Cophenetic correlation analysis
        from scipy.cluster.hierarchy import cophenet
        
        methods = ['single', 'complete', 'average', 'ward']
        coph_corrs = []
        
        for method in methods:
            linkage_temp = linkage(feature_matrix, method=method)
            coph_corr, _ = cophenet(linkage_temp, pdist(feature_matrix))
            coph_corrs.append(coph_corr)
        
        bars = ax4.bar(methods, coph_corrs, color='steelblue', alpha=0.8, 
                      edgecolor='black', linewidth=0.5)
        ax4.set_ylabel('Cophenetic Correlation')
        ax4.set_title('Clustering Method Comparison', fontweight='bold')
        ax4.set_ylim(0, 1)
        
        # Add value labels
        for bar, corr in zip(bars, coph_corrs):
            ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01,
                    f'{corr:.3f}', ha='center', va='bottom', fontweight='bold')
        
        plt.tight_layout()
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / 'hierarchical_clustering_analysis.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
        
        # Plotly version - Interactive dendrogram
        import plotly.figure_factory as ff
        
        # Create dendrogram with plotly
        community_labels = [COMMUNITY_NAMES[i] for i in feature_matrix.index]
        
        fig_plotly = ff.create_dendrogram(
            feature_matrix.values,
            labels=community_labels,
            linkagefun=lambda x: linkage(x, 'ward'),
            orientation='left'
        )
        
        fig_plotly.update_layout(
            **get_plotly_layout(
                title='Interactive Hierarchical Clustering Dendrogram',
                height=700,
                xaxis_title='Distance',
                yaxis_title='Communities'
            )
        )
        
        figures['plotly'] = fig_plotly
        
        if save_plots:
            fig_plotly.write_html(self.output_dir / 'hierarchical_clustering_interactive.html')
        
        return figures
    
    def create_multidimensional_scaling_analysis(self, save_plots: bool = True) -> Dict:
        """
        Create multidimensional scaling (MDS) analysis visualizations.
        
        Args:
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing matplotlib and plotly figures
        """
        # Get feature matrix and similarity matrix
        feature_matrix = self.data_loader.get_feature_matrix()
        similarity_matrix = self.data_loader.get_similarity_matrix()
        distance_matrix = 1 - similarity_matrix
        
        # Perform different dimensionality reduction techniques
        mds = MDS(n_components=2, dissimilarity='precomputed', random_state=42)
        mds_coords = mds.fit_transform(distance_matrix)
        
        # PCA for comparison
        pca = PCA(n_components=2, random_state=42)
        pca_coords = pca.fit_transform(feature_matrix)
        
        # t-SNE for comparison
        tsne = TSNE(n_components=2, random_state=42, perplexity=min(30, len(feature_matrix)-1))
        tsne_coords = tsne.fit_transform(feature_matrix)
        
        figures = {}
        
        # Matplotlib version
        fig_mpl, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=FIGURE_SIZES['large'])
        
        community_names = [COMMUNITY_NAMES[i] for i in feature_matrix.index]
        colors = [get_community_color(i) for i in feature_matrix.index]
        
        # MDS plot
        scatter1 = ax1.scatter(mds_coords[:, 0], mds_coords[:, 1], 
                              c=colors, s=100, alpha=0.7, edgecolors='black')
        for i, name in enumerate(community_names):
            ax1.annotate(name, (mds_coords[i, 0], mds_coords[i, 1]),
                        xytext=(5, 5), textcoords='offset points',
                        fontsize=8, alpha=0.8)
        ax1.set_title(f'MDS Projection (Stress: {mds.stress_:.3f})', fontweight='bold')
        ax1.set_xlabel('MDS Dimension 1')
        ax1.set_ylabel('MDS Dimension 2')
        ax1.grid(True, alpha=0.3)
        
        # PCA plot
        scatter2 = ax2.scatter(pca_coords[:, 0], pca_coords[:, 1], 
                              c=colors, s=100, alpha=0.7, edgecolors='black')
        for i, name in enumerate(community_names):
            ax2.annotate(name, (pca_coords[i, 0], pca_coords[i, 1]),
                        xytext=(5, 5), textcoords='offset points',
                        fontsize=8, alpha=0.8)
        ax2.set_title(f'PCA Projection\n(Explained Variance: {pca.explained_variance_ratio_.sum():.3f})', 
                     fontweight='bold')
        ax2.set_xlabel(f'PC1 ({pca.explained_variance_ratio_[0]:.1%})')
        ax2.set_ylabel(f'PC2 ({pca.explained_variance_ratio_[1]:.1%})')
        ax2.grid(True, alpha=0.3)
        
        # t-SNE plot
        scatter3 = ax3.scatter(tsne_coords[:, 0], tsne_coords[:, 1], 
                              c=colors, s=100, alpha=0.7, edgecolors='black')
        for i, name in enumerate(community_names):
            ax3.annotate(name, (tsne_coords[i, 0], tsne_coords[i, 1]),
                        xytext=(5, 5), textcoords='offset points',
                        fontsize=8, alpha=0.8)
        ax3.set_title('t-SNE Projection', fontweight='bold')
        ax3.set_xlabel('t-SNE Dimension 1')
        ax3.set_ylabel('t-SNE Dimension 2')
        ax3.grid(True, alpha=0.3)
        
        # Explained variance for different numbers of components (PCA)
        pca_full = PCA()
        pca_full.fit(feature_matrix)
        cumulative_variance = np.cumsum(pca_full.explained_variance_ratio_)
        
        ax4.plot(range(1, len(cumulative_variance) + 1), cumulative_variance, 
                'bo-', linewidth=2, markersize=6)
        ax4.axhline(y=0.8, color='red', linestyle='--', alpha=0.7, label='80% Variance')
        ax4.axhline(y=0.9, color='orange', linestyle='--', alpha=0.7, label='90% Variance')
        ax4.set_xlabel('Number of Components')
        ax4.set_ylabel('Cumulative Explained Variance')
        ax4.set_title('PCA Variance Explained', fontweight='bold')
        ax4.legend()
        ax4.grid(True, alpha=0.3)
        ax4.set_xlim(1, len(cumulative_variance))
        ax4.set_ylim(0, 1)
        
        plt.tight_layout()
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / 'multidimensional_scaling_analysis.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
        
        # Plotly version - Interactive 3D MDS
        # 3D MDS
        mds_3d = MDS(n_components=3, dissimilarity='precomputed', random_state=42)
        mds_3d_coords = mds_3d.fit_transform(distance_matrix)
        
        fig_plotly = go.Figure(data=go.Scatter3d(
            x=mds_3d_coords[:, 0],
            y=mds_3d_coords[:, 1],
            z=mds_3d_coords[:, 2],
            mode='markers+text',
            marker=dict(
                size=10,
                color=[get_community_color(i) for i in feature_matrix.index],
                opacity=0.8,
                line=dict(width=1, color='black')
            ),
            text=community_names,
            textposition='top center',
            hovertemplate='<b>%{text}</b><br>X: %{x:.3f}<br>Y: %{y:.3f}<br>Z: %{z:.3f}<extra></extra>'
        ))
        
        fig_plotly.update_layout(
            **get_plotly_layout(
                title=f'Interactive 3D MDS Projection (Stress: {mds_3d.stress_:.3f})',
                height=700
            ),
            scene=dict(
                xaxis_title='MDS Dimension 1',
                yaxis_title='MDS Dimension 2',
                zaxis_title='MDS Dimension 3'
            )
        )
        
        figures['plotly'] = fig_plotly
        
        if save_plots:
            fig_plotly.write_html(self.output_dir / 'mds_3d_interactive.html')
        
        return figures
    
    def create_feature_importance_analysis(self, save_plots: bool = True) -> Dict:
        """
        Create feature importance analysis for community classification.
        
        Args:
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing matplotlib and plotly figures
        """
        # Get feature matrix
        feature_matrix = self.data_loader.get_feature_matrix()
        
        # Create synthetic clustering for feature importance analysis
        kmeans = KMeans(n_clusters=4, random_state=42)
        cluster_labels = kmeans.fit_predict(feature_matrix)
        
        # Random Forest feature importance
        rf = RandomForestClassifier(n_estimators=100, random_state=42)
        rf.fit(feature_matrix, cluster_labels)
        rf_importance = rf.feature_importances_
        
        # Statistical feature selection
        selector = SelectKBest(f_classif, k='all')
        selector.fit(feature_matrix, cluster_labels)
        statistical_scores = selector.scores_
        
        # Feature variance analysis
        feature_variance = feature_matrix.var()
        
        figures = {}
        
        # Matplotlib version
        fig_mpl, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=FIGURE_SIZES['large'])
        
        feature_names = feature_matrix.columns
        
        # Random Forest feature importance
        rf_sorted_idx = np.argsort(rf_importance)[::-1][:15]  # Top 15 features
        
        bars1 = ax1.barh(range(len(rf_sorted_idx)), rf_importance[rf_sorted_idx],
                        color='steelblue', alpha=0.8, edgecolor='black', linewidth=0.5)
        ax1.set_yticks(range(len(rf_sorted_idx)))
        ax1.set_yticklabels([feature_names[i] for i in rf_sorted_idx], fontsize=9)
        ax1.set_xlabel('Feature Importance')
        ax1.set_title('Random Forest Feature Importance\n(Top 15 Features)', fontweight='bold')
        
        # Add value labels
        for bar, imp in zip(bars1, rf_importance[rf_sorted_idx]):
            ax1.text(bar.get_width() + 0.001, bar.get_y() + bar.get_height()/2,
                    f'{imp:.3f}', va='center', ha='left', fontsize=8)
        
        # Statistical significance (F-scores)
        stat_sorted_idx = np.argsort(statistical_scores)[::-1][:15]
        
        bars2 = ax2.barh(range(len(stat_sorted_idx)), statistical_scores[stat_sorted_idx],
                        color='lightcoral', alpha=0.8, edgecolor='black', linewidth=0.5)
        ax2.set_yticks(range(len(stat_sorted_idx)))
        ax2.set_yticklabels([feature_names[i] for i in stat_sorted_idx], fontsize=9)
        ax2.set_xlabel('F-Score')
        ax2.set_title('Statistical Feature Significance\n(Top 15 Features)', fontweight='bold')
        
        # Feature variance
        var_sorted_idx = np.argsort(feature_variance)[::-1][:15]
        
        bars3 = ax3.barh(range(len(var_sorted_idx)), feature_variance[var_sorted_idx],
                        color='lightgreen', alpha=0.8, edgecolor='black', linewidth=0.5)
        ax3.set_yticks(range(len(var_sorted_idx)))
        ax3.set_yticklabels([feature_names[i] for i in var_sorted_idx], fontsize=9)
        ax3.set_xlabel('Variance')
        ax3.set_title('Feature Variance\n(Top 15 Features)', fontweight='bold')
        
        # Combined importance ranking
        # Normalize all importance measures to 0-1 scale
        rf_norm = (rf_importance - rf_importance.min()) / (rf_importance.max() - rf_importance.min())
        stat_norm = (statistical_scores - statistical_scores.min()) / (statistical_scores.max() - statistical_scores.min())
        var_norm = (feature_variance - feature_variance.min()) / (feature_variance.max() - feature_variance.min())
        
        combined_importance = (rf_norm + stat_norm + var_norm) / 3
        combined_sorted_idx = np.argsort(combined_importance)[::-1][:15]
        
        bars4 = ax4.barh(range(len(combined_sorted_idx)), combined_importance[combined_sorted_idx],
                        color='gold', alpha=0.8, edgecolor='black', linewidth=0.5)
        ax4.set_yticks(range(len(combined_sorted_idx)))
        ax4.set_yticklabels([feature_names[i] for i in combined_sorted_idx], fontsize=9)
        ax4.set_xlabel('Combined Importance Score')
        ax4.set_title('Combined Feature Importance\n(Top 15 Features)', fontweight='bold')
        
        plt.tight_layout()
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / 'feature_importance_analysis.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
        
        # Plotly version - Interactive feature importance comparison
        top_features_idx = combined_sorted_idx[:10]  # Top 10 for clarity
        top_feature_names = [feature_names[i] for i in top_features_idx]
        
        fig_plotly = go.Figure()
        
        # Add traces for different importance measures
        fig_plotly.add_trace(go.Bar(
            name='Random Forest',
            x=top_feature_names,
            y=rf_importance[top_features_idx],
            marker_color='steelblue',
            opacity=0.7
        ))
        
        fig_plotly.add_trace(go.Bar(
            name='Statistical (F-score)',
            x=top_feature_names,
            y=statistical_scores[top_features_idx],
            marker_color='lightcoral',
            opacity=0.7,
            yaxis='y2'
        ))
        
        # Update layout for dual y-axis
        fig_plotly.update_layout(
            **get_plotly_layout(
                title='Interactive Feature Importance Comparison - Top 10 Features',
                xaxis_title='Features',
                height=600,
                barmode='group'
            ),
            yaxis=dict(title='RF Importance', side='left'),
            yaxis2=dict(title='F-Score', side='right', overlaying='y')
        )
        
        figures['plotly'] = fig_plotly
        
        if save_plots:
            fig_plotly.write_html(self.output_dir / 'feature_importance_interactive.html')
        
        return figures
    
    def create_clustering_comparison_analysis(self, save_plots: bool = True) -> Dict:
        """
        Create comparison of different clustering methods.
        
        Args:
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing matplotlib and plotly figures
        """
        # Get feature matrix
        feature_matrix = self.data_loader.get_feature_matrix()
        
        # Apply different clustering methods
        clustering_methods = {
            'K-Means (k=3)': KMeans(n_clusters=3, random_state=42),
            'K-Means (k=4)': KMeans(n_clusters=4, random_state=42),
            'K-Means (k=5)': KMeans(n_clusters=5, random_state=42),
            'DBSCAN (eps=0.5)': DBSCAN(eps=0.5, min_samples=2)
        }
        
        # Get 2D representation for visualization
        pca = PCA(n_components=2, random_state=42)
        coords_2d = pca.fit_transform(feature_matrix)
        
        clustering_results = {}
        for name, clusterer in clustering_methods.items():
            labels = clusterer.fit_predict(feature_matrix)
            clustering_results[name] = labels
        
        figures = {}
        
        # Matplotlib version
        fig_mpl, axes = plt.subplots(2, 2, figsize=FIGURE_SIZES['large'])
        axes = axes.flatten()
        
        community_names = [COMMUNITY_NAMES[i] for i in feature_matrix.index]
        
        for i, (method_name, labels) in enumerate(clustering_results.items()):
            ax = axes[i]
            
            # Use different colors for clusters
            unique_labels = np.unique(labels)
            colors = plt.cm.tab10(np.linspace(0, 1, len(unique_labels)))
            
            for label in unique_labels:
                mask = labels == label
                if label == -1:  # DBSCAN noise points
                    ax.scatter(coords_2d[mask, 0], coords_2d[mask, 1], 
                              c='black', marker='x', s=50, alpha=0.6, label='Noise')
                else:
                    ax.scatter(coords_2d[mask, 0], coords_2d[mask, 1], 
                              c=[colors[label]], s=80, alpha=0.7, 
                              edgecolors='black', label=f'Cluster {label}')
            
            # Add community labels
            for j, name in enumerate(community_names):
                ax.annotate(name, (coords_2d[j, 0], coords_2d[j, 1]),
                           xytext=(5, 5), textcoords='offset points',
                           fontsize=7, alpha=0.8)
            
            ax.set_title(method_name, fontweight='bold')
            ax.set_xlabel(f'PC1 ({pca.explained_variance_ratio_[0]:.1%})')
            ax.set_ylabel(f'PC2 ({pca.explained_variance_ratio_[1]:.1%})')
            ax.legend(fontsize=8)
            ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / 'clustering_comparison_analysis.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
        
        # Plotly version - Interactive clustering comparison
        fig_plotly = make_subplots(
            rows=2, cols=2,
            subplot_titles=list(clustering_results.keys()),
            specs=[[{"type": "scatter"}, {"type": "scatter"}],
                   [{"type": "scatter"}, {"type": "scatter"}]]
        )
        
        for i, (method_name, labels) in enumerate(clustering_results.items()):
            row = i // 2 + 1
            col = i % 2 + 1
            
            unique_labels = np.unique(labels)
            colors = px.colors.qualitative.Set1[:len(unique_labels)]
            
            for j, label in enumerate(unique_labels):
                mask = labels == label
                
                if label == -1:  # DBSCAN noise points
                    marker_symbol = 'x'
                    marker_color = 'black'
                    cluster_name = 'Noise'
                else:
                    marker_symbol = 'circle'
                    marker_color = colors[j % len(colors)]
                    cluster_name = f'Cluster {label}'
                
                fig_plotly.add_trace(
                    go.Scatter(
                        x=coords_2d[mask, 0],
                        y=coords_2d[mask, 1],
                        mode='markers+text',
                        marker=dict(
                            symbol=marker_symbol,
                            size=8,
                            color=marker_color,
                            line=dict(width=1, color='black')
                        ),
                        text=[community_names[k] for k in np.where(mask)[0]],
                        textposition='top center',
                        name=f'{method_name}: {cluster_name}',
                        showlegend=(i == 0),  # Only show legend for first subplot
                        hovertemplate='<b>%{text}</b><br>PC1: %{x:.3f}<br>PC2: %{y:.3f}<extra></extra>'
                    ),
                    row=row, col=col
                )
        
        fig_plotly.update_layout(
            **get_plotly_layout(
                title='Interactive Clustering Methods Comparison',
                height=800,
                showlegend=True
            )
        )
        
        # Update subplot axes
        for i in range(1, 5):
            fig_plotly.update_xaxes(title_text=f'PC1 ({pca.explained_variance_ratio_[0]:.1%})', 
                                   row=(i-1)//2+1, col=(i-1)%2+1)
            fig_plotly.update_yaxes(title_text=f'PC2 ({pca.explained_variance_ratio_[1]:.1%})', 
                                   row=(i-1)//2+1, col=(i-1)%2+1)
        
        figures['plotly'] = fig_plotly
        
        if save_plots:
            fig_plotly.write_html(self.output_dir / 'clustering_comparison_interactive.html')
        
        return figures
    
    def generate_all_similarity_analysis_plots(self, save_plots: bool = True) -> Dict:
        """
        Generate all similarity analysis visualizations.
        
        Args:
            save_plots (bool): Whether to save all plots
            
        Returns:
            Dict: All generated figures organized by category
        """
        print("🎨 Generating community similarity analysis visualizations...")
        
        all_figures = {}
        
        # Generate each visualization
        print("  📊 Creating distance matrix analysis...")
        all_figures['distance_matrices'] = {}
        for method in ['cosine', 'euclidean', 'correlation']:
            print(f"    - {method.title()} similarity matrix...")
            all_figures['distance_matrices'][method] = self.create_distance_matrix_analysis(
                method=method, save_plots=save_plots)
        
        print("  🌳 Creating hierarchical clustering analysis...")
        all_figures['hierarchical_clustering'] = self.create_hierarchical_clustering_analysis(save_plots=save_plots)
        
        print("  📐 Creating multidimensional scaling analysis...")
        all_figures['mds_analysis'] = self.create_multidimensional_scaling_analysis(save_plots=save_plots)
        
        print("  🎯 Creating feature importance analysis...")
        all_figures['feature_importance'] = self.create_feature_importance_analysis(save_plots=save_plots)
        
        print("  🔍 Creating clustering comparison analysis...")
        all_figures['clustering_comparison'] = self.create_clustering_comparison_analysis(save_plots=save_plots)
        
        print(f"✅ Similarity analysis complete! Saved to: {self.output_dir}")
        return all_figures

# =============================================================================
# COMMAND LINE INTERFACE
# =============================================================================

def main():
    """Command line interface for similarity analysis visualizations."""
    import argparse
    try:
        from .data_loader import load_data
    except ImportError:
        from data_loader import load_data
    
    parser = argparse.ArgumentParser(description='Generate community similarity analysis visualizations')
    parser.add_argument('--data-dir', type=str, default=None,
                       help='Directory containing community data files')
    parser.add_argument('--output-dir', type=str, 
                       default='communities_visualizations/outputs/static_plots/similarity_analysis',
                       help='Output directory for generated plots')
    parser.add_argument('--similarity-methods', nargs='+', 
                       default=['cosine', 'euclidean', 'correlation'],
                       help='Similarity methods to analyze (default: cosine euclidean correlation)')
    parser.add_argument('--no-save', action='store_true',
                       help='Don\'t save plots to files (display only)')
    
    args = parser.parse_args()
    
    try:
        # Load data
        print("📥 Loading community data...")
        data_loader = load_data(args.data_dir)
        
        # Create analyzer
        analyzer = SimilarityAnalyzer(data_loader, args.output_dir)
        
        # Generate all plots
        figures = analyzer.generate_all_similarity_analysis_plots(save_plots=not args.no_save)
        
        print("🎉 Community similarity analysis visualization generation complete!")
        
        # Show summary
        total_plots = 0
        for category_figs in figures.values():
            if isinstance(category_figs, dict):
                for subcategory_figs in category_figs.values():
                    if isinstance(subcategory_figs, dict):
                        total_plots += len(subcategory_figs)
                    else:
                        total_plots += 1
            else:
                total_plots += 1
        
        print(f"Generated {total_plots} visualizations across {len(figures)} categories")
        
    except Exception as e:
        print(f"❌ Error generating visualizations: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()