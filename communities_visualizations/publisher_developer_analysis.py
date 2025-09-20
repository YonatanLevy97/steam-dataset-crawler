"""
Publisher and Developer Analysis Visualizations

Creates comprehensive visualizations analyzing publisher and developer patterns,
concentration ratios, and cross-community presence across Steam game communities.
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
import networkx as nx
from collections import Counter
import warnings

try:
    from .config import (
        COMMUNITY_COLORS, COMMUNITY_NAMES, FIGURE_SIZES, DPI_SETTINGS,
        get_community_color, get_plotly_layout, NETWORK_PARAMS
    )
    from .data_loader import CommunityDataLoader
except ImportError:
    from config import (
        COMMUNITY_COLORS, COMMUNITY_NAMES, FIGURE_SIZES, DPI_SETTINGS,
        get_community_color, get_plotly_layout, NETWORK_PARAMS
    )
    from data_loader import CommunityDataLoader

class PublisherDeveloperAnalyzer:
    """
    Creates publisher and developer analysis visualizations for Steam game communities.
    """
    
    def __init__(self, data_loader: CommunityDataLoader, output_dir: str = None):
        """
        Initialize the analyzer.
        
        Args:
            data_loader (CommunityDataLoader): Loaded community data
            output_dir (str, optional): Output directory for plots
        """
        self.data_loader = data_loader
        self.output_dir = Path(output_dir) if output_dir else Path('outputs/static_plots/publishers_developers')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load required data
        if data_loader.community_profiles is None:
            data_loader.load_community_profiles()
        if data_loader.detailed_profiles is None:
            data_loader.load_detailed_profiles()
    
    def create_publisher_concentration_analysis(self, save_plots: bool = True) -> Dict:
        """
        Create publisher concentration analysis visualizations.
        
        Args:
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing matplotlib and plotly figures
        """
        # Extract publisher data
        publisher_data = []
        
        for comm_id, comm_name in COMMUNITY_NAMES.items():
            if str(comm_id) not in self.data_loader.detailed_profiles['community_profiles']:
                continue
            
            profile = self.data_loader.detailed_profiles['community_profiles'][str(comm_id)]
            publishers = profile['categorical_features']['publishers']['top_values'][:10]
            
            # Calculate concentration metrics
            total_games = profile['size']
            top_publisher_count = publishers[0]['count'] if publishers else 0
            top_5_count = sum([p['count'] for p in publishers[:5]])
            
            # HHI (Herfindahl-Hirschman Index) approximation
            percentages = [p['percentage'] for p in publishers]
            hhi = sum([(p/100)**2 for p in percentages])
            
            publisher_data.append({
                'community_id': comm_id,
                'community_name': comm_name,
                'total_games': total_games,
                'top_publisher': publishers[0]['value'] if publishers else 'N/A',
                'top_publisher_count': top_publisher_count,
                'top_publisher_percentage': publishers[0]['percentage'] if publishers else 0,
                'top_5_concentration': (top_5_count / total_games) * 100,
                'hhi_index': hhi,
                'unique_publishers': len(publishers)
            })
        
        concentration_df = pd.DataFrame(publisher_data)
        
        figures = {}
        
        # Matplotlib version
        fig_mpl, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=FIGURE_SIZES['large'])
        
        colors = [get_community_color(i) for i in concentration_df['community_id']]
        
        # Top publisher dominance
        bars1 = ax1.bar(concentration_df['community_name'], 
                       concentration_df['top_publisher_percentage'],
                       color=colors, alpha=0.8, edgecolor='black', linewidth=0.5)
        ax1.set_title('Top Publisher Dominance by Community', fontweight='bold')
        ax1.set_ylabel('Top Publisher Percentage')
        ax1.tick_params(axis='x', rotation=45)
        
        # Add value labels
        for bar, pct in zip(bars1, concentration_df['top_publisher_percentage']):
            ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                    f'{pct:.1f}%', ha='center', va='bottom', fontsize=8)
        
        # HHI vs Community Size
        scatter2 = ax2.scatter(concentration_df['total_games'], concentration_df['hhi_index'],
                              c=colors, s=80, alpha=0.7, edgecolors='black')
        ax2.set_xlabel('Community Size')
        ax2.set_ylabel('Publisher Concentration (HHI)')
        ax2.set_title('Publisher Concentration vs Community Size', fontweight='bold')
        ax2.grid(True, alpha=0.3)
        
        # Add trend line
        z = np.polyfit(concentration_df['total_games'], concentration_df['hhi_index'], 1)
        p = np.poly1d(z)
        ax2.plot(concentration_df['total_games'], p(concentration_df['total_games']), 
                "r--", alpha=0.8, linewidth=2)
        
        # Top 5 concentration
        bars3 = ax3.barh(concentration_df['community_name'], 
                        concentration_df['top_5_concentration'],
                        color=colors, alpha=0.8, edgecolor='black', linewidth=0.5)
        ax3.set_title('Top 5 Publishers Concentration', fontweight='bold')
        ax3.set_xlabel('Top 5 Publishers Percentage')
        
        # Publisher diversity (unique count)
        bars4 = ax4.bar(concentration_df['community_name'], 
                       concentration_df['unique_publishers'],
                       color=colors, alpha=0.8, edgecolor='black', linewidth=0.5)
        ax4.set_title('Publisher Diversity (Unique Count)', fontweight='bold')
        ax4.set_ylabel('Number of Unique Publishers')
        ax4.tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / 'publisher_concentration_analysis.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
        
        # Plotly version - Interactive bubble chart
        fig_plotly = go.Figure()
        
        fig_plotly.add_trace(go.Scatter(
            x=concentration_df['total_games'],
            y=concentration_df['hhi_index'],
            mode='markers+text',
            marker=dict(
                size=concentration_df['top_publisher_percentage'],
                color=[get_community_color(i) for i in concentration_df['community_id']],
                opacity=0.7,
                line=dict(width=1, color='black'),
                sizemode='diameter',
                sizeref=2.*max(concentration_df['top_publisher_percentage'])/(40.**2),
                sizemin=4
            ),
            text=concentration_df['community_name'],
            textposition='top center',
            hovertemplate=
                '<b>%{text}</b><br>' +
                'Community Size: %{x}<br>' +
                'HHI Index: %{y:.4f}<br>' +
                'Top Publisher: %{customdata[0]}<br>' +
                'Top Publisher %: %{customdata[1]:.1f}%<br>' +
                'Top 5 Concentration: %{customdata[2]:.1f}%<br>' +
                '<extra></extra>',
            customdata=np.column_stack((
                concentration_df['top_publisher'],
                concentration_df['top_publisher_percentage'],
                concentration_df['top_5_concentration']
            )),
            name='Communities'
        ))
        
        fig_plotly.update_layout(
            **get_plotly_layout(
                title='Publisher Concentration Analysis<br><sub>Bubble size = top publisher dominance</sub>',
                xaxis_title='Community Size',
                yaxis_title='Publisher Concentration Index (HHI)',
                height=600
            )
        )
        
        figures['plotly'] = fig_plotly
        
        if save_plots:
            fig_plotly.write_html(self.output_dir / 'publisher_concentration_interactive.html')
        
        return figures
    
    def create_publisher_network_analysis(self, min_games: int = 3, save_plots: bool = True) -> Dict:
        """
        Create publisher network analysis showing cross-community presence.
        
        Args:
            min_games (int): Minimum games required for publisher to be included
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing matplotlib and plotly figures
        """
        # Extract all publisher-community relationships
        publisher_community_data = []
        
        for comm_id, comm_name in COMMUNITY_NAMES.items():
            if str(comm_id) not in self.data_loader.detailed_profiles['community_profiles']:
                continue
            
            profile = self.data_loader.detailed_profiles['community_profiles'][str(comm_id)]
            publishers = profile['categorical_features']['publishers']['top_values']
            
            for pub_info in publishers:
                if pub_info['count'] >= min_games:
                    publisher_community_data.append({
                        'publisher': pub_info['value'],
                        'community_id': comm_id,
                        'community_name': comm_name,
                        'count': pub_info['count'],
                        'percentage': pub_info['percentage']
                    })
        
        pub_comm_df = pd.DataFrame(publisher_community_data)
        
        # Find publishers present in multiple communities
        publisher_counts = pub_comm_df.groupby('publisher').agg({
            'community_id': 'nunique',
            'count': 'sum'
        }).rename(columns={'community_id': 'num_communities', 'count': 'total_games'})
        
        cross_community_pubs = publisher_counts[publisher_counts['num_communities'] > 1].sort_values('num_communities', ascending=False)
        
        figures = {}
        
        # Matplotlib version - Network graph
        fig_mpl, (ax1, ax2) = plt.subplots(1, 2, figsize=FIGURE_SIZES['wide'])
        
        # Create network graph
        G = nx.Graph()
        
        # Add nodes for communities and publishers
        for comm_name in COMMUNITY_NAMES.values():
            G.add_node(comm_name, node_type='community', size=1000)
        
        top_cross_pubs = cross_community_pubs.head(15).index  # Top 15 cross-community publishers
        for pub in top_cross_pubs:
            G.add_node(pub, node_type='publisher', size=500)
        
        # Add edges
        for _, row in pub_comm_df[pub_comm_df['publisher'].isin(top_cross_pubs)].iterrows():
            G.add_edge(row['community_name'], row['publisher'], weight=row['count'])
        
        # Draw network
        pos = nx.spring_layout(G, k=2, iterations=50)
        
        # Draw community nodes
        community_nodes = [n for n, d in G.nodes(data=True) if d['node_type'] == 'community']
        nx.draw_networkx_nodes(G, pos, nodelist=community_nodes, 
                              node_color=[get_community_color(list(COMMUNITY_NAMES.values()).index(n)) 
                                        for n in community_nodes],
                              node_size=800, alpha=0.8, ax=ax1)
        
        # Draw publisher nodes
        publisher_nodes = [n for n, d in G.nodes(data=True) if d['node_type'] == 'publisher']
        nx.draw_networkx_nodes(G, pos, nodelist=publisher_nodes,
                              node_color='lightgray', node_size=300, alpha=0.6, ax=ax1)
        
        # Draw edges
        nx.draw_networkx_edges(G, pos, alpha=0.3, width=0.5, ax=ax1)
        
        # Draw labels
        nx.draw_networkx_labels(G, pos, font_size=6, ax=ax1)
        
        ax1.set_title('Publisher-Community Network\n(Cross-community publishers)', fontweight='bold')
        ax1.axis('off')
        
        # Cross-community publisher ranking
        top_cross_df = cross_community_pubs.head(10).reset_index()
        bars = ax2.barh(range(len(top_cross_df)), top_cross_df['num_communities'],
                       color='skyblue', alpha=0.8, edgecolor='black', linewidth=0.5)
        ax2.set_yticks(range(len(top_cross_df)))
        ax2.set_yticklabels([pub[:20] + '...' if len(pub) > 20 else pub 
                            for pub in top_cross_df['publisher']], fontsize=9)
        ax2.set_xlabel('Number of Communities')
        ax2.set_title('Top Cross-Community Publishers', fontweight='bold')
        
        # Add value labels
        for bar, count in zip(bars, top_cross_df['num_communities']):
            ax2.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height()/2,
                    f'{count}', va='center', ha='left', fontsize=9)
        
        plt.tight_layout()
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / 'publisher_network_analysis.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
        
        # Plotly version - Interactive network using plotly
        # Create network layout
        pos_plotly = nx.spring_layout(G, k=3, iterations=50)
        
        # Prepare edge traces
        edge_x, edge_y = [], []
        edge_info = []
        
        for edge in G.edges():
            x0, y0 = pos_plotly[edge[0]]
            x1, y1 = pos_plotly[edge[1]]
            edge_x.extend([x0, x1, None])
            edge_y.extend([y0, y1, None])
            edge_info.append(f"{edge[0]} - {edge[1]}")
        
        fig_plotly = go.Figure()
        
        # Add edges
        fig_plotly.add_trace(go.Scatter(
            x=edge_x, y=edge_y,
            mode='lines',
            line=dict(width=0.5, color='rgba(125,125,125,0.3)'),
            hoverinfo='none',
            showlegend=False
        ))
        
        # Add community nodes
        community_x = [pos_plotly[node][0] for node in community_nodes]
        community_y = [pos_plotly[node][1] for node in community_nodes]
        
        fig_plotly.add_trace(go.Scatter(
            x=community_x, y=community_y,
            mode='markers+text',
            marker=dict(
                size=20,
                color=[get_community_color(list(COMMUNITY_NAMES.values()).index(n)) 
                      for n in community_nodes],
                line=dict(width=2, color='black')
            ),
            text=community_nodes,
            textposition='middle center',
            textfont=dict(size=8),
            name='Communities',
            hovertemplate='<b>%{text}</b><br>Type: Community<extra></extra>'
        ))
        
        # Add publisher nodes
        publisher_x = [pos_plotly[node][0] for node in publisher_nodes]
        publisher_y = [pos_plotly[node][1] for node in publisher_nodes]
        
        fig_plotly.add_trace(go.Scatter(
            x=publisher_x, y=publisher_y,
            mode='markers+text',
            marker=dict(
                size=10,
                color='lightgray',
                line=dict(width=1, color='black')
            ),
            text=[pub[:10] + '...' if len(pub) > 10 else pub for pub in publisher_nodes],
            textposition='middle center',
            textfont=dict(size=6),
            name='Publishers',
            hovertemplate='<b>%{text}</b><br>Type: Publisher<extra></extra>'
        ))
        
        fig_plotly.update_layout(
            **get_plotly_layout(
                title='Interactive Publisher-Community Network<br><sub>Showing cross-community publishers</sub>',
                showlegend=True,
                height=700,
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
            )
        )
        
        figures['plotly'] = fig_plotly
        
        if save_plots:
            fig_plotly.write_html(self.output_dir / 'publisher_network_interactive.html')
        
        return figures
    
    def create_developer_concentration_comparison(self, save_plots: bool = True) -> Dict:
        """
        Create developer vs publisher concentration comparison.
        
        Args:
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing matplotlib and plotly figures
        """
        # Extract both publisher and developer concentration data
        comparison_data = []
        
        for comm_id, comm_name in COMMUNITY_NAMES.items():
            if str(comm_id) not in self.data_loader.detailed_profiles['community_profiles']:
                continue
            
            profile = self.data_loader.detailed_profiles['community_profiles'][str(comm_id)]
            publishers = profile['categorical_features']['publishers']['top_values'][:5]
            developers = profile['categorical_features']['developers']['top_values'][:5]
            
            # Calculate concentration metrics
            pub_concentration = publishers[0]['percentage'] if publishers else 0
            dev_concentration = developers[0]['percentage'] if developers else 0
            
            pub_top5 = sum([p['percentage'] for p in publishers])
            dev_top5 = sum([d['percentage'] for d in developers])
            
            comparison_data.append({
                'community_id': comm_id,
                'community_name': comm_name,
                'top_publisher': publishers[0]['value'] if publishers else 'N/A',
                'top_developer': developers[0]['value'] if developers else 'N/A',
                'pub_concentration': pub_concentration,
                'dev_concentration': dev_concentration,
                'pub_top5_concentration': pub_top5,
                'dev_top5_concentration': dev_top5,
                'size': profile['size']
            })
        
        comparison_df = pd.DataFrame(comparison_data)
        
        figures = {}
        
        # Matplotlib version
        fig_mpl, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=FIGURE_SIZES['large'])
        
        colors = [get_community_color(i) for i in comparison_df['community_id']]
        
        # Publisher vs Developer concentration scatter
        scatter1 = ax1.scatter(comparison_df['pub_concentration'], 
                              comparison_df['dev_concentration'],
                              c=colors, s=80, alpha=0.7, edgecolors='black')
        
        # Add diagonal line
        max_conc = max(comparison_df['pub_concentration'].max(), 
                      comparison_df['dev_concentration'].max())
        ax1.plot([0, max_conc], [0, max_conc], 'k--', alpha=0.5, 
                label='Publisher = Developer')
        
        ax1.set_xlabel('Top Publisher Concentration (%)')
        ax1.set_ylabel('Top Developer Concentration (%)')
        ax1.set_title('Publisher vs Developer Concentration', fontweight='bold')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Side-by-side comparison bars
        x_pos = np.arange(len(comparison_df))
        width = 0.35
        
        bars1 = ax2.bar(x_pos - width/2, comparison_df['pub_concentration'], 
                       width, label='Publishers', alpha=0.8, color='skyblue')
        bars2 = ax2.bar(x_pos + width/2, comparison_df['dev_concentration'], 
                       width, label='Developers', alpha=0.8, color='lightcoral')
        
        ax2.set_xlabel('Community')
        ax2.set_ylabel('Top Entity Concentration (%)')
        ax2.set_title('Publisher vs Developer Concentration Comparison', fontweight='bold')
        ax2.set_xticks(x_pos)
        ax2.set_xticklabels([name[:8] + '...' if len(name) > 8 else name 
                            for name in comparison_df['community_name']], rotation=45)
        ax2.legend()
        
        # Top 5 concentration comparison
        ax3.scatter(comparison_df['pub_top5_concentration'], 
                   comparison_df['dev_top5_concentration'],
                   c=colors, s=comparison_df['size']/15, alpha=0.7, edgecolors='black')
        
        max_top5 = max(comparison_df['pub_top5_concentration'].max(),
                      comparison_df['dev_top5_concentration'].max())
        ax3.plot([0, max_top5], [0, max_top5], 'k--', alpha=0.5)
        
        ax3.set_xlabel('Top 5 Publishers Concentration (%)')
        ax3.set_ylabel('Top 5 Developers Concentration (%)')
        ax3.set_title('Top 5 Concentration Comparison\n(Bubble size = community size)', 
                     fontweight='bold')
        ax3.grid(True, alpha=0.3)
        
        # Concentration difference analysis
        comparison_df['concentration_diff'] = (comparison_df['pub_concentration'] - 
                                             comparison_df['dev_concentration'])
        
        bars4 = ax4.barh(comparison_df['community_name'], 
                        comparison_df['concentration_diff'],
                        color=['red' if x < 0 else 'blue' for x in comparison_df['concentration_diff']],
                        alpha=0.7, edgecolor='black', linewidth=0.5)
        
        ax4.axvline(x=0, color='black', linestyle='-', linewidth=1)
        ax4.set_xlabel('Concentration Difference (Pub - Dev) %')
        ax4.set_title('Publisher vs Developer Dominance', fontweight='bold')
        ax4.text(0.02, 0.98, 'Publisher\nDominant', transform=ax4.transAxes, 
                va='top', ha='left', color='blue', fontweight='bold')
        ax4.text(0.98, 0.98, 'Developer\nDominant', transform=ax4.transAxes, 
                va='top', ha='right', color='red', fontweight='bold')
        
        plt.tight_layout()
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / 'developer_publisher_comparison.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
        
        # Plotly version - Interactive comparison
        fig_plotly = go.Figure()
        
        fig_plotly.add_trace(go.Scatter(
            x=comparison_df['pub_concentration'],
            y=comparison_df['dev_concentration'],
            mode='markers+text',
            marker=dict(
                size=comparison_df['size']/20,
                color=[get_community_color(i) for i in comparison_df['community_id']],
                opacity=0.7,
                line=dict(width=1, color='black'),
                sizemode='diameter'
            ),
            text=comparison_df['community_name'],
            textposition='top center',
            hovertemplate=
                '<b>%{text}</b><br>' +
                'Top Publisher: %{customdata[0]}<br>' +
                'Publisher Concentration: %{x:.1f}%<br>' +
                'Top Developer: %{customdata[1]}<br>' +
                'Developer Concentration: %{y:.1f}%<br>' +
                'Community Size: %{marker.size}<br>' +
                '<extra></extra>',
            customdata=np.column_stack((
                comparison_df['top_publisher'],
                comparison_df['top_developer']
            )),
            name='Communities'
        ))
        
        # Add diagonal line
        max_conc = max(comparison_df['pub_concentration'].max(), 
                      comparison_df['dev_concentration'].max())
        fig_plotly.add_trace(go.Scatter(
            x=[0, max_conc],
            y=[0, max_conc],
            mode='lines',
            line=dict(color='black', dash='dash'),
            name='Equal Concentration',
            showlegend=True
        ))
        
        fig_plotly.update_layout(
            **get_plotly_layout(
                title='Publisher vs Developer Concentration Analysis<br><sub>Bubble size represents community size</sub>',
                xaxis_title='Top Publisher Concentration (%)',
                yaxis_title='Top Developer Concentration (%)',
                height=600
            )
        )
        
        figures['plotly'] = fig_plotly
        
        if save_plots:
            fig_plotly.write_html(self.output_dir / 'publisher_developer_comparison_interactive.html')
        
        return figures
    
    def create_top_publishers_across_communities(self, top_n: int = 10, save_plots: bool = True) -> Dict:
        """
        Create visualization of top publishers across all communities.
        
        Args:
            top_n (int): Number of top publishers to analyze
            save_plots (bool): Whether to save plots to files
            
        Returns:
            Dict: Dictionary containing matplotlib and plotly figures
        """
        # Collect all publishers across communities
        all_publishers = []
        
        for comm_id, comm_name in COMMUNITY_NAMES.items():
            if str(comm_id) not in self.data_loader.detailed_profiles['community_profiles']:
                continue
            
            profile = self.data_loader.detailed_profiles['community_profiles'][str(comm_id)]
            publishers = profile['categorical_features']['publishers']['top_values']
            
            for pub_info in publishers:
                all_publishers.append({
                    'publisher': pub_info['value'],
                    'community_id': comm_id,
                    'community_name': comm_name,
                    'count': pub_info['count'],
                    'percentage': pub_info['percentage']
                })
        
        all_pubs_df = pd.DataFrame(all_publishers)
        
        # Get top publishers by total games
        top_publishers = (all_pubs_df.groupby('publisher')['count']
                         .sum()
                         .sort_values(ascending=False)
                         .head(top_n)
                         .index.tolist())
        
        # Create matrix for heatmap
        publisher_matrix = all_pubs_df[all_pubs_df['publisher'].isin(top_publishers)].pivot_table(
            index='publisher',
            columns='community_name',
            values='percentage',
            fill_value=0
        )
        
        figures = {}
        
        # Matplotlib version
        fig_mpl, (ax1, ax2) = plt.subplots(2, 1, figsize=FIGURE_SIZES['large'])
        
        # Publisher presence heatmap
        sns.heatmap(publisher_matrix, annot=True, fmt='.1f', cmap='YlOrRd',
                   ax=ax1, cbar_kws={'label': 'Percentage in Community'})
        ax1.set_title(f'Top {top_n} Publishers Across Communities', fontweight='bold')
        ax1.set_xlabel('Community')
        ax1.set_ylabel('Publisher')
        
        # Total games by publisher
        publisher_totals = all_pubs_df[all_pubs_df['publisher'].isin(top_publishers)].groupby('publisher')['count'].sum().sort_values(ascending=True)
        
        bars = ax2.barh(range(len(publisher_totals)), publisher_totals.values,
                       color='steelblue', alpha=0.8, edgecolor='black', linewidth=0.5)
        ax2.set_yticks(range(len(publisher_totals)))
        ax2.set_yticklabels([pub[:25] + '...' if len(pub) > 25 else pub 
                            for pub in publisher_totals.index], fontsize=10)
        ax2.set_xlabel('Total Games Across All Communities')
        ax2.set_title(f'Total Game Count - Top {top_n} Publishers', fontweight='bold')
        
        # Add value labels
        for bar, count in zip(bars, publisher_totals.values):
            ax2.text(bar.get_width() + max(publisher_totals.values) * 0.01, 
                    bar.get_y() + bar.get_height()/2,
                    f'{count}', va='center', ha='left', fontsize=9)
        
        plt.tight_layout()
        figures['matplotlib'] = fig_mpl
        
        if save_plots:
            fig_mpl.savefig(self.output_dir / f'top_{top_n}_publishers_analysis.png', 
                           dpi=DPI_SETTINGS['web'], bbox_inches='tight')
        
        # Plotly version - Interactive stacked bar
        publisher_community_data = []
        
        for publisher in top_publishers:
            pub_data = all_pubs_df[all_pubs_df['publisher'] == publisher]
            for _, row in pub_data.iterrows():
                publisher_community_data.append({
                    'publisher': publisher,
                    'community': row['community_name'],
                    'count': row['count'],
                    'percentage': row['percentage']
                })
        
        pub_comm_df = pd.DataFrame(publisher_community_data)
        
        fig_plotly = px.bar(
            pub_comm_df,
            x='publisher',
            y='count',
            color='community',
            title=f'Top {top_n} Publishers - Games Distribution Across Communities',
            labels={'count': 'Number of Games', 'publisher': 'Publisher'},
            color_discrete_map={comm_name: get_community_color(comm_id) 
                               for comm_id, comm_name in COMMUNITY_NAMES.items()}
        )
        
        fig_plotly.update_layout(
            **get_plotly_layout(
                height=600,
                xaxis_tickangle=-45
            )
        )
        
        figures['plotly'] = fig_plotly
        
        if save_plots:
            fig_plotly.write_html(self.output_dir / f'top_{top_n}_publishers_interactive.html')
        
        return figures
    
    def generate_all_publisher_developer_plots(self, save_plots: bool = True) -> Dict:
        """
        Generate all publisher and developer analysis visualizations.
        
        Args:
            save_plots (bool): Whether to save all plots
            
        Returns:
            Dict: All generated figures organized by category
        """
        print("🎨 Generating publisher and developer analysis visualizations...")
        
        all_figures = {}
        
        # Generate each visualization
        print("  📈 Creating publisher concentration analysis...")
        all_figures['publisher_concentration'] = self.create_publisher_concentration_analysis(save_plots=save_plots)
        
        print("  🌐 Creating publisher network analysis...")
        all_figures['publisher_network'] = self.create_publisher_network_analysis(save_plots=save_plots)
        
        print("  ⚖️ Creating developer vs publisher comparison...")
        all_figures['dev_pub_comparison'] = self.create_developer_concentration_comparison(save_plots=save_plots)
        
        print("  🏆 Creating top publishers analysis...")
        all_figures['top_publishers'] = self.create_top_publishers_across_communities(save_plots=save_plots)
        
        print(f"✅ Publisher/developer analysis complete! Saved to: {self.output_dir}")
        return all_figures

# =============================================================================
# COMMAND LINE INTERFACE
# =============================================================================

def main():
    """Command line interface for publisher and developer analysis visualizations."""
    import argparse
    try:
        from .data_loader import load_data
    except ImportError:
        from data_loader import load_data
    
    parser = argparse.ArgumentParser(description='Generate publisher and developer analysis visualizations')
    parser.add_argument('--data-dir', type=str, default=None,
                       help='Directory containing community data files')
    parser.add_argument('--output-dir', type=str, 
                       default='communities_visualizations/outputs/static_plots/publishers_developers',
                       help='Output directory for generated plots')
    parser.add_argument('--top-publishers', type=int, default=10,
                       help='Number of top publishers to analyze (default: 10)')
    parser.add_argument('--min-games', type=int, default=3,
                       help='Minimum games for publisher network inclusion (default: 3)')
    parser.add_argument('--no-save', action='store_true',
                       help='Don\'t save plots to files (display only)')
    
    args = parser.parse_args()
    
    try:
        # Load data
        print("📥 Loading community data...")
        data_loader = load_data(args.data_dir)
        
        # Create analyzer
        analyzer = PublisherDeveloperAnalyzer(data_loader, args.output_dir)
        
        # Generate all plots
        figures = analyzer.generate_all_publisher_developer_plots(save_plots=not args.no_save)
        
        print("🎉 Publisher and developer analysis visualization generation complete!")
        
        # Show summary
        total_plots = sum(len(category_figs) for category_figs in figures.values())
        print(f"Generated {total_plots} visualizations across {len(figures)} categories")
        
    except Exception as e:
        print(f"❌ Error generating visualizations: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()