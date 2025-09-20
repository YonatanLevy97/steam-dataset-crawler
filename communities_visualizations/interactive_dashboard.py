"""
Interactive Dashboard for Steam Communities Analysis

Creates a comprehensive multi-tab Dash application for exploring
Steam game community data with interactive visualizations.
"""

import dash
from dash import dcc, html, Input, Output, callback, dash_table
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional
import warnings
warnings.filterwarnings('ignore')

try:
    from .config import (
        COMMUNITY_COLORS, COMMUNITY_NAMES, COMMUNITY_DESCRIPTIONS,
        get_community_color, get_plotly_layout
    )
    from .data_loader import CommunityDataLoader
except ImportError:
    from config import (
        COMMUNITY_COLORS, COMMUNITY_NAMES, COMMUNITY_DESCRIPTIONS,
        get_community_color, get_plotly_layout
    )
    from data_loader import CommunityDataLoader

class CommunityDashboard:
    """
    Interactive dashboard for Steam community analysis.
    """
    
    def __init__(self, data_loader: CommunityDataLoader, port: int = 8050):
        """
        Initialize the dashboard.
        
        Args:
            data_loader (CommunityDataLoader): Loaded community data
            port (int): Port to run the dashboard on
        """
        self.data_loader = data_loader
        self.port = port
        
        # Load all required data
        if data_loader.community_profiles is None:
            data_loader.load_community_profiles()
        if data_loader.detailed_profiles is None:
            data_loader.load_detailed_profiles()
        
        # Initialize Dash app
        self.app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
        self.app.title = "Steam Communities Analysis Dashboard"
        
        # Set up layout
        self.setup_layout()
        self.setup_callbacks()
    
    def setup_layout(self):
        """Set up the dashboard layout with multiple tabs."""
        
        # Header
        header = dbc.Row([
            dbc.Col([
                html.H1("🎮 Steam Communities Analysis Dashboard", 
                       className="text-center mb-3 text-primary"),
                html.P("Interactive exploration of Steam game communities detected by Louvain clustering",
                      className="text-center lead text-muted mb-4"),
                html.Hr()
            ])
        ])
        
        # Control panel
        controls = dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("🎛️ Analysis Controls"),
                    dbc.CardBody([
                        dbc.Row([
                            dbc.Col([
                                html.Label("Communities to Compare:", className="fw-bold"),
                                dcc.Dropdown(
                                    id='community-selector',
                                    options=[{'label': f"{name} (ID: {cid})", 'value': cid} 
                                           for cid, name in COMMUNITY_NAMES.items()],
                                    value=list(COMMUNITY_NAMES.keys())[:5],
                                    multi=True
                                )
                            ], width=6),
                            dbc.Col([
                                html.Label("Analysis Method:", className="fw-bold"),
                                dcc.Dropdown(
                                    id='analysis-method',
                                    options=[
                                        {'label': 'Community Overview', 'value': 'overview'},
                                        {'label': 'Genre Analysis', 'value': 'genres'},
                                        {'label': 'Publisher Analysis', 'value': 'publishers'},
                                        {'label': 'Technical Features', 'value': 'technical'},
                                        {'label': 'Similarity Analysis', 'value': 'similarity'}
                                    ],
                                    value='overview'
                                )
                            ], width=3),
                            dbc.Col([
                                html.Label("Visualization Type:", className="fw-bold"),
                                dcc.Dropdown(
                                    id='viz-type',
                                    options=[
                                        {'label': 'Scatter Plot', 'value': 'scatter'},
                                        {'label': 'Bar Chart', 'value': 'bar'},
                                        {'label': 'Heatmap', 'value': 'heatmap'},
                                        {'label': 'Box Plot', 'value': 'box'},
                                        {'label': '3D Scatter', 'value': '3d'}
                                    ],
                                    value='scatter'
                                )
                            ], width=3)
                        ])
                    ])
                ])
            ])
        ], className="mb-4")
        
        # Main content tabs
        tabs = dbc.Tabs([
            dbc.Tab(label="🏠 Overview", tab_id="overview-tab"),
            dbc.Tab(label="📊 Community Comparison", tab_id="comparison-tab"),
            dbc.Tab(label="🎯 Genre Analysis", tab_id="genre-tab"),
            dbc.Tab(label="🏢 Publishers & Developers", tab_id="publisher-tab"),
            dbc.Tab(label="⚙️ Technical Features", tab_id="technical-tab"),
            dbc.Tab(label="🔍 Similarity Analysis", tab_id="similarity-tab"),
            dbc.Tab(label="📈 Data Explorer", tab_id="explorer-tab")
        ], id="main-tabs", active_tab="overview-tab", className="mb-4")
        
        # Content area
        content = html.Div(id="tab-content", className="min-vh-75")
        
        # Footer
        footer = html.Div([
            html.Hr(),
            dbc.Row([
                dbc.Col([
                    html.P("Steam Communities Analysis Dashboard | Powered by Plotly Dash", 
                          className="text-center text-muted small")
                ])
            ])
        ], className="mt-5")
        
        # Complete layout
        self.app.layout = dbc.Container([
            header,
            controls,
            tabs,
            content,
            footer
        ], fluid=True)
    
    def setup_callbacks(self):
        """Set up dashboard callbacks for interactivity."""
        
        @self.app.callback(
            Output('tab-content', 'children'),
            [Input('main-tabs', 'active_tab'),
             Input('community-selector', 'value'),
             Input('analysis-method', 'value'),
             Input('viz-type', 'value')]
        )
        def render_tab_content(active_tab, selected_communities, analysis_method, viz_type):
            """Render content based on active tab and selections."""
            
            if active_tab == "overview-tab":
                return self.create_overview_tab()
            elif active_tab == "comparison-tab":
                return self.create_comparison_tab(selected_communities, viz_type)
            elif active_tab == "genre-tab":
                return self.create_genre_tab(selected_communities)
            elif active_tab == "publisher-tab":
                return self.create_publisher_tab(selected_communities)
            elif active_tab == "technical-tab":
                return self.create_technical_tab(selected_communities)
            elif active_tab == "similarity-tab":
                return self.create_similarity_tab()
            elif active_tab == "explorer-tab":
                return self.create_explorer_tab()
            
            return html.Div("Select a tab to view content.")
    
    def create_overview_tab(self):
        """Create overview tab content."""
        df = self.data_loader.community_profiles
        
        # Key statistics cards
        stats_cards = dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4(f"{len(df)}", className="card-title text-primary"),
                        html.P("Total Communities", className="card-text")
                    ])
                ])
            ], width=2),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4(f"{df['size'].sum():,}", className="card-title text-success"),
                        html.P("Total Games", className="card-text")
                    ])
                ])
            ], width=2),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4(f"${df['average_price'].mean():.1f}", className="card-title text-warning"),
                        html.P("Average Price", className="card-text")
                    ])
                ])
            ], width=2),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4(f"{df['metacritic_score_mean'].mean():.1f}", className="card-title text-info"),
                        html.P("Average Rating", className="card-text")
                    ])
                ])
            ], width=2),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4(f"{df['release_year_mean'].mean():.0f}", className="card-title text-secondary"),
                        html.P("Average Release Year", className="card-text")
                    ])
                ])
            ], width=2),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4(f"{df['windows_true_percentage'].mean():.1f}%", className="card-title text-dark"),
                        html.P("Windows Support", className="card-text")
                    ])
                ])
            ], width=2)
        ], className="mb-4")
        
        # Community size visualization
        size_fig = go.Figure(data=go.Bar(
            x=[COMMUNITY_NAMES[i] for i in df['community_id']],
            y=df['size'],
            marker_color=[get_community_color(i) for i in df['community_id']],
            hovertemplate='<b>%{x}</b><br>Games: %{y:,}<extra></extra>'
        ))
        size_fig.update_layout(
            title="Community Sizes",
            xaxis_title="Community",
            yaxis_title="Number of Games",
            showlegend=False
        )
        
        # Price vs Quality scatter
        quality_fig = go.Figure(data=go.Scatter(
            x=df['average_price'],
            y=df['metacritic_score_mean'],
            mode='markers+text',
            marker=dict(
                size=df['size']/20,
                color=[get_community_color(i) for i in df['community_id']],
                opacity=0.7,
                line=dict(width=1, color='black')
            ),
            text=[COMMUNITY_NAMES[i] for i in df['community_id']],
            textposition='top center',
            hovertemplate='<b>%{text}</b><br>Price: $%{x:.2f}<br>Rating: %{y:.1f}<extra></extra>'
        ))
        quality_fig.update_layout(
            title="Price vs Quality Analysis",
            xaxis_title="Average Price ($)",
            yaxis_title="Average Metacritic Score",
            showlegend=False
        )
        
        # Community descriptions table
        descriptions_data = []
        for cid, name in COMMUNITY_NAMES.items():
            if cid < len(df):
                row = df[df['community_id'] == cid].iloc[0]
                descriptions_data.append({
                    'Community': name,
                    'Description': COMMUNITY_DESCRIPTIONS.get(cid, 'N/A'),
                    'Size': f"{row['size']:,}",
                    'Top Genre': row.get('genres_most_common', 'N/A'),
                    'Avg Price': f"${row['average_price']:.2f}",
                    'Rating': f"{row['metacritic_score_mean']:.1f}"
                })
        
        descriptions_table = dash_table.DataTable(
            data=descriptions_data,
            columns=[{"name": col, "id": col} for col in descriptions_data[0].keys()],
            style_cell={'textAlign': 'left', 'padding': '10px'},
            style_header={'backgroundColor': 'rgb(230, 230, 230)', 'fontWeight': 'bold'},
            style_data_conditional=[
                {
                    'if': {'row_index': i},
                    'backgroundColor': get_community_color(i) + '20'
                } for i in range(len(descriptions_data))
            ]
        )
        
        return html.Div([
            stats_cards,
            dbc.Row([
                dbc.Col([
                    dcc.Graph(figure=size_fig)
                ], width=6),
                dbc.Col([
                    dcc.Graph(figure=quality_fig)
                ], width=6)
            ], className="mb-4"),
            dbc.Row([
                dbc.Col([
                    html.H4("Community Descriptions", className="mb-3"),
                    descriptions_table
                ])
            ])
        ])
    
    def create_comparison_tab(self, selected_communities, viz_type):
        """Create community comparison tab content."""
        if not selected_communities:
            return html.Div("Please select communities to compare.", className="alert alert-warning")
        
        df = self.data_loader.community_profiles
        df_filtered = df[df['community_id'].isin(selected_communities)]
        
        # Comparison metrics
        metrics = ['size', 'average_price', 'metacritic_score_mean', 'release_year_mean', 
                  'windows_true_percentage', 'has_dlc_true_percentage']
        metric_names = ['Community Size', 'Average Price', 'Metacritic Score', 'Release Year',
                       'Windows Support %', 'Has DLC %']
        
        if viz_type == 'bar':
            # Bar chart comparison
            fig = make_subplots(rows=2, cols=3, subplot_titles=metric_names)
            
            for i, (metric, name) in enumerate(zip(metrics, metric_names)):
                row = i // 3 + 1
                col = i % 3 + 1
                
                fig.add_trace(
                    go.Bar(
                        x=[COMMUNITY_NAMES[cid] for cid in df_filtered['community_id']],
                        y=df_filtered[metric],
                        marker_color=[get_community_color(cid) for cid in df_filtered['community_id']],
                        name=name,
                        showlegend=False
                    ),
                    row=row, col=col
                )
            
            fig.update_layout(height=800, title_text="Community Comparison - Multiple Metrics")
            
        elif viz_type == 'scatter':
            # Interactive scatter plot
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=df_filtered['average_price'],
                y=df_filtered['metacritic_score_mean'],
                mode='markers+text',
                marker=dict(
                    size=df_filtered['size']/15,
                    color=[get_community_color(cid) for cid in df_filtered['community_id']],
                    opacity=0.7,
                    line=dict(width=1, color='black')
                ),
                text=[COMMUNITY_NAMES[cid] for cid in df_filtered['community_id']],
                textposition='top center',
                hovertemplate='<b>%{text}</b><br>Price: $%{x:.2f}<br>Rating: %{y:.1f}<br>Size: %{marker.size}<extra></extra>'
            ))
            
            fig.update_layout(
                title="Community Comparison - Price vs Quality",
                xaxis_title="Average Price ($)",
                yaxis_title="Average Metacritic Score"
            )
            
        elif viz_type == 'heatmap':
            # Correlation heatmap
            correlation_matrix = df_filtered[metrics].corr()
            
            fig = go.Figure(data=go.Heatmap(
                z=correlation_matrix.values,
                x=metric_names,
                y=metric_names,
                colorscale='RdBu',
                zmid=0,
                hoverongaps=False,
                text=np.round(correlation_matrix.values, 3),
                texttemplate='%{text}'
            ))
            
            fig.update_layout(
                title="Feature Correlation Heatmap",
                height=600
            )
        
        else:  # Default to scatter
            fig = go.Figure(data=go.Scatter(
                x=df_filtered['size'],
                y=df_filtered['average_price'],
                mode='markers+text',
                marker=dict(
                    size=12,
                    color=[get_community_color(cid) for cid in df_filtered['community_id']],
                    opacity=0.7
                ),
                text=[COMMUNITY_NAMES[cid] for cid in df_filtered['community_id']],
                textposition='top center'
            ))
            fig.update_layout(title="Community Size vs Price", xaxis_title="Size", yaxis_title="Price")
        
        return dbc.Row([
            dbc.Col([
                dcc.Graph(figure=fig)
            ])
        ])
    
    def create_genre_tab(self, selected_communities):
        """Create genre analysis tab content."""
        if not selected_communities:
            selected_communities = list(COMMUNITY_NAMES.keys())[:5]
        
        # Get genre data
        genre_data = []
        for comm_id in selected_communities:
            if str(comm_id) in self.data_loader.detailed_profiles['community_profiles']:
                profile = self.data_loader.detailed_profiles['community_profiles'][str(comm_id)]
                genres = profile['categorical_features']['genres']['top_values'][:5]
                for genre in genres:
                    genre_data.append({
                        'community': COMMUNITY_NAMES[comm_id],
                        'genre': genre['value'],
                        'percentage': genre['percentage'],
                        'count': genre['count']
                    })
        
        if not genre_data:
            return html.Div("No genre data available for selected communities.", className="alert alert-warning")
        
        genre_df = pd.DataFrame(genre_data)
        
        # Genre distribution stacked bar
        fig_stacked = px.bar(
            genre_df,
            x='community',
            y='percentage',
            color='genre',
            title='Genre Distribution Across Communities',
            labels={'percentage': 'Percentage', 'community': 'Community'},
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        
        # Genre popularity across communities
        genre_popularity = genre_df.groupby('genre')['percentage'].mean().sort_values(ascending=False)
        
        fig_popularity = go.Figure(data=go.Bar(
            x=genre_popularity.index,
            y=genre_popularity.values,
            marker_color='lightblue'
        ))
        fig_popularity.update_layout(
            title="Average Genre Popularity Across Selected Communities",
            xaxis_title="Genre",
            yaxis_title="Average Percentage"
        )
        
        return html.Div([
            dbc.Row([
                dbc.Col([
                    dcc.Graph(figure=fig_stacked)
                ], width=6),
                dbc.Col([
                    dcc.Graph(figure=fig_popularity)
                ], width=6)
            ])
        ])
    
    def create_publisher_tab(self, selected_communities):
        """Create publisher analysis tab content."""
        if not selected_communities:
            selected_communities = list(COMMUNITY_NAMES.keys())[:5]
        
        # Get publisher data
        publisher_data = []
        for comm_id in selected_communities:
            if str(comm_id) in self.data_loader.detailed_profiles['community_profiles']:
                profile = self.data_loader.detailed_profiles['community_profiles'][str(comm_id)]
                publishers = profile['categorical_features']['publishers']['top_values'][:5]
                for pub in publishers:
                    publisher_data.append({
                        'community': COMMUNITY_NAMES[comm_id],
                        'publisher': pub['value'],
                        'percentage': pub['percentage'],
                        'count': pub['count']
                    })
        
        if not publisher_data:
            return html.Div("No publisher data available for selected communities.", className="alert alert-warning")
        
        pub_df = pd.DataFrame(publisher_data)
        
        # Publisher concentration analysis
        df = self.data_loader.community_profiles
        df_filtered = df[df['community_id'].isin(selected_communities)]
        
        concentration_fig = go.Figure(data=go.Scatter(
            x=[COMMUNITY_NAMES[cid] for cid in df_filtered['community_id']],
            y=df_filtered['publishers_top_percentage'],
            mode='markers+lines',
            marker=dict(
                size=df_filtered['size']/20,
                color=[get_community_color(cid) for cid in df_filtered['community_id']],
                opacity=0.7
            ),
            line=dict(color='gray', dash='dash', width=1),
            hovertemplate='<b>%{x}</b><br>Top Publisher Dominance: %{y:.1f}%<extra></extra>'
        ))
        concentration_fig.update_layout(
            title="Publisher Concentration by Community",
            xaxis_title="Community",
            yaxis_title="Top Publisher Percentage"
        )
        
        # Top publishers across communities
        top_publishers = pub_df.groupby('publisher')['count'].sum().sort_values(ascending=False).head(10)
        
        top_pub_fig = go.Figure(data=go.Bar(
            x=top_publishers.values,
            y=top_publishers.index,
            orientation='h',
            marker_color='lightcoral'
        ))
        top_pub_fig.update_layout(
            title="Top Publishers Across Selected Communities",
            xaxis_title="Total Games",
            yaxis_title="Publisher"
        )
        
        return html.Div([
            dbc.Row([
                dbc.Col([
                    dcc.Graph(figure=concentration_fig)
                ], width=6),
                dbc.Col([
                    dcc.Graph(figure=top_pub_fig)
                ], width=6)
            ])
        ])
    
    def create_technical_tab(self, selected_communities):
        """Create technical features tab content."""
        if not selected_communities:
            selected_communities = list(COMMUNITY_NAMES.keys())
        
        df = self.data_loader.community_profiles
        df_filtered = df[df['community_id'].isin(selected_communities)]
        
        # Platform support radar chart
        platforms = ['windows_true_percentage', 'mac_true_percentage', 'linux_true_percentage']
        platform_names = ['Windows', 'Mac', 'Linux']
        
        radar_fig = go.Figure()
        
        for _, row in df_filtered.iterrows():
            radar_fig.add_trace(go.Scatterpolar(
                r=[row[platform] for platform in platforms],
                theta=platform_names,
                fill='toself',
                name=COMMUNITY_NAMES[row['community_id']],
                line_color=get_community_color(row['community_id'])
            ))
        
        radar_fig.update_layout(
            polar=dict(
                radialaxis=dict(
                    visible=True,
                    range=[0, 100]
                )
            ),
            title="Platform Support Comparison"
        )
        
        # DLC vs Achievements
        dlc_achieve_fig = go.Figure(data=go.Scatter(
            x=df_filtered['dlc_count_mean'],
            y=df_filtered['achievements_total_mean'],
            mode='markers+text',
            marker=dict(
                size=df_filtered['size']/15,
                color=[get_community_color(cid) for cid in df_filtered['community_id']],
                opacity=0.7
            ),
            text=[COMMUNITY_NAMES[cid] for cid in df_filtered['community_id']],
            textposition='top center',
            hovertemplate='<b>%{text}</b><br>Avg DLC: %{x:.2f}<br>Avg Achievements: %{y:.1f}<extra></extra>'
        ))
        dlc_achieve_fig.update_layout(
            title="DLC vs Achievements Analysis",
            xaxis_title="Average DLC Count",
            yaxis_title="Average Achievements"
        )
        
        # Free games percentage
        free_games_fig = go.Figure(data=go.Bar(
            x=[COMMUNITY_NAMES[cid] for cid in df_filtered['community_id']],
            y=df_filtered['is_free_true_percentage'],
            marker_color=[get_community_color(cid) for cid in df_filtered['community_id']],
            hovertemplate='<b>%{x}</b><br>Free Games: %{y:.1f}%<extra></extra>'
        ))
        free_games_fig.update_layout(
            title="Free Games Percentage by Community",
            xaxis_title="Community",
            yaxis_title="Free Games Percentage"
        )
        
        return html.Div([
            dbc.Row([
                dbc.Col([
                    dcc.Graph(figure=radar_fig)
                ], width=6),
                dbc.Col([
                    dcc.Graph(figure=dlc_achieve_fig)
                ], width=6)
            ]),
            dbc.Row([
                dbc.Col([
                    dcc.Graph(figure=free_games_fig)
                ], width=12)
            ])
        ])
    
    def create_similarity_tab(self):
        """Create similarity analysis tab content."""
        similarity_matrix = self.data_loader.get_similarity_matrix()
        
        # Similarity heatmap
        heatmap_fig = go.Figure(data=go.Heatmap(
            z=similarity_matrix.values,
            x=similarity_matrix.columns,
            y=similarity_matrix.index,
            colorscale='RdBu',
            zmid=0,
            hoverongaps=False,
            text=np.round(similarity_matrix.values, 3),
            texttemplate='%{text}'
        ))
        heatmap_fig.update_layout(
            title="Community Similarity Matrix (Cosine Similarity)",
            height=600
        )
        
        # Most similar pairs
        similarity_pairs = []
        for i in range(len(similarity_matrix)):
            for j in range(i+1, len(similarity_matrix)):
                similarity_pairs.append({
                    'Community 1': similarity_matrix.index[i],
                    'Community 2': similarity_matrix.index[j],
                    'Similarity': similarity_matrix.iloc[i, j]
                })
        
        pairs_df = pd.DataFrame(similarity_pairs).sort_values('Similarity', ascending=False)
        
        # Top similar pairs table
        similar_pairs_table = dash_table.DataTable(
            data=pairs_df.head(10).to_dict('records'),
            columns=[{"name": col, "id": col, "type": "numeric", "format": {"specifier": ".3f"}} 
                    if col == "Similarity" else {"name": col, "id": col} 
                    for col in pairs_df.columns],
            style_cell={'textAlign': 'left', 'padding': '10px'},
            style_header={'backgroundColor': 'rgb(230, 230, 230)', 'fontWeight': 'bold'},
            style_data_conditional=[
                {
                    'if': {'filter_query': '{Similarity} > 0.8'},
                    'backgroundColor': 'lightgreen',
                    'color': 'black'
                },
                {
                    'if': {'filter_query': '{Similarity} < 0.3'},
                    'backgroundColor': 'lightcoral',
                    'color': 'black'
                }
            ]
        )
        
        return html.Div([
            dbc.Row([
                dbc.Col([
                    dcc.Graph(figure=heatmap_fig)
                ], width=8),
                dbc.Col([
                    html.H5("Most Similar Community Pairs", className="mb-3"),
                    similar_pairs_table
                ], width=4)
            ])
        ])
    
    def create_explorer_tab(self):
        """Create data explorer tab content."""
        df = self.data_loader.community_profiles
        
        # Data table with all community information
        display_columns = [
            'community_name', 'size', 'average_price', 'metacritic_score_mean',
            'release_year_mean', 'windows_true_percentage', 'mac_true_percentage',
            'linux_true_percentage', 'has_dlc_true_percentage', 'is_free_true_percentage'
        ]
        
        column_names = {
            'community_name': 'Community',
            'size': 'Size',
            'average_price': 'Avg Price',
            'metacritic_score_mean': 'Rating',
            'release_year_mean': 'Release Year',
            'windows_true_percentage': 'Windows %',
            'mac_true_percentage': 'Mac %', 
            'linux_true_percentage': 'Linux %',
            'has_dlc_true_percentage': 'Has DLC %',
            'is_free_true_percentage': 'Free Games %'
        }
        
        data_table = dash_table.DataTable(
            data=df[display_columns].to_dict('records'),
            columns=[
                {"name": column_names.get(col, col), "id": col, 
                 "type": "numeric" if col != 'community_name' else "text",
                 "format": {"specifier": ".1f"} if col.endswith('_percentage') or col in ['average_price', 'metacritic_score_mean', 'release_year_mean'] else {}}
                for col in display_columns
            ],
            style_cell={'textAlign': 'left', 'padding': '10px'},
            style_header={'backgroundColor': 'rgb(230, 230, 230)', 'fontWeight': 'bold'},
            style_data={'backgroundColor': 'rgb(248, 248, 248)'},
            sort_action="native",
            filter_action="native",
            page_action="native",
            page_current=0,
            page_size=10,
            export_format="csv"
        )
        
        # Summary statistics
        summary_stats = html.Div([
            html.H5("Dataset Summary Statistics", className="mb-3"),
            html.Pre(df.describe().round(2).to_string(), 
                    style={'backgroundColor': '#f8f9fa', 'padding': '15px', 'fontSize': '12px'})
        ])
        
        return html.Div([
            dbc.Row([
                dbc.Col([
                    html.H4("Community Data Explorer", className="mb-3"),
                    data_table
                ])
            ], className="mb-4"),
            dbc.Row([
                dbc.Col([
                    summary_stats
                ])
            ])
        ])
    
    def run_server(self, debug: bool = True, host: str = '127.0.0.1'):
        """Run the dashboard server."""
        print(f"🚀 Starting Steam Communities Dashboard...")
        print(f"📊 Dashboard will be available at: http://{host}:{self.port}")
        print(f"📈 Loaded data for {len(self.data_loader.community_profiles)} communities")
        print(f"🎯 Interactive exploration ready!")
        
        self.app.run_server(debug=debug, host=host, port=self.port)

# =============================================================================
# STANDALONE DASHBOARD LAUNCHER
# =============================================================================

def create_dashboard(data_dir: str = None, port: int = 8050) -> CommunityDashboard:
    """
    Create and return a dashboard instance.
    
    Args:
        data_dir (str, optional): Directory containing community data files
        port (int): Port to run dashboard on
        
    Returns:
        CommunityDashboard: Dashboard instance
    """
    try:
        from .data_loader import load_data
    except ImportError:
        from data_loader import load_data
    
    print("📥 Loading community data for dashboard...")
    data_loader = load_data(data_dir)
    
    print("🏗️ Creating interactive dashboard...")
    dashboard = CommunityDashboard(data_loader, port=port)
    
    return dashboard

def main():
    """Command line interface for launching the dashboard."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Launch Steam Communities Interactive Dashboard')
    parser.add_argument('--data-dir', type=str, default=None,
                       help='Directory containing community data files')
    parser.add_argument('--port', type=int, default=8050,
                       help='Port to run dashboard on (default: 8050)')
    parser.add_argument('--host', type=str, default='127.0.0.1',
                       help='Host to run dashboard on (default: 127.0.0.1)')
    parser.add_argument('--no-debug', action='store_true',
                       help='Disable debug mode')
    
    args = parser.parse_args()
    
    try:
        # Create and run dashboard
        dashboard = create_dashboard(args.data_dir, args.port)
        dashboard.run_server(debug=not args.no_debug, host=args.host)
        
    except KeyboardInterrupt:
        print("\n👋 Dashboard stopped by user")
    except Exception as e:
        print(f"❌ Error running dashboard: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()