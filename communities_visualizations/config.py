"""
Configuration file for Steam Communities Visualization Suite.

Contains styling parameters, color schemes, and constants used across all visualization modules.
"""

import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go
from plotly.colors import qualitative
import numpy as np

# =============================================================================
# COMMUNITY COLOR SCHEMES
# =============================================================================

# Primary color palette for 14 communities (colorblind-friendly)
COMMUNITY_COLORS = {
    0: '#1f77b4',   # Blue
    1: '#ff7f0e',   # Orange  
    2: '#2ca02c',   # Green
    3: '#d62728',   # Red
    4: '#9467bd',   # Purple
    5: '#8c564b',   # Brown
    6: '#e377c2',   # Pink
    7: '#7f7f7f',   # Gray
    8: '#bcbd22',   # Olive
    9: '#17becf',   # Cyan
    10: '#aec7e8',  # Light Blue
    11: '#ffbb78',  # Light Orange
    12: '#98df8a',  # Light Green
    13: '#ff9896'   # Light Red
}

# Alternative color schemes
COMMUNITY_COLORS_DARK = {
    0: '#0d47a1', 1: '#e65100', 2: '#1b5e20', 3: '#b71c1c',
    4: '#4a148c', 5: '#3e2723', 6: '#880e4f', 7: '#424242',
    8: '#827717', 9: '#006064', 10: '#0277bd', 11: '#f57c00',
    12: '#388e3c', 13: '#d32f2f'
}

COMMUNITY_COLORS_PASTEL = {
    0: '#aed6f1', 1: '#f8c471', 2: '#a9dfbf', 3: '#f1948a',
    4: '#d2b4de', 5: '#d7bde2', 6: '#f8d7da', 7: '#d5d8dc',
    8: '#f4d03f', 9: '#76d7c4', 10: '#85c1e9', 11: '#f9ca24',
    12: '#6c5ce7', 13: '#fd79a8'
}

# Plotly color scale for continuous data
PLOTLY_COLORS = qualitative.Set3[:14]

# =============================================================================
# PLOT STYLING PARAMETERS
# =============================================================================

# Figure sizes (width, height) in inches
FIGURE_SIZES = {
    'small': (8, 6),
    'medium': (12, 8),
    'large': (16, 10),
    'wide': (20, 8),
    'square': (10, 10),
    'dashboard': (15, 12)
}

# DPI settings
DPI_SETTINGS = {
    'screen': 100,
    'print': 300,
    'web': 150
}

# Font sizes
FONT_SIZES = {
    'title': 16,
    'subtitle': 14,
    'axis_label': 12,
    'tick_label': 10,
    'legend': 11,
    'annotation': 9
}

# Matplotlib style parameters
MATPLOTLIB_STYLE = {
    'figure.facecolor': 'white',
    'axes.facecolor': 'white',
    'axes.edgecolor': 'black',
    'axes.linewidth': 0.8,
    'axes.grid': True,
    'grid.alpha': 0.3,
    'axes.axisbelow': True,
    'legend.frameon': True,
    'legend.fancybox': True,
    'legend.shadow': True,
    'legend.framealpha': 0.9
}

# Seaborn style parameters
SEABORN_STYLE = 'whitegrid'
SEABORN_PALETTE = 'husl'

# =============================================================================
# PLOTLY STYLING PARAMETERS  
# =============================================================================

PLOTLY_LAYOUT = {
    'font': {'family': 'Arial, sans-serif', 'size': 12},
    'title': {'font': {'size': 16}, 'x': 0.5, 'xanchor': 'center'},
    'paper_bgcolor': 'white',
    'plot_bgcolor': 'rgba(0,0,0,0)',
    'showlegend': True,
    'legend': {
        'bgcolor': 'rgba(255,255,255,0.8)',
        'bordercolor': 'rgba(0,0,0,0.2)',
        'borderwidth': 1
    },
    'margin': {'l': 80, 'r': 80, 't': 100, 'b': 80}
}

PLOTLY_AXIS = {
    'showgrid': True,
    'gridwidth': 1,
    'gridcolor': 'rgba(128,128,128,0.2)',
    'showline': True,
    'linewidth': 1,
    'linecolor': 'black',
    'mirror': True
}

# =============================================================================
# DATA FILE PATHS
# =============================================================================

# Default input data paths (relative to project root)
DEFAULT_DATA_PATHS = {
    'community_profiles': 'community_14_profiles_analysis/community_average_profiles.csv',
    'detailed_profiles': 'community_14_profiles_analysis/detailed_community_profiles.json',
    'community_summary': 'louvain_14_communities_summary.csv',
    'overall_profile': 'community_14_profiles_analysis/overall_average_profile.csv'
}

# Output directories
OUTPUT_PATHS = {
    'static_plots': 'communities_visualizations/outputs/static_plots',
    'interactive_plots': 'communities_visualizations/outputs/interactive_plots',
    'data_exports': 'communities_visualizations/outputs/data_exports'
}

# =============================================================================
# VISUALIZATION SPECIFIC PARAMETERS
# =============================================================================

# Heatmap parameters
HEATMAP_PARAMS = {
    'cmap': 'viridis',
    'annot': True,
    'fmt': '.2f',
    'square': False,
    'linewidths': 0.5,
    'cbar_kws': {'shrink': 0.8}
}

# Network visualization parameters
NETWORK_PARAMS = {
    'node_size': 100,
    'node_alpha': 0.8,
    'edge_alpha': 0.6,
    'font_size': 8,
    'with_labels': True,
    'layout': 'spring'
}

# Box plot parameters
BOXPLOT_PARAMS = {
    'whis': 1.5,
    'showfliers': True,
    'patch_artist': True,
    'boxprops': {'alpha': 0.7},
    'whiskerprops': {'alpha': 0.7},
    'capprops': {'alpha': 0.7}
}

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def setup_matplotlib_style():
    """Apply custom matplotlib styling."""
    plt.rcParams.update(MATPLOTLIB_STYLE)
    for param, value in FONT_SIZES.items():
        if param == 'title':
            plt.rcParams['axes.titlesize'] = value
        elif param == 'axis_label':
            plt.rcParams['axes.labelsize'] = value
        elif param == 'tick_label':
            plt.rcParams['xtick.labelsize'] = value
            plt.rcParams['ytick.labelsize'] = value
        elif param == 'legend':
            plt.rcParams['legend.fontsize'] = value

def setup_seaborn_style():
    """Apply custom seaborn styling."""
    sns.set_style(SEABORN_STYLE)
    sns.set_palette(SEABORN_PALETTE)

def get_community_color(community_id, style='default'):
    """
    Get color for a specific community.
    
    Args:
        community_id (int): Community ID (0-13)
        style (str): Color style ('default', 'dark', 'pastel')
    
    Returns:
        str: Hex color code
    """
    color_map = {
        'default': COMMUNITY_COLORS,
        'dark': COMMUNITY_COLORS_DARK,
        'pastel': COMMUNITY_COLORS_PASTEL
    }
    
    return color_map.get(style, COMMUNITY_COLORS).get(community_id, '#000000')

def get_plotly_layout(**kwargs):
    """
    Get customized plotly layout.
    
    Args:
        **kwargs: Additional layout parameters to override defaults
        
    Returns:
        dict: Plotly layout dictionary
    """
    layout = PLOTLY_LAYOUT.copy()
    layout.update(kwargs)
    return layout

def get_plotly_axis(**kwargs):
    """
    Get customized plotly axis parameters.
    
    Args:
        **kwargs: Additional axis parameters to override defaults
        
    Returns:
        dict: Plotly axis dictionary
    """
    axis = PLOTLY_AXIS.copy()
    axis.update(kwargs)
    return axis

# Apply default styling on import
setup_matplotlib_style()
setup_seaborn_style()

# =============================================================================
# COMMUNITY METADATA
# =============================================================================

# Community names/descriptions based on their characteristics
COMMUNITY_NAMES = {
    0: "Indie Casual",
    1: "Indie Adventure", 
    2: "Indie Puzzle",
    3: "Indie Story",
    4: "Indie Simulation",
    5: "Indie Strategy", 
    6: "Indie Horror",
    7: "Indie Action",
    8: "Indie RPG",
    9: "Action Arcade",
    10: "Visual Novels",
    11: "AAA Action",
    12: "Sports & Racing", 
    13: "Children's Games"
}

# Community descriptions for tooltips and legends
COMMUNITY_DESCRIPTIONS = {
    0: "Casual indie games with broad appeal",
    1: "Story-driven indie adventure games",
    2: "Logic and puzzle-focused indie titles", 
    3: "Narrative-heavy indie experiences",
    4: "Indie simulation and management games",
    5: "Indie strategy and tactical games",
    6: "Horror and thriller indie games",
    7: "Action-oriented indie titles",
    8: "Indie RPG and character progression games",
    9: "Fast-paced arcade action games",
    10: "Interactive novels and story games",
    11: "High-budget action games",
    12: "Sports simulation and racing games",
    13: "Family-friendly and educational games"
}