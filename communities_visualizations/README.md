# Steam Communities Visualization Suite

A comprehensive visualization toolkit for analyzing Steam game communities detected through the Louvain clustering algorithm. This suite creates interactive and static visualizations across multiple analysis dimensions including community composition, genre patterns, publisher networks, temporal trends, technical features, and similarity analysis.

## 🎯 Overview

This visualization suite transforms raw community data into insightful, publication-ready visualizations that reveal patterns and relationships within Steam's gaming ecosystem. The toolkit supports both exploratory data analysis and presentation-ready outputs.

### Key Features

- **📊 Comprehensive Analysis**: 6 major analysis categories with 15+ visualization types
- **🎨 Dual Output**: Both static (PNG/PDF) and interactive (HTML) visualizations
- **🚀 Interactive Dashboard**: Multi-tab Dash application for real-time exploration
- **📈 Scalable Architecture**: Modular design supports easy extension and customization
- **🎛️ Flexible Configuration**: Extensive customization options for colors, styles, and outputs
- **📋 Detailed Reporting**: Automated generation reports with performance metrics

## 📁 Project Structure

```
communities_visualizations/
├── __init__.py                         # Package initialization
├── config.py                          # Configuration and styling constants
├── data_loader.py                     # Data loading and preprocessing
├── community_overview.py              # Community size, composition, pricing analysis
├── genre_category_analysis.py         # Genre distributions and diversity analysis
├── publisher_developer_analysis.py    # Publisher/developer network analysis
├── temporal_rating_analysis.py        # Timeline and review score analysis
├── technical_features_analysis.py     # Language support and technical features
├── similarity_analysis.py             # Community similarity and clustering
├── interactive_dashboard.py           # Multi-tab interactive dashboard
├── generate_all_plots.py             # Master orchestration script
├── requirements.txt                   # Python dependencies
├── README.md                          # This documentation
└── outputs/                           # Generated visualizations and reports
    ├── static_plots/                  # PNG/PDF static visualizations
    │   ├── community_overview/
    │   ├── genres_categories/
    │   ├── publishers_developers/
    │   ├── temporal_ratings/
    │   ├── technical_features/
    │   └── similarity_analysis/
    ├── interactive_plots/             # HTML interactive visualizations
    ├── data_exports/                  # Processed data exports (CSV)
    ├── generation_report.json         # Detailed generation report
    └── generation_report.md          # Human-readable report
```

## 🚀 Quick Start

### Installation

1. **Install Dependencies**
```bash
cd communities_visualizations
pip install -r requirements.txt
```

2. **Verify Data Files**
Ensure your community data files are available:
- `community_14_profiles_analysis/community_average_profiles.csv`
- `community_14_profiles_analysis/detailed_community_profiles.json`
- `louvain_14_communities_summary.csv` (optional)

3. **Generate All Visualizations**
```bash
# Generate all visualization categories
python generate_all_plots.py

# Generate specific categories only
python generate_all_plots.py --categories overview genres

# Specify custom data and output directories
python generate_all_plots.py --data-dir /path/to/data --output-dir /path/to/output
```

4. **Launch Interactive Dashboard**
```bash
# Launch dashboard on default port 8050
python interactive_dashboard.py

# Launch on custom port with external access
python interactive_dashboard.py --port 8080 --host 0.0.0.0
```

### Quick Validation

```bash
# Validate setup without generating plots
python generate_all_plots.py --validate-only
```

## 📊 Analysis Categories

### 1. Community Overview (`community_overview.py`)

**Purpose**: Fundamental community characteristics and high-level comparisons

**Visualizations**:
- Community size distribution (bar chart + pie chart)
- Genre composition by community (pie chart grid + sunburst)
- Platform support matrix (heatmap + stacked bars)
- Price distribution analysis (box plots + scatter)
- Summary dashboard with key metrics

**Key Insights**:
- Community size patterns and outliers
- Genre diversity across communities
- Platform compatibility trends
- Pricing strategies by community type

**Example Usage**:
```python
from communities_visualizations.community_overview import CommunityOverviewVisualizer
from communities_visualizations.data_loader import load_data

data_loader = load_data('path/to/data')
visualizer = CommunityOverviewVisualizer(data_loader, 'output/path')
figures = visualizer.generate_all_overview_plots()
```

### 2. Genre & Category Analysis (`genre_category_analysis.py`)

**Purpose**: Deep analysis of genre patterns, diversity, and evolution

**Visualizations**:
- Genre distribution heatmap with hierarchical clustering
- Shannon & Simpson diversity indices analysis
- Top genres comparison across communities
- Category clustering using PCA and K-means
- Genre evolution timeline by release year

**Key Insights**:
- Genre specialization vs. diversification patterns
- Community formation around specific genres
- Temporal evolution of genre preferences
- Genre-based community clustering

**Example Usage**:
```python
from communities_visualizations.genre_category_analysis import GenreCategoryAnalyzer

analyzer = GenreCategoryAnalyzer(data_loader, 'output/path')
figures = analyzer.generate_all_genre_category_plots()
```

### 3. Publisher & Developer Analysis (`publisher_developer_analysis.py`)

**Purpose**: Publisher concentration, network effects, and market dynamics

**Visualizations**:
- Publisher concentration analysis (HHI index)
- Cross-community publisher networks
- Developer vs. publisher dominance comparison
- Top publishers distribution analysis

**Key Insights**:
- Market concentration patterns
- Publisher strategies across communities
- Cross-community publisher presence
- Developer-publisher relationship dynamics

**Example Usage**:
```python
from communities_visualizations.publisher_developer_analysis import PublisherDeveloperAnalyzer

analyzer = PublisherDeveloperAnalyzer(data_loader, 'output/path')
figures = analyzer.generate_all_publisher_developer_plots()
```

### 4. Temporal & Rating Analysis (`temporal_rating_analysis.py`)

**Purpose**: Time-based patterns, quality metrics, and age-related trends

**Visualizations**:
- Release timeline analysis with community formation eras
- Metacritic score distributions and coverage analysis
- Age rating patterns and content maturity
- Review engagement and recommendation patterns
- Quality evolution over time

**Key Insights**:
- Community formation in relation to gaming eras
- Quality vs. popularity relationships
- Age rating and pricing correlations
- Temporal quality trends

**Example Usage**:
```python
from communities_visualizations.temporal_rating_analysis import TemporalRatingAnalyzer

analyzer = TemporalRatingAnalyzer(data_loader, 'output/path')
figures = analyzer.generate_all_temporal_rating_plots()
```

### 5. Technical Features Analysis (`technical_features_analysis.py`)

**Purpose**: Technical capabilities, accessibility, and platform support

**Visualizations**:
- Language support matrix and international appeal
- Controller support patterns and platform relationships
- DLC vs. achievements content richness analysis
- Free vs. paid game distribution
- Platform compatibility 3D analysis

**Key Insights**:
- Internationalization strategies by community
- Technical feature clustering patterns
- Content monetization approaches
- Platform-specific community characteristics

**Example Usage**:
```python
from communities_visualizations.technical_features_analysis import TechnicalFeaturesAnalyzer

analyzer = TechnicalFeaturesAnalyzer(data_loader, 'output/path')
figures = analyzer.generate_all_technical_features_plots()
```

### 6. Similarity Analysis (`similarity_analysis.py`)

**Purpose**: Community relationships, clustering validation, and feature importance

**Visualizations**:
- Similarity matrices (cosine, euclidean, correlation)
- Hierarchical clustering dendrograms
- Multidimensional scaling (2D/3D projections)
- Feature importance analysis (Random Forest + statistical)
- Clustering method comparison (K-means, DBSCAN)

**Key Insights**:
- Community similarity patterns and groupings
- Optimal clustering parameters validation
- Most discriminative features for community classification
- Alternative clustering perspectives

**Example Usage**:
```python
from communities_visualizations.similarity_analysis import SimilarityAnalyzer

analyzer = SimilarityAnalyzer(data_loader, 'output/path')
figures = analyzer.generate_all_similarity_analysis_plots()
```

## 🎛️ Interactive Dashboard

The interactive dashboard provides real-time exploration capabilities with multiple analysis tabs.

### Features

- **🏠 Overview Tab**: Key statistics and community summaries
- **📊 Comparison Tab**: Side-by-side community comparison with multiple visualization types
- **🎯 Genre Tab**: Interactive genre analysis with filtering
- **🏢 Publisher Tab**: Publisher network exploration
- **⚙️ Technical Tab**: Technical features radar charts and analysis
- **🔍 Similarity Tab**: Interactive similarity matrix with drill-down capability
- **📈 Explorer Tab**: Raw data table with sorting, filtering, and export

### Launching the Dashboard

```bash
# Basic launch
python interactive_dashboard.py

# Custom configuration
python interactive_dashboard.py --port 8080 --host 0.0.0.0 --data-dir /path/to/data

# Programmatic launch
from communities_visualizations.interactive_dashboard import create_dashboard
dashboard = create_dashboard(data_dir='path/to/data')
dashboard.run_server(debug=True)
```

### Dashboard Controls

- **Community Selector**: Choose communities to analyze
- **Analysis Method**: Switch between analysis perspectives
- **Visualization Type**: Change plot types dynamically
- **Real-time Updates**: All visualizations update based on selections

## ⚙️ Configuration

### Styling and Colors

The `config.py` file contains extensive customization options:

```python
# Community color schemes
COMMUNITY_COLORS = {0: '#1f77b4', 1: '#ff7f0e', ...}  # Default
COMMUNITY_COLORS_DARK = {...}                          # Dark theme
COMMUNITY_COLORS_PASTEL = {...}                        # Pastel theme

# Figure sizes
FIGURE_SIZES = {
    'small': (8, 6),
    'medium': (12, 8),
    'large': (16, 10),
    'dashboard': (15, 12)
}

# Styling parameters
MATPLOTLIB_STYLE = {...}
PLOTLY_LAYOUT = {...}
```

### Data Paths

```python
DEFAULT_DATA_PATHS = {
    'community_profiles': 'community_14_profiles_analysis/community_average_profiles.csv',
    'detailed_profiles': 'community_14_profiles_analysis/detailed_community_profiles.json',
    'community_summary': 'louvain_14_communities_summary.csv'
}
```

### Community Metadata

```python
COMMUNITY_NAMES = {
    0: "Indie Casual",
    1: "Indie Adventure",
    ...
}

COMMUNITY_DESCRIPTIONS = {
    0: "Casual indie games with broad appeal",
    1: "Story-driven indie adventure games",
    ...
}
```

## 🛠️ Advanced Usage

### Custom Analysis Pipeline

```python
from communities_visualizations import *

# Load and validate data
data_loader = load_data('data/directory')
validation = data_loader.validate_data()

if validation['overall_valid']:
    # Custom analysis workflow
    
    # 1. Generate specific visualizations
    overview_viz = CommunityOverviewVisualizer(data_loader)
    size_figs = overview_viz.create_community_size_distribution()
    
    # 2. Access processed data
    genre_matrix = data_loader.get_genre_matrix(top_n=10)
    similarity_matrix = data_loader.get_similarity_matrix(method='cosine')
    
    # 3. Export processed data
    data_loader.export_processed_data('exports/')
```

### Extending the Analysis

```python
# Add custom visualization to existing analyzer
class CustomGenreAnalyzer(GenreCategoryAnalyzer):
    def create_custom_genre_analysis(self, save_plots=True):
        # Your custom analysis logic
        fig = go.Figure(...)
        
        if save_plots:
            fig.write_html(self.output_dir / 'custom_analysis.html')
        
        return {'custom_analysis': fig}

# Use in pipeline
analyzer = CustomGenreAnalyzer(data_loader, 'output/')
custom_figs = analyzer.create_custom_genre_analysis()
```

### Batch Processing

```python
# Process multiple datasets
datasets = ['dataset1/', 'dataset2/', 'dataset3/']

for dataset in datasets:
    orchestrator = VisualizationOrchestrator(
        data_dir=dataset,
        output_dir=f'outputs/{dataset}',
        verbose=True
    )
    results = orchestrator.generate_all_visualizations()
    print(f"Generated {results['report']['total_plots_generated']} plots for {dataset}")
```

## 📋 Command Line Interface

### Generate All Plots

```bash
# Full generation with all options
python generate_all_plots.py \
    --data-dir /path/to/community/data \
    --output-dir /path/to/outputs \
    --categories overview genres publishers temporal technical similarity

# Quick validation
python generate_all_plots.py --validate-only

# Minimal output
python generate_all_plots.py --quiet

# Help and options
python generate_all_plots.py --help
```

### Individual Modules

Each analysis module can be run independently:

```bash
# Community overview only
python community_overview.py --data-dir data/ --output-dir outputs/overview/

# Genre analysis with custom parameters
python genre_category_analysis.py --top-genres 15 --output-dir outputs/genres/

# Publisher analysis
python publisher_developer_analysis.py --top-publishers 20 --min-games 5

# Interactive dashboard
python interactive_dashboard.py --port 8080 --host 0.0.0.0 --no-debug
```

## 📈 Performance and Scalability

### Performance Metrics

- **Generation Speed**: ~50-100 plots per minute (varies by complexity)
- **Memory Usage**: ~500MB-2GB depending on dataset size
- **Disk Space**: ~50-200MB for complete visualization suite
- **Processing Time**: ~5-15 minutes for full suite generation

### Optimization Tips

1. **Selective Generation**: Use `--categories` to generate only needed analysis
2. **Data Preprocessing**: Cache processed matrices for repeated analysis
3. **Resolution Control**: Use DPI settings to balance quality vs. file size
4. **Parallel Processing**: Individual modules can be run in parallel
5. **Memory Management**: Process large datasets in chunks if needed

### Scaling to Larger Datasets

```python
# For datasets with >50 communities
config.FIGURE_SIZES['large'] = (20, 12)  # Larger figures
config.HEATMAP_PARAMS['annot'] = False   # Disable annotations
data_loader.get_genre_matrix(top_n=8)     # Reduce complexity
```

## 🔧 Troubleshooting

### Common Issues

**1. Missing Data Files**
```bash
# Check file existence
python generate_all_plots.py --validate-only

# Error: FileNotFoundError: Community profiles file not found
# Solution: Ensure data files are in correct location or specify --data-dir
```

**2. Memory Issues**
```python
# Reduce memory usage
import matplotlib.pyplot as plt
plt.ioff()  # Turn off interactive mode

# Process in smaller batches
selected_communities = list(range(5))  # Process first 5 communities
```

**3. Visualization Quality Issues**
```python
# Increase DPI for high-quality outputs
config.DPI_SETTINGS['print'] = 600

# Adjust figure sizes
config.FIGURE_SIZES['large'] = (20, 16)
```

**4. Dashboard Connection Issues**
```bash
# Check port availability
netstat -an | grep 8050

# Use different port
python interactive_dashboard.py --port 8080

# Allow external connections
python interactive_dashboard.py --host 0.0.0.0
```

### Debug Mode

```python
# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Validate data integrity
data_loader = load_data()
validation = data_loader.validate_data()
print("Validation results:", validation)

# Test individual components
overview_viz = CommunityOverviewVisualizer(data_loader)
test_fig = overview_viz.create_community_size_distribution(save_plots=False)
```

## 🤝 Contributing

### Development Setup

```bash
# Clone and setup development environment
git clone <repository>
cd communities_visualizations

# Install development dependencies
pip install -r requirements.txt
pip install -e .  # Editable install

# Run tests
python -m pytest tests/  # If tests exist
```

### Adding New Analysis Types

1. **Create New Module**: Follow existing module structure
2. **Update Configuration**: Add new color schemes, figure sizes
3. **Integrate with Orchestrator**: Add to `generate_all_plots.py`
4. **Add Dashboard Tab**: Update `interactive_dashboard.py`
5. **Update Documentation**: Add to README and generate examples

### Code Style

- **PEP 8 Compliance**: Use consistent formatting
- **Type Hints**: Include type annotations for better maintainability
- **Docstrings**: Comprehensive docstrings for all functions
- **Error Handling**: Graceful error handling with informative messages

## 📄 License

This project is part of the Steam Dataset Analysis suite. Please refer to the main project license for usage terms.

## 📞 Support

For issues, questions, or contributions:

1. **Check Documentation**: Review this README and inline documentation
2. **Validate Setup**: Use `--validate-only` flag to check configuration
3. **Check Logs**: Review generation reports for detailed error information
4. **Create Issue**: Submit detailed issue reports with error messages and system information

## 🎉 Acknowledgments

This visualization suite builds upon:
- **Plotly & Dash**: Interactive visualization framework
- **Matplotlib & Seaborn**: Static visualization capabilities  
- **Pandas & NumPy**: Data processing foundation
- **Scikit-learn**: Machine learning and analysis tools
- **NetworkX**: Network analysis capabilities

---

**Happy Visualizing! 🎨📊🎮**