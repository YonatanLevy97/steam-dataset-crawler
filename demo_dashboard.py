#!/usr/bin/env python3
"""
Launch the Steam Communities Interactive Dashboard

This demonstrates the interactive exploration capabilities.
"""

import sys
from pathlib import Path

# Add the communities_visualizations directory to Python path
viz_dir = Path(__file__).parent / "communities_visualizations"
sys.path.insert(0, str(viz_dir))

def main():
    print("🚀 Steam Communities Interactive Dashboard")
    print("=" * 50)
    
    try:
        from interactive_dashboard import create_dashboard
        
        print("📥 Loading community data for dashboard...")
        dashboard = create_dashboard(data_dir=None, port=8050)
        
        print("\n🌐 Dashboard Features:")
        print("  🏠 Overview Tab - Key statistics and community summaries")
        print("  📊 Comparison Tab - Side-by-side community comparison")
        print("  🎯 Genre Tab - Interactive genre analysis")
        print("  🏢 Publisher Tab - Publisher network exploration")
        print("  ⚙️ Technical Tab - Technical features analysis")
        print("  🔍 Similarity Tab - Community similarity matrix")
        print("  📈 Explorer Tab - Raw data with filtering and export")
        
        print(f"\n🎯 Starting dashboard server...")
        print(f"📊 Dashboard URL: http://127.0.0.1:8050")
        print(f"👆 Click the link above or copy-paste into your browser")
        print(f"🛑 Press Ctrl+C to stop the server")
        
        dashboard.run_server(debug=False, host='127.0.0.1')
        
    except KeyboardInterrupt:
        print("\n👋 Dashboard stopped by user")
        return 0
    except Exception as e:
        print(f"❌ Error launching dashboard: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    main()