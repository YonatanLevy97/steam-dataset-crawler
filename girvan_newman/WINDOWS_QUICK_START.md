# 🪟 Windows Quick Start Guide

## ⚡ 30-Second Setup

1. **Install Python** from [python.org](https://www.python.org/downloads/) (check "Add Python to PATH")
2. **Install packages**: Run `examples\install_requirements.bat`
3. **Test it works**: Run `examples\quick_test.bat`

## 🚀 Basic Usage

```cmd
cd girvan_newman

REM Quick test (recommended first)
examples\quick_test.bat

REM Full analysis
scripts\run_girvan_newman.bat --edges ..\out\graph_runs\...\edges_top100.csv.gz --giant-only

REM Feature analysis
python scripts\detailed_community_feature_analysis.py --communities ..\out\girvan_newman_...\community_assignments_best.csv --metadata ..\out\dead_labels_enriched.csv
```

## 🗂️ Windows Files

- **`scripts\run_girvan_newman.bat`** - Main analysis (Windows version)
- **`examples\quick_test.bat`** - Fast test
- **`examples\install_requirements.bat`** - Install Python packages
- **All `.py` files work unchanged on Windows**

## 🔧 Troubleshooting

**"python not found"**: Try `py` instead of `python`
**Package errors**: Run `examples\install_requirements.bat`
**Path issues**: Use `"quotes"` around paths with spaces

## ✅ Ready!

Your Windows team can now use Girvan-Newman community detection! 🎉

For detailed setup, see [WINDOWS_SETUP.md](WINDOWS_SETUP.md)