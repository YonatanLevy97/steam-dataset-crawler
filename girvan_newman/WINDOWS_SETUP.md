# Windows Setup Guide for Girvan-Newman

## 🪟 Running on Windows

Windows users can use the Girvan-Newman system with these Windows-specific files and setup instructions.

## 📋 Prerequisites

### 1. Install Python
- Download Python 3.8+ from [python.org](https://www.python.org/downloads/)
- **IMPORTANT**: Check "Add Python to PATH" during installation
- Verify installation: Open Command Prompt and run `python --version`

### 2. Install Required Packages
Open Command Prompt and run:
```cmd
pip install networkx matplotlib numpy pandas
```

### 3. Verify Git Bash (Optional but Recommended)
If you have Git installed, you can also use Git Bash to run the `.sh` scripts:
- Download Git from [git-scm.com](https://git-scm.com/downloads)
- Git Bash provides a Unix-like environment on Windows

## 🚀 Windows Usage

### Option 1: Use Windows Batch Files (Recommended)
```cmd
cd girvan_newman

# Quick test
examples\quick_test.bat

# Full analysis  
scripts\run_girvan_newman.bat --edges ..\out\graph_runs\...\edges_top100.csv.gz --giant-only

# Feature analysis
python scripts\detailed_community_feature_analysis.py --communities ..\out\girvan_newman_...\community_assignments_best.csv --metadata ..\out\dead_labels_enriched.csv
```

### Option 2: Use Git Bash (If Available)
```bash
cd girvan_newman

# Use the regular .sh scripts
./examples/quick_test.sh
./scripts/run_girvan_newman.sh --edges ../out/graph_runs/.../edges_top100.csv.gz --giant-only
```

### Option 3: Direct Python Calls
```cmd
cd girvan_newman

# Direct Python execution
python scripts\girvan_newman_analysis.py --edges ..\out\graph_runs\...\edges_top100.csv.gz --out-dir ..\out\girvan_newman_analysis --giant-only
```

## 📁 Windows-Specific Files

### Batch Scripts (`.bat`)
- **`scripts\run_girvan_newman.bat`** - Main wrapper for Windows
- **`examples\quick_test.bat`** - Quick test for Windows
- **`examples\basic_usage.bat`** - Full workflow example for Windows

### Python Scripts (Work on All Platforms)
- **`scripts\girvan_newman_analysis.py`** - Core algorithm
- **`scripts\detailed_community_feature_analysis.py`** - Feature analysis
- **`scripts\community_feature_summary.py`** - Readable summaries

## 💡 Windows Tips

### Path Separators
- Windows uses backslashes `\` instead of forward slashes `/`
- The batch files handle this automatically
- When typing paths manually, use `\` or `\\`

### Examples:
```cmd
# Good (Windows style)
--edges ..\out\graph_runs\20250919_143520\edges\edges_top100.csv.gz

# Also works (forward slashes often work in Windows)
--edges ../out/graph_runs/20250919_143520/edges/edges_top100.csv.gz
```

### Python Executable
- Most Windows systems use `python` (not `python3`)
- If you get "python not found", try `py` instead:
```cmd
py scripts\girvan_newman_analysis.py --help
```

### PowerShell Alternative
If you prefer PowerShell over Command Prompt:
```powershell
cd girvan_newman
python scripts\girvan_newman_analysis.py --edges "..\out\graph_runs\...\edges_top100.csv.gz" --giant-only
```

## 🔧 Troubleshooting

### "python is not recognized"
1. Reinstall Python and check "Add Python to PATH"
2. Or use `py` instead of `python`
3. Or find Python installation and add to PATH manually

### "No module named 'networkx'"
```cmd
pip install networkx matplotlib numpy pandas
```

### Path Issues
- Use full paths if relative paths don't work
- Use quotes around paths with spaces: `"C:\My Data\edges.csv"`

### Permission Errors
- Run Command Prompt as Administrator if needed
- Make sure you can write to the output directory

## 📊 Complete Windows Workflow

```cmd
# 1. Navigate to girvan_newman folder
cd girvan_newman

# 2. Run quick test to verify everything works
examples\quick_test.bat

# 3. Run full analysis
scripts\run_girvan_newman.bat --edges ..\out\graph_runs\...\edges_top100.csv.gz --giant-only

# 4. Analyze features
python scripts\detailed_community_feature_analysis.py --communities ..\out\girvan_newman_...\community_assignments_best.csv --metadata ..\out\dead_labels_enriched.csv --out-dir ..\out\detailed_analysis

# 5. Create summaries  
python scripts\community_feature_summary.py --analysis ..\out\detailed_analysis\detailed_feature_analysis.json --out-dir ..\out\community_summary
```

## 🎯 Key Differences from Mac/Linux

| Aspect | Windows | Mac/Linux |
|--------|---------|-----------|
| File Extension | `.bat` | `.sh` |
| Path Separator | `\` | `/` |
| Python Command | `python` | `python3` |
| Script Execution | `script.bat` | `./script.sh` |
| Line Endings | CRLF | LF |

## ✅ Windows Files Summary

### Ready-to-Use Windows Files:
- ✅ `scripts\run_girvan_newman.bat` - Main analysis wrapper
- ✅ `examples\quick_test.bat` - Fast test
- ✅ `examples\basic_usage.bat` - Complete workflow
- ✅ All Python scripts work unchanged on Windows

### Cross-Platform Files (Work on Windows Too):
- ✅ `scripts\girvan_newman_analysis.py` 
- ✅ `scripts\detailed_community_feature_analysis.py`
- ✅ `scripts\community_feature_summary.py`

Your Windows team members can now use the system with native Windows batch files! 🎉