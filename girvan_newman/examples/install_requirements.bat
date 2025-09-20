@echo off
REM install_requirements.bat - Install required Python packages for Windows

echo Installing required Python packages for Girvan-Newman...
echo.

echo Installing networkx...
pip install networkx>=3.0

echo Installing matplotlib...
pip install matplotlib>=3.5

echo Installing numpy...
pip install numpy>=1.20

echo Installing pandas...
pip install pandas>=1.5

echo.
echo Installation complete!
echo.
echo Testing imports...
python -c "import networkx; import matplotlib; import numpy; import pandas; print('All packages imported successfully!')"

echo.
echo Ready to use Girvan-Newman! Try: examples\quick_test.bat
pause