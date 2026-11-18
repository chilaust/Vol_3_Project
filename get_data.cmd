: << 'EOF'
@echo off
goto :WIN
EOF

# --- MAC / LINUX EXECUTION ---
cd "$(dirname "$0")"
echo "Detected macOS/Linux. Setting up environment..."
if [ ! -d ".venv" ]; then
    python3 -m venv .venv
fi
source .venv/bin/activate
pip install --upgrade pip -q
pip install -r requirements.txt
python3 build_data.py
echo ""
read -p "Done! Press Enter to close..."
exit 0

:WIN
REM --- WINDOWS EXECUTION ---
cd /d "%~dp0"
echo Detected Windows. Setting up environment...
if not exist ".venv" (
    python -m venv .venv
)
call .venv\Scripts\activate.bat
pip install --upgrade pip -q
pip install -r requirements.txt
python build_data.py
echo.
pause