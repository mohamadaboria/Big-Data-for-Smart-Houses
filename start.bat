@echo off
echo ========================================
echo Smart House Data Platform - Quick Start
echo ========================================
echo.

REM Check if Docker is installed and running
echo [1/6] Checking Docker installation...
docker --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Docker is not installed or not in PATH
    echo Please install Docker Desktop from: https://www.docker.com/products/docker-desktop
    pause
    exit /b 1
)

docker info >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Docker is not running
    echo Please start Docker Desktop and try again
    pause
    exit /b 1
)
echo Docker is ready!

REM Check if Python  is installed
echo [2/6] Checking Python  installation...
py --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Python  is not installed or not accessible via 'py '
    echo Please install Python  from: https://www.python.org/downloads/
    pause
    exit /b 1
)
echo Python  is ready!

REM Create virtual environment if it doesn't exist
echo [3/6] Setting up Python virtual environment...
if not exist "venv" (
    echo Creating virtual environment with Python ...
    py  -m venv venv
    if %errorlevel% neq 0 (
        echo ERROR: Failed to create virtual environment with Python 
        pause
        exit /b 1
    )
)

REM Activate virtual environment
echo Activating virtual environment...
if exist "venv\Scripts\activate.bat" (
    call venv\Scripts\activate.bat
) else (
    echo ERROR: Virtual environment activation script not found
    echo Recreating virtual environment...
    rmdir /s /q venv 2>nul
    py  -m venv venv
    if %errorlevel% neq 0 (
        echo ERROR: Could not recreate virtual environment
        pause
        exit /b 1
    )
    call venv\Scripts\activate.bat
)

REM Upgrade pip first
echo Upgrading pip...
python -m pip install --upgrade pip

REM Install Python dependencies
echo Installing Python dependencies...
pip install -r requirements.txt
if %errorlevel% neq 0 (
    echo ERROR: Failed to install dependencies
    pause
    exit /b 1
)

REM Start Docker services
echo [4/6] Starting Docker services (Kafka, Zookeeper, Kafka UI)...
docker-compose up -d

REM Wait for Kafka to be ready
echo [5/6] Waiting for Kafka to be ready...
timeout /t 30 /nobreak >nul
echo Kafka should be ready now!

REM Start the data pipeline components
echo [6/6] Starting data pipeline components...
echo.
echo Starting components in separate windows...
echo - Producer (IoT Simulator)
echo - Bronze Consumer
echo - ETL Processors
echo - Dashboard
echo.

REM Start producer in new window
start "IoT Producer" cmd /k "cd /d "%~dp0" && call venv\Scripts\activate.bat && python producer\telemetry_sim.py && pause"

REM Wait a bit for producer to start
timeout /t 5 /nobreak >nul

REM Start bronze consumer in new window
start "Bronze Consumer" cmd /k "cd /d "%~dp0" && call venv\Scripts\activate.bat && python consumer\ingest_bronze.py && pause"

REM Wait a bit for consumer to start
timeout /t 5 /nobreak >nul

REM Start ETL processors in new window
start "ETL Processors" cmd /k "cd /d "%~dp0" && call venv\Scripts\activate.bat && python scripts\run_etl.py && pause"

REM Wait a bit for ETL to start
timeout /t 5 /nobreak >nul

REM Start dashboard in new window
start "Dashboard" cmd /k "cd /d "%~dp0" && call venv\Scripts\activate.bat && streamlit run dashboard\app.py && pause"

echo.
echo ========================================
echo All components started successfully!
echo ========================================
echo.
echo Access points:
echo - Dashboard: http://localhost:8501
echo - Kafka UI: http://localhost:8080
echo.
echo To stop all services, run: stop.bat
echo.
pause

