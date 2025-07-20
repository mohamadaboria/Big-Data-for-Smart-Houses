@echo off
echo ========================================
echo Smart House Data Platform - Status Check
echo ========================================
echo.

echo [1/4] Docker Services Status:
echo.
docker-compose ps
echo.

echo [2/4] Kafka Topics:
echo.
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>nul
if %errorlevel% neq 0 (
    echo Kafka is not ready or not running
)
echo.

echo [3/4] Data Directory Structure:
echo.
if exist "data" (
    echo data\ directory exists
    if exist "data\bronze" echo   bronze\ directory exists
    if exist "data\silver" echo   silver\ directory exists
    if exist "data\gold" echo   gold\ directory exists
    
    echo.
    echo File counts:
    for /f %%i in ('dir /s /b data\*.parquet 2^>nul ^| find /c ".parquet"') do echo   Total Parquet files: %%i
) else (
    echo data\ directory does not exist
)
echo.

echo [4/4] Python Processes:
echo.
tasklist /fi "imagename eq python.exe" /fo table 2>nul | findstr python.exe
if %errorlevel% neq 0 (
    echo No Python processes running
)
echo.

echo [5/5] Access Points:
echo.
echo Dashboard: http://localhost:8501
echo Kafka UI: http://localhost:8080
echo.

pause

