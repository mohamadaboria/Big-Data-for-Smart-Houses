@echo off
echo ========================================
echo Smart House Data Platform - Stop Services
echo ========================================
echo.

echo [1/3] Stopping Docker services...
docker-compose down

echo [2/3] Stopping Python processes...
REM Kill Python processes related to our project
taskkill /f /im python.exe 2>nul
taskkill /f /im streamlit.exe 2>nul

echo [3/3] Cleanup complete!
echo.
echo All services have been stopped.
echo.
pause

