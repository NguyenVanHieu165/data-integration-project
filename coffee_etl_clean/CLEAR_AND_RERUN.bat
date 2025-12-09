@echo off
echo ============================================================================
echo CLEAR STAGING ZONES AND RERUN PIPELINE
echo ============================================================================
echo.

echo Step 1: Clearing staging zones...
echo.

if exist staging\raw\*.csv (
    del /q staging\raw\*.csv
    echo   - Cleared RAW zone
) else (
    echo   - RAW zone already empty
)

if exist staging\clean\*.csv (
    del /q staging\clean\*.csv
    echo   - Cleared CLEAN zone
) else (
    echo   - CLEAN zone already empty
)

if exist staging\error\*.csv (
    del /q staging\error\*.csv
    echo   - Cleared ERROR zone
) else (
    echo   - ERROR zone already empty
)

echo.
echo Step 2: Running pipeline with FIXED validation rules...
echo.
echo Press any key to start pipeline...
pause > nul

python RUN_ALL_STEPS.py

echo.
echo ============================================================================
echo DONE! Check dashboard at http://localhost:5000
echo ============================================================================
pause
