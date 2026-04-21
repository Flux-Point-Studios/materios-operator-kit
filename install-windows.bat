@echo off
REM ============================================================================
REM  Materios Node Installer for Windows
REM
REM  Double-click this file to run.
REM  Requires: WSL2 + Docker Desktop
REM ============================================================================

title Materios Node Installer

echo.
echo   ======================================
echo     Materios Node Installer (Windows)
echo   ======================================
echo.

REM Check if WSL is available
wsl --status >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo   WSL2 is not installed.
    echo.
    echo   Materios requires Windows Subsystem for Linux ^(WSL2^).
    echo   To install it, open PowerShell as Administrator and run:
    echo.
    echo     wsl --install
    echo.
    echo   Then restart your computer and run this installer again.
    echo.
    pause
    exit /b 1
)

REM Check if Docker Desktop is running
docker info >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo   Docker Desktop is not running or not installed.
    echo.
    echo   Please install Docker Desktop for Windows:
    echo   https://www.docker.com/products/docker-desktop/
    echo.
    echo   Make sure to enable "Use the WSL 2 based engine" in Docker settings.
    echo   After installing, start Docker Desktop and run this installer again.
    echo.
    start https://www.docker.com/products/docker-desktop/
    pause
    exit /b 1
)

echo   Docker Desktop is running. Starting installation...
echo.

REM Ask for mode
echo   How would you like to participate?
echo.
echo     1^) Full Validator  - run a blockchain node + attestation daemon
echo        ^(Requires: 2+ CPU, 2GB RAM, 50GB disk, port 30333 open^)
echo.
echo     2^) Attestor Only   - run just the attestation daemon ^(lighter^)
echo        ^(Requires: 1 CPU, 512MB RAM, 1GB disk, outbound internet^)
echo.
set /p MODE_CHOICE="  Enter 1 or 2 [1]: "
if "%MODE_CHOICE%"=="" set MODE_CHOICE=1

if "%MODE_CHOICE%"=="2" (
    set MODE=attestor
) else (
    set MODE=validator
)

REM Ask for name
echo.
set /p NODE_LABEL="  Choose a name for your node: "
if "%NODE_LABEL%"=="" set NODE_LABEL=%COMPUTERNAME%

REM Ask for install dir (optional - lets operators run multiple attestors on one host)
echo.
echo   Install directory ^(leave blank for default^).
echo   Only set this if you're running a SECOND attestor alongside an existing one.
echo   Default: ~/materios-operator ^(validator^) or ~/materios-attestor ^(attestor^)
set /p INSTALL_DIR="  Install dir: "

REM Run installer in WSL
echo.
echo   Running installer in WSL...
echo.

if "%INSTALL_DIR%"=="" (
    wsl -e bash -c "curl -sSL https://raw.githubusercontent.com/Flux-Point-Studios/materios-operator-kit/main/install.sh | bash -s -- --mode %MODE% --label %NODE_LABEL%"
) else (
    wsl -e bash -c "curl -sSL https://raw.githubusercontent.com/Flux-Point-Studios/materios-operator-kit/main/install.sh | bash -s -- --mode %MODE% --label %NODE_LABEL% --install-dir %INSTALL_DIR%"
)

echo.
echo   ========================================
echo   Installation complete!
echo.
echo   To check your node, open WSL and run:
if "%INSTALL_DIR%"=="" (
    echo     cd ~/materios-operator ^&^& docker compose logs -f
) else (
    echo     cd %INSTALL_DIR% ^&^& docker compose logs -f
)
echo.
pause
