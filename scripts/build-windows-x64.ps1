param(
    [ValidateSet("standalone", "onefile")]
    [string]$Mode = "standalone",
    [switch]$RegenerateSpec,
    [switch]$SkipVenv,
    [string]$PythonLauncher = "py -3.12"
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

if (-not [Environment]::Is64BitOperatingSystem) {
    throw "This build script only supports 64-bit Windows."
}

if (-not (Get-Command dumpbin.exe -ErrorAction SilentlyContinue)) {
    Write-Warning "dumpbin.exe not found on PATH. Install Visual Studio Build Tools / MSVC."
}

if (-not $SkipVenv) {
    if (-not (Test-Path ".venv\Scripts\python.exe")) {
        Write-Host "Creating virtual environment..."
        & cmd /c "$PythonLauncher -m venv .venv"
    }

    Write-Host "Installing dependencies..."
    & ".\.venv\Scripts\python.exe" -m pip install -U pip
    & ".\.venv\Scripts\python.exe" -m pip install -e .
}

$PythonExe = (Resolve-Path ".\.venv\Scripts\python.exe").Path
$DeployExe = (Resolve-Path ".\.venv\Scripts\pyside6-deploy.exe").Path
$PythonBits = & $PythonExe -c "import struct; print(struct.calcsize('P') * 8)"

if ($PythonBits.Trim() -ne "64") {
    throw "The active Python interpreter is not 64-bit. Install and use 64-bit Python for win_amd64 packaging."
}

if ($RegenerateSpec -or -not (Test-Path ".\pysidedeploy.spec")) {
    Write-Host "Generating pysidedeploy.spec..."
    & $DeployExe ".\main.py" --init --force
}

$ExecDirectory = "dist/windows-x64"

Write-Host "Configuring pysidedeploy.spec..."
& $PythonExe ".\scripts\configure_pyside6_spec.py" `
    --spec ".\pysidedeploy.spec" `
    --python-path $PythonExe `
    --exec-directory $ExecDirectory `
    --mode $Mode `
    --title "mesh-bc-tester"

Write-Host "Building win_amd64 package..."
& $DeployExe -c ".\pysidedeploy.spec" --force

Write-Host ""
Write-Host "Build finished."
Write-Host "Output directory: $ExecDirectory"
