# Sets up the MSVC x64 build environment and pins CUDA 13.1 in the current session.
# Uses a generated wrapper .cmd so batch-file quoting is handled by cmd itself.
$ErrorActionPreference = "Stop"
$vswhere = "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe"
if (-not (Test-Path $vswhere)) { throw "vswhere not found" }
$vspath = & $vswhere -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath
if (-not $vspath) { throw "Visual Studio Build Tools not found" }
$vcvars = Join-Path $vspath "VC\Auxiliary\Build\vcvars64.bat"
if (-not (Test-Path $vcvars)) { throw "vcvars64.bat not found at $vcvars" }

$wrapper = Join-Path $env:TEMP ("vsenv_" + [guid]::NewGuid().ToString("N") + ".cmd")
@"
@echo off
call "$vcvars" >nul 2>&1
set
"@ | Set-Content -Path $wrapper -Encoding ASCII
try {
    $envOutput = & cmd.exe /c $wrapper
} finally {
    Remove-Item -Path $wrapper -Force -ErrorAction SilentlyContinue
}
foreach ($line in $envOutput) {
    $idx = $line.IndexOf("=")
    if ($idx -gt 0) {
        $name = $line.Substring(0, $idx)
        $val = $line.Substring($idx + 1)
        Set-Item -Path "env:$name" -Value $val
    }
}

# Pin CUDA 13.1 (supports sm_120 / RTX 5090): force it to the front of PATH.
$cudaRoot = "C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\v13.1"
if (Test-Path $cudaRoot) {
    $env:CUDA_PATH = $cudaRoot
    $env:CUDA_HOME = $cudaRoot
    $env:CUDAToolkit_ROOT = $cudaRoot
    $cuBin = Join-Path $cudaRoot "bin"
    $env:PATH = $cuBin + ";" + $env:PATH
}
