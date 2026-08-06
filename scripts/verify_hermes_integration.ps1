param(
    [string]$HermesSource = $env:HERMES_ROOT,
    [string]$HermesAuxiliarySource = $env:HERMES_AUXILIARY_ROOT
)

$ErrorActionPreference = "Stop"
$repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$pluginPython = Join-Path $repo ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $pluginPython)) {
    throw "找不到插件虚拟环境 Python: $pluginPython"
}
if (-not $HermesSource) {
    throw "请通过 -HermesSource 或 HERMES_ROOT 指定 Hermes 源码目录"
}
$HermesSource = (Resolve-Path -LiteralPath $HermesSource).Path
$sitePackages = Join-Path $HermesSource "venv\Lib\site-packages"
if (-not (Test-Path -LiteralPath $sitePackages)) {
    throw "找不到 Hermes site-packages: $sitePackages"
}
if ($HermesAuxiliarySource) {
    $HermesAuxiliarySource = (Resolve-Path -LiteralPath $HermesAuxiliarySource).Path
}

$tempHome = Join-Path ([IO.Path]::GetTempPath()) ("hermes-onebot11-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $tempHome | Out-Null
$oldPythonPath = $env:PYTHONPATH
$oldHermesHome = $env:HERMES_HOME
try {
    $env:HERMES_HOME = $tempHome
    $sources = @()
    if ($HermesAuxiliarySource) {
        $sources += $HermesAuxiliarySource
    }
    $sources += $HermesSource
    $sources += $sitePackages
    $sources += $repo
    $env:PYTHONPATH = ($sources -join [IO.Path]::PathSeparator)

    & $pluginPython -m pytest -q
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
    if ($HermesAuxiliarySource) {
        & $pluginPython -m pytest -q (Join-Path $HermesAuxiliarySource "tests\agent\test_auxiliary_no_fallback.py")
        if ($LASTEXITCODE -ne 0) {
            exit $LASTEXITCODE
        }
    }
}
finally {
    $env:PYTHONPATH = $oldPythonPath
    $env:HERMES_HOME = $oldHermesHome
    if (Test-Path -LiteralPath $tempHome) {
        Remove-Item -LiteralPath $tempHome -Recurse -Force
    }
}
