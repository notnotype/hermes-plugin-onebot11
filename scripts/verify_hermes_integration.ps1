param(
    [string]$HermesSource = $env:HERMES_ROOT
)

$ErrorActionPreference = "Stop"
$repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$script = Join-Path $repo "scripts\verify_hermes_integration.py"
$pythonCandidates = @(
    (Join-Path $repo ".venv\Scripts\python.exe"),
    (Join-Path $repo "..\..\..\..\.venv\Scripts\python.exe")
)
$pluginPython = $pythonCandidates |
    Where-Object { Test-Path -LiteralPath $_ } |
    Select-Object -First 1
if (-not $pluginPython) {
    throw "找不到插件虚拟环境 Python；已检查: $($pythonCandidates -join ', ')"
}
$pluginPython = (Resolve-Path -LiteralPath $pluginPython).Path
if (-not $HermesSource) {
    throw "请通过 -HermesSource 或 HERMES_ROOT 指定 Hermes 源码目录"
}
$HermesSource = (Resolve-Path -LiteralPath $HermesSource).Path

$arguments = @(
    $script,
    "--plugin-root", $repo,
    "--hermes-source", $HermesSource
)
$hermesSiteCandidates = @(
    (Join-Path $HermesSource "venv\Lib\site-packages"),
    (Join-Path $HermesSource "..\..\..\..\venv\Lib\site-packages")
)
$hermesSitePackages = $hermesSiteCandidates |
    Where-Object { Test-Path -LiteralPath $_ } |
    Select-Object -First 1
if ($hermesSitePackages) {
    $arguments += @("--hermes-site-packages", (Resolve-Path -LiteralPath $hermesSitePackages).Path)
}
& $pluginPython @arguments
exit $LASTEXITCODE
