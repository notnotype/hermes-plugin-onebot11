param(
    [string]$HermesSource = $env:HERMES_ROOT,
    [string]$HermesAuxiliarySource = $env:HERMES_AUXILIARY_ROOT
)

$ErrorActionPreference = "Stop"
$repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$pluginPython = Join-Path $repo ".venv\Scripts\python.exe"
$script = Join-Path $repo "scripts\verify_hermes_integration.py"
if (-not (Test-Path -LiteralPath $pluginPython)) {
    throw "找不到插件虚拟环境 Python: $pluginPython"
}
if (-not $HermesSource) {
    throw "请通过 -HermesSource 或 HERMES_ROOT 指定 Hermes 源码目录"
}
$HermesSource = (Resolve-Path -LiteralPath $HermesSource).Path

$arguments = @(
    $script,
    "--plugin-root", $repo,
    "--hermes-source", $HermesSource
)
if ($HermesAuxiliarySource) {
    $arguments += @("--hermes-auxiliary-source", (Resolve-Path -LiteralPath $HermesAuxiliarySource).Path)
}

& $pluginPython @arguments
exit $LASTEXITCODE
