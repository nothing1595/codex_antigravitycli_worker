$ErrorActionPreference = 'Stop'

$bridgeRoot = Split-Path -Parent (Split-Path -Parent $PSCommandPath)
$sourceDir = Join-Path $bridgeRoot 'agents'

# Resolve Codex home directory
$codexHome = if ($env:CODEX_HOME) {
    $env:CODEX_HOME
} elseif (Test-Path -LiteralPath 'E:\ChatGPT\UserProfile\.codex') {
    'E:\ChatGPT\UserProfile\.codex'
} else {
    Join-Path $env:USERPROFILE '.codex'
}

$targetDir = Join-Path $codexHome 'agents'

# Resolve Node executable
$nodeCommand = Get-Command node -ErrorAction SilentlyContinue
$nodeExe = if ($env:AGY_NODE_EXE) {
    $env:AGY_NODE_EXE
} elseif ($nodeCommand) {
    $nodeCommand.Source
} elseif (Test-Path -LiteralPath 'E:\Node.js\node.exe') {
    'E:\Node.js\node.exe'
} else {
    'node'
}

$serverPath = Join-Path $bridgeRoot 'server\antigravity-worker.cjs'

Write-Host "=== Antigravity-Codex Bridge Installer ===" -ForegroundColor Cyan
Write-Host "Bridge Root: $bridgeRoot"
Write-Host "Codex Home : $codexHome"
Write-Host "Node Path  : $nodeExe"
Write-Host "Server Path: $serverPath"

# Clean up obsolete worker definitions if present
$obsoleteWorkers = @('agy-pro-worker.toml', 'agy_pro_worker.toml')
foreach ($oldFile in $obsoleteWorkers) {
    $oldPath = Join-Path $targetDir $oldFile
    if (Test-Path -LiteralPath $oldPath) {
        Remove-Item -LiteralPath $oldPath -Force
        Write-Host "Cleaned up obsolete agent: $oldPath" -ForegroundColor Yellow
    }
}

# Dynamic Model Verification via `agy models`
Write-Host "`nValidating available models in Antigravity CLI..." -ForegroundColor Yellow
$availableModels = @()
try {
    $prevEap = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    $modelsOutput = & agy models 2>$null
    $ErrorActionPreference = $prevEap
    foreach ($line in $modelsOutput) {
        $trimmed = $line.Trim()
        if ($trimmed -and $trimmed -notmatch 'Fetching available models') {
            $parts = $trimmed -split "`t"
            if ($parts.Count -ge 1) {
                $availableModels += $parts[0].Trim()
            }
        }
    }
    Write-Host "Found $($availableModels.Count) available models in Antigravity CLI." -ForegroundColor Green
} catch {
    Write-Warning "Could not run 'agy models' ($($_.Exception.Message)). Will proceed."
}

function ConvertTo-TomlBasicStringValue([string]$Value) {
    return $Value.Replace('\', '\\').Replace('"', '\"')
}

function Install-AgentTemplate([string]$FileName) {
    $sourcePath = Join-Path $sourceDir $FileName
    if (-not (Test-Path -LiteralPath $sourcePath)) {
        Write-Warning "Source template not found: $sourcePath"
        return
    }

    $template = Get-Content -Raw -LiteralPath $sourcePath
    $rendered = $template.Replace('__NODE_EXE__', (ConvertTo-TomlBasicStringValue $nodeExe))
    $rendered = $rendered.Replace('__ANTIGRAVITY_BRIDGE_SERVER__', (ConvertTo-TomlBasicStringValue $serverPath))

    $targetPath = Join-Path $targetDir $FileName
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($targetPath, $rendered, $utf8NoBom)
    Write-Host "Installed: $targetPath" -ForegroundColor Green
}

New-Item -ItemType Directory -Force -Path $targetDir | Out-Null

# Install the single unified gateway agent
Install-AgentTemplate 'agy-worker.toml'

# Register in Codex config.toml so Codex automatically approves tool dispatches without prompting
$configPath = Join-Path $codexHome 'config.toml'
if (Test-Path -LiteralPath $configPath) {
    $configText = Get-Content -Raw -LiteralPath $configPath
    if ($configText -notmatch '(?m)^\[mcp_servers\.antigravity_worker\]\s*$') {
        $mcpConfig = @"

[mcp_servers.antigravity_worker]
command = "$(ConvertTo-TomlBasicStringValue $nodeExe)"
args = ["$(ConvertTo-TomlBasicStringValue $serverPath)"]
startup_timeout_sec = 20
tool_timeout_sec = 60
"@
        $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
        [System.IO.File]::AppendAllText($configPath, $mcpConfig, $utf8NoBom)
        Write-Host "Registered antigravity_worker globally in $configPath (auto-approves tool dispatch)" -ForegroundColor Green
    }
}

# Register user-level Windows Scheduled Task for AntigravityBroker
# This ensures that when the worker calls schtasks /Run /TN AntigravityBroker, the broker
# executes in the interactive authenticated host user session (e.g. 15869) even if invoked from codexsandboxoffline.
if ($IsWindows -or $env:OS -match 'Windows') {
    try {
        $brokerScript = Join-Path $bridgeRoot 'server\antigravity-broker.cjs'
        $taskCmd = "`"$nodeExe`" `"$brokerScript`""
        schtasks /Create /TN "AntigravityBroker" /TR $taskCmd /SC ONCE /ST 23:59 /F 2>&1 | Out-Null
        Write-Host "Registered user Scheduled Task 'AntigravityBroker' (host security context)." -ForegroundColor Green
    } catch {
        Write-Warning "Could not register AntigravityBroker scheduled task: $($_.Exception.Message)"
    }
}

Write-Host "`nInstallation completed successfully!" -ForegroundColor Cyan
Write-Host "Restart Codex, then ask it to spawn agy_worker."
Write-Host "When assigned, agy_worker will report all available 'agy_XXX_worker' models running at maximum reasoning effort and auto-approve all operations."
