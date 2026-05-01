Param(
    [int]$Port = 8501,
    [string]$Python = ".\environment\python.exe"
)

Write-Output "Checking port $Port..."
try {
    $conn = Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue
    if ($conn) {
        $pids = $conn | Select-Object -ExpandProperty OwningProcess -Unique
        foreach ($procId in $pids) {
            Write-Output "Killing process $procId on port $Port"
            Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue
        }
    } else {
        Write-Output "No process listening on port $Port"
    }
} catch {
    # fallback: parse netstat (older Windows versions)
    $out = netstat -ano | Select-String ":$Port\s"
    foreach ($line in $out) {
        $parts = ($line -split '\s+') | Where-Object { $_ -ne '' }
        $procIdFallback = $parts[-1]
        Write-Output "Killing PID $procIdFallback"
        taskkill /PID $procIdFallback /F | Out-Null
    }
}

Write-Output "Launching Streamlit on port $Port..."
# Ensure Python subprocess output uses UTF-8 to avoid UnicodeDecodeError when reading logs
$env:PYTHONIOENCODING = 'utf-8'
$env:PYTHONUTF8 = '1'

Start-Process -FilePath $Python -ArgumentList '-m','streamlit','run','app.py','--server.port',$Port -NoNewWindow
