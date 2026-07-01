# run_padding_experiments.ps1
# 위치: moq-rs 루트 폴더에서 실행

$ErrorActionPreference = "Continue"

# ===== 설정 =====
$RelayUrl = "https://192.168.81.129:4443"
$VideoPath = "clean_bbb.mp4"
$NameBase = "test"

$RunSeconds = 400
$RestartGapSeconds = 30

# 0M ~ 8M
$Bitrates = @(0, 1000000, 2000000, 3000000, 4000000, 5000000, 6000000, 7000000, 8000000)

# 로그 저장 폴더
$OutDir = "padding_runs"
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

function Stop-MoqProcesses {
    taskkill /IM moq-sub.exe /F 2>$null
    taskkill /IM ffplay.exe /F 2>$null
    taskkill /IM moq-pub.exe /F 2>$null
    taskkill /IM ffmpeg.exe /F 2>$null
}

function Stop-ProcessTree {
    param(
        [Parameter(Mandatory=$true)]
        [int]$Pid
    )

    taskkill /PID $Pid /T /F 2>$null
}

# 시작 전 잔여 프로세스 정리
Stop-MoqProcesses
Start-Sleep -Seconds 3

foreach ($bitrate in $Bitrates) {
    $mbps = [int]($bitrate / 1000000)
    $label = "${mbps}M"
    $name = "${NameBase}_${label}"

    Write-Host ""
    Write-Host "=============================="
    Write-Host "Start experiment: $label"
    Write-Host "target-bitrate: $bitrate"
    Write-Host "name: $name"
    Write-Host "=============================="

    $probeLog = Join-Path $OutDir "subscriber_probe_${label}.csv"
    $frameLog = Join-Path $OutDir "frame_${label}.log"
    $subErr   = Join-Path $OutDir "sub_${label}.err"
    $pubErr   = Join-Path $OutDir "pub_${label}.err"

    # 기존 로그 삭제
    Remove-Item $probeLog -ErrorAction SilentlyContinue
    Remove-Item $frameLog -ErrorAction SilentlyContinue
    Remove-Item $subErr   -ErrorAction SilentlyContinue
    Remove-Item $pubErr   -ErrorAction SilentlyContinue

    # ===== pub 시작 =====
    # 주의: pub 코드는 기존 구조 유지, --name만 조건별로 변경
    $pubCmd = @"
ffmpeg -hide_banner -stream_loop -1 -re -i "$VideoPath" -map 0:v:0 -an -sn -dn -map_metadata -1 -map_chapters -1 -c:v copy -f mp4 -movflags empty_moov+frag_keyframe+separate_moof+omit_tfhd_offset -frag_duration 500000 -min_frag_duration 500000 - 2> "$pubErr" | target\debug\moq-pub.exe --name $name --tls-disable-verify $RelayUrl
"@

    $pubProc = Start-Process -FilePath "cmd.exe" `
        -ArgumentList "/d", "/c", $pubCmd `
        -PassThru `
        -WindowStyle Normal

    # pub announce 안정화 대기
    Start-Sleep -Seconds 5

    # ===== sub 시작 =====
    $subCmd = @"
set RUST_LOG=off&& target\debug\moq-sub.exe --name $name --tls-disable-verify --probe-enable --probe-target-bitrate $bitrate --probe-duration-ms 3000 --probe-epoch-ms 500 --probe-count 100 --probe-interval-ms 1000 --probe-padding-mode stream --probe-log "$probeLog" $RelayUrl 2> "$subErr" | ffplay -fflags nobuffer -flags low_delay -f mp4 -vf showinfo - 2> "$frameLog"
"@

    $subProc = Start-Process -FilePath "cmd.exe" `
        -ArgumentList "/d", "/c", $subCmd `
        -PassThru `
        -WindowStyle Normal

    Write-Host "Running $label for $RunSeconds seconds..."
    Start-Sleep -Seconds $RunSeconds

    Write-Host "Stopping $label..."

    # sub 쪽 먼저 종료
    if ($subProc -and !$subProc.HasExited) {
        Stop-ProcessTree -Pid $subProc.Id
    }

    Start-Sleep -Seconds 3

    # pub 쪽 종료
    if ($pubProc -and !$pubProc.HasExited) {
        Stop-ProcessTree -Pid $pubProc.Id
    }

    # 잔여 프로세스 정리
    Stop-MoqProcesses

    Write-Host "Saved:"
    Write-Host "  $probeLog"
    Write-Host "  $frameLog"
    Write-Host "  $subErr"
    Write-Host "  $pubErr"

    Write-Host "Waiting $RestartGapSeconds seconds before next run..."
    Start-Sleep -Seconds $RestartGapSeconds
}

Write-Host ""
Write-Host "All experiments completed."