# Script cleanup logs cũ
# Chạy: powershell -ExecutionPolicy Bypass -File CLEANUP_LOGS.ps1

Write-Host "=" -NoNewline -ForegroundColor Cyan
Write-Host ("=" * 79) -ForegroundColor Cyan
Write-Host "CLEANUP OLD LOGS" -ForegroundColor Yellow
Write-Host "=" -NoNewline -ForegroundColor Cyan
Write-Host ("=" * 79) -ForegroundColor Cyan

$logsDir = "logs"

# Đếm số folder run_*
$runFolders = Get-ChildItem "$logsDir\run_*" -Directory -ErrorAction SilentlyContinue
$totalRuns = $runFolders.Count

Write-Host "`nTổng số lần chạy: $totalRuns" -ForegroundColor White

if ($totalRuns -eq 0) {
    Write-Host "Không có log nào để cleanup" -ForegroundColor Yellow
    exit
}

# Hiển thị menu
Write-Host "`nChọn hành động:" -ForegroundColor Cyan
Write-Host "1. Xóa logs cũ hơn 7 ngày" -ForegroundColor White
Write-Host "2. Xóa logs cũ hơn 30 ngày" -ForegroundColor White
Write-Host "3. Giữ lại 10 lần chạy gần nhất" -ForegroundColor White
Write-Host "4. Giữ lại 5 lần chạy gần nhất" -ForegroundColor White
Write-Host "5. Xóa tất cả logs (giữ lại folder)" -ForegroundColor Red
Write-Host "0. Thoát" -ForegroundColor Gray

$choice = Read-Host "`nNhập lựa chọn"

switch ($choice) {
    "1" {
        Write-Host "`nXóa logs cũ hơn 7 ngày..." -ForegroundColor Yellow
        $cutoffDate = (Get-Date).AddDays(-7)
        $toDelete = $runFolders | Where-Object { $_.CreationTime -lt $cutoffDate }
        
        if ($toDelete.Count -eq 0) {
            Write-Host "Không có log nào cũ hơn 7 ngày" -ForegroundColor Green
        } else {
            Write-Host "Sẽ xóa $($toDelete.Count) folders:" -ForegroundColor Yellow
            $toDelete | ForEach-Object { Write-Host "  - $($_.Name)" -ForegroundColor Gray }
            
            $confirm = Read-Host "`nXác nhận xóa? (y/n)"
            if ($confirm -eq "y") {
                $toDelete | Remove-Item -Recurse -Force
                Write-Host "✅ Đã xóa $($toDelete.Count) folders" -ForegroundColor Green
            } else {
                Write-Host "Đã hủy" -ForegroundColor Yellow
            }
        }
    }
    
    "2" {
        Write-Host "`nXóa logs cũ hơn 30 ngày..." -ForegroundColor Yellow
        $cutoffDate = (Get-Date).AddDays(-30)
        $toDelete = $runFolders | Where-Object { $_.CreationTime -lt $cutoffDate }
        
        if ($toDelete.Count -eq 0) {
            Write-Host "Không có log nào cũ hơn 30 ngày" -ForegroundColor Green
        } else {
            Write-Host "Sẽ xóa $($toDelete.Count) folders:" -ForegroundColor Yellow
            $toDelete | ForEach-Object { Write-Host "  - $($_.Name)" -ForegroundColor Gray }
            
            $confirm = Read-Host "`nXác nhận xóa? (y/n)"
            if ($confirm -eq "y") {
                $toDelete | Remove-Item -Recurse -Force
                Write-Host "✅ Đã xóa $($toDelete.Count) folders" -ForegroundColor Green
            } else {
                Write-Host "Đã hủy" -ForegroundColor Yellow
            }
        }
    }
    
    "3" {
        Write-Host "`nGiữ lại 10 lần chạy gần nhất..." -ForegroundColor Yellow
        $toKeep = 10
        $sorted = $runFolders | Sort-Object CreationTime -Descending
        $toDelete = $sorted | Select-Object -Skip $toKeep
        
        if ($toDelete.Count -eq 0) {
            Write-Host "Chỉ có $totalRuns lần chạy, không cần xóa" -ForegroundColor Green
        } else {
            Write-Host "Sẽ xóa $($toDelete.Count) folders (giữ lại $toKeep gần nhất):" -ForegroundColor Yellow
            $toDelete | ForEach-Object { Write-Host "  - $($_.Name)" -ForegroundColor Gray }
            
            $confirm = Read-Host "`nXác nhận xóa? (y/n)"
            if ($confirm -eq "y") {
                $toDelete | Remove-Item -Recurse -Force
                Write-Host "✅ Đã xóa $($toDelete.Count) folders" -ForegroundColor Green
            } else {
                Write-Host "Đã hủy" -ForegroundColor Yellow
            }
        }
    }
    
    "4" {
        Write-Host "`nGiữ lại 5 lần chạy gần nhất..." -ForegroundColor Yellow
        $toKeep = 5
        $sorted = $runFolders | Sort-Object CreationTime -Descending
        $toDelete = $sorted | Select-Object -Skip $toKeep
        
        if ($toDelete.Count -eq 0) {
            Write-Host "Chỉ có $totalRuns lần chạy, không cần xóa" -ForegroundColor Green
        } else {
            Write-Host "Sẽ xóa $($toDelete.Count) folders (giữ lại $toKeep gần nhất):" -ForegroundColor Yellow
            $toDelete | ForEach-Object { Write-Host "  - $($_.Name)" -ForegroundColor Gray }
            
            $confirm = Read-Host "`nXác nhận xóa? (y/n)"
            if ($confirm -eq "y") {
                $toDelete | Remove-Item -Recurse -Force
                Write-Host "✅ Đã xóa $($toDelete.Count) folders" -ForegroundColor Green
            } else {
                Write-Host "Đã hủy" -ForegroundColor Yellow
            }
        }
    }
    
    "5" {
        Write-Host "`n⚠️  CẢNH BÁO: Sẽ xóa TẤT CẢ logs!" -ForegroundColor Red
        $confirm = Read-Host "Xác nhận xóa TẤT CẢ? (yes để xác nhận)"
        if ($confirm -eq "yes") {
            $runFolders | Remove-Item -Recurse -Force
            Write-Host "✅ Đã xóa tất cả $totalRuns folders" -ForegroundColor Green
        } else {
            Write-Host "Đã hủy" -ForegroundColor Yellow
        }
    }
    
    "0" {
        Write-Host "Thoát" -ForegroundColor Gray
    }
    
    default {
        Write-Host "Lựa chọn không hợp lệ" -ForegroundColor Red
    }
}

Write-Host "`n" -NoNewline
Write-Host "=" -NoNewline -ForegroundColor Cyan
Write-Host ("=" * 79) -ForegroundColor Cyan
