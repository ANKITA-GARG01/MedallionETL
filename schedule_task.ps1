# =============================================================
#  schedule_task.ps1  —  Registers the ETL pipeline in
#  Windows Task Scheduler
#
#  Run once as Administrator:
#  Right-click PowerShell → Run as Administrator
#  Then: .\schedule_task.ps1
#
#  Default schedule: every day at 08:00 AM
#  Change $RunTime below to your preferred time
# =============================================================

$TaskName   = "MedallionETL_Pipeline"
$BatFile    = "C:\MedallionETL\run_pipeline.bat"
$RunTime    = "08:00"                # 24hr format — change as needed
$RunAsUser  = $env:USERDOMAIN + "\" + $env:USERNAME   # your Windows login

Write-Host ""
Write-Host "Registering Task Scheduler job..." -ForegroundColor Cyan
Write-Host "  Task name : $TaskName"
Write-Host "  Script    : $BatFile"
Write-Host "  Schedule  : Daily at $RunTime"
Write-Host "  Run as    : $RunAsUser"
Write-Host ""

# Remove existing task if it exists
if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "Removed existing task." -ForegroundColor Yellow
}

# Define the action — run the .bat file
$Action = New-ScheduledTaskAction `
    -Execute "cmd.exe" `
    -Argument "/c `"$BatFile`""

# Define the trigger — daily at $RunTime
$Trigger = New-ScheduledTaskTrigger `
    -Daily `
    -At $RunTime

# Define settings
$Settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Hours 2) `
    -RestartCount 2 `
    -RestartInterval (New-TimeSpan -Minutes 5) `
    -StartWhenAvailable                          `
    -RunOnlyIfNetworkAvailable:$false

# Register the task
Register-ScheduledTask `
    -TaskName $TaskName `
    -Action   $Action   `
    -Trigger  $Trigger  `
    -Settings $Settings `
    -RunLevel Highest   `
    -Force

Write-Host ""
Write-Host "Task registered successfully." -ForegroundColor Green
Write-Host ""
Write-Host "Useful commands:" -ForegroundColor Cyan
Write-Host "  Run now    : Start-ScheduledTask -TaskName '$TaskName'"
Write-Host "  Check status: Get-ScheduledTask  -TaskName '$TaskName' | Select-Object TaskName,State"
Write-Host "  View log   : Get-Content C:\MedallionETL\logs\scheduler.log -Tail 30"
Write-Host "  Remove task: Unregister-ScheduledTask -TaskName '$TaskName' -Confirm:`$false"
Write-Host ""
