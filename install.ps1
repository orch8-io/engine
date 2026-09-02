param(
  [string]$Version = "latest",
  [string]$InstallDir = "$env:LOCALAPPDATA\Orch8\bin"
)
$ErrorActionPreference = "Stop"
$repo = "orch8-io/engine"
if ($Version -eq "latest") {
  $Version = (Invoke-RestMethod "https://api.github.com/repos/$repo/releases/latest").tag_name
}
if (-not $Version.StartsWith("v")) { $Version = "v$Version" }
$target = "x86_64-pc-windows-msvc"
$archive = "orch8-$Version-$target.zip"
$base = "https://github.com/$repo/releases/download/$Version"
$temp = Join-Path ([IO.Path]::GetTempPath()) ([Guid]::NewGuid())
New-Item -ItemType Directory -Path $temp | Out-Null
try {
  Invoke-WebRequest "$base/$archive" -OutFile "$temp\$archive"
  Invoke-WebRequest "$base/$archive.sha256" -OutFile "$temp\$archive.sha256"
  $expected = (Get-Content "$temp\$archive.sha256").Split(' ')[0].Trim().ToLowerInvariant()
  $actual = (Get-FileHash "$temp\$archive" -Algorithm SHA256).Hash.ToLowerInvariant()
  if ($actual -ne $expected) { throw "checksum mismatch for $archive" }
  Expand-Archive "$temp\$archive" -DestinationPath $temp
  New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
  Get-ChildItem $temp -Recurse -Filter "orch8*.exe" | Copy-Item -Destination $InstallDir
  Write-Host "Installed Orch8 $Version to $InstallDir"
  Write-Host "Add $InstallDir to PATH if it is not already present."
} finally {
  Remove-Item -Recurse -Force $temp -ErrorAction SilentlyContinue
}
