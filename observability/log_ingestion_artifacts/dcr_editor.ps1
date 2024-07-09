param ([Parameter(Mandatory=$true)] $ResourceId)

# get DCR content and put into a file
$FilePath = "temp.dcr"
$DCR = Invoke-AzRestMethod -Path ("$ResourceId"+"?api-version=2021-09-01-preview") -Method GET
$DCR.Content | ConvertFrom-Json | ConvertTo-Json -Depth 20 | Out-File $FilePath

# Open DCR in code editor
code $FilePath | Wait-Process

#Wait for confirmation to apply changes
$Output = Read-Host "Apply changes to DCR (Y/N)? "
if ("Y" -eq $Output.toupper())
{ 
	#write DCR content back from the file
	$DCRContent = Get-Content $FilePath -Raw
	Invoke-AzRestMethod -Path ("$ResourceId"+"?api-version=2021-09-01-preview") -Method PUT -Payload $DCRContent		
}

#Delete temporary file
Remove-Item $FilePath