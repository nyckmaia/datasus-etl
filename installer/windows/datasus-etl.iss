; Inno Setup script for DataSUS ETL.
; Compile with: iscc installer\windows\datasus-etl.iss /DAppVersion=X.Y.Z
; Expects the Nuitka standalone dir at dist\datasus-etl.dist\.

#ifndef AppVersion
  #define AppVersion "0.0.0"
#endif

#define AppName        "DataSUS ETL"
#define AppPublisher   "Nycholas Maia"
#define AppURL         "https://github.com/nyckmaia/datasus-etl"
#define AppExe         "datasus.exe"
#define SourceDistDir  "..\..\dist\datasus-etl.dist"
#define IconFile       "..\icons\icon.ico"

[Setup]
; Keep AppId stable across versions so upgrades replace (not duplicate) the
; install. Regenerate only if you want a separate product.
AppId={{9C4F8E2E-7D3A-4B5C-8E1A-2A3F2B7C9D10}
AppName={#AppName}
AppVersion={#AppVersion}
AppVerName={#AppName} {#AppVersion}
AppPublisher={#AppPublisher}
AppPublisherURL={#AppURL}
AppSupportURL={#AppURL}/issues
AppUpdatesURL={#AppURL}/releases
DefaultDirName={autopf}\DataSUS ETL
DefaultGroupName={#AppName}
DisableProgramGroupPage=yes
OutputDir=..\..\dist
OutputBaseFilename=datasus-etl-{#AppVersion}-windows-x64
SetupIconFile={#IconFile}
Compression=lzma2/ultra64
SolidCompression=yes
WizardStyle=modern
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
PrivilegesRequired=lowest
PrivilegesRequiredOverridesAllowed=dialog commandline
ChangesEnvironment=yes
UninstallDisplayIcon={app}\{#AppExe}
UninstallDisplayName={#AppName} {#AppVersion}

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"
Name: "portuguese"; MessagesFile: "compiler:Languages\BrazilianPortuguese.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: checkedonce
Name: "addtopath"; Description: "Add datasus CLI to PATH"; GroupDescription: "Integration:"; Flags: checkedonce

[Files]
Source: "{#SourceDistDir}\*"; DestDir: "{app}"; Flags: recursesubdirs createallsubdirs ignoreversion

[Icons]
Name: "{group}\{#AppName}"; Filename: "{app}\{#AppExe}"; Parameters: "ui"; WorkingDir: "{app}"; IconFilename: "{app}\{#AppExe}"
Name: "{group}\Uninstall {#AppName}"; Filename: "{uninstallexe}"
Name: "{autodesktop}\{#AppName}"; Filename: "{app}\{#AppExe}"; Parameters: "ui"; WorkingDir: "{app}"; IconFilename: "{app}\{#AppExe}"; Tasks: desktopicon

[Run]
Filename: "{app}\{#AppExe}"; Parameters: "ui"; Description: "{cm:LaunchProgram,{#AppName}}"; Flags: nowait postinstall skipifsilent

; ---------------------------------------------------------------------------
; PATH integration (Tasks.addtopath). We mutate HKCU\Environment\Path so no
; admin rights are required. SendMessage broadcasts the change so new shells
; pick it up without a reboot.
; ---------------------------------------------------------------------------
[Code]
const
  EnvKey = 'Environment';

function NeedsAddPath(const Param: string): Boolean;
var
  OrigPath: string;
begin
  if not RegQueryStringValue(HKEY_CURRENT_USER, EnvKey, 'Path', OrigPath) then
  begin
    Result := True;
    exit;
  end;
  Result := Pos(';' + Uppercase(Param) + ';', ';' + Uppercase(OrigPath) + ';') = 0;
end;

procedure AddPath(const Param: string);
var
  OrigPath: string;
begin
  if not RegQueryStringValue(HKEY_CURRENT_USER, EnvKey, 'Path', OrigPath) then
    OrigPath := '';
  if NeedsAddPath(Param) then
  begin
    if (OrigPath = '') then
      OrigPath := Param
    else
      OrigPath := OrigPath + ';' + Param;
    RegWriteExpandStringValue(HKEY_CURRENT_USER, EnvKey, 'Path', OrigPath);
  end;
end;

procedure RemovePath(const Param: string);
var
  OrigPath: string;
  P: Integer;
begin
  if not RegQueryStringValue(HKEY_CURRENT_USER, EnvKey, 'Path', OrigPath) then
    exit;
  P := Pos(';' + Uppercase(Param), ';' + Uppercase(OrigPath));
  if P = 0 then
  begin
    P := Pos(Uppercase(Param) + ';', Uppercase(OrigPath));
    if P = 0 then exit;
  end
  else
    P := P - 1;
  Delete(OrigPath, P, Length(Param) + 1);
  RegWriteExpandStringValue(HKEY_CURRENT_USER, EnvKey, 'Path', OrigPath);
end;

procedure CurStepChanged(CurStep: TSetupStep);
begin
  if (CurStep = ssPostInstall) and WizardIsTaskSelected('addtopath') then
    AddPath(ExpandConstant('{app}'));
end;

procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
begin
  if CurUninstallStep = usUninstall then
    RemovePath(ExpandConstant('{app}'));
end;
