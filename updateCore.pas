unit updateCore;

interface

uses
  SysUtils, Classes, Windows, Forms,
  IdHTTP,
  synacode,
  uniUtils, uniMessageBox, shellAPI,
  nativeXml;

const
  UPDATE_SETTINGS_FILE = 'UpdateSettigns.xml';
  APPLICATION_VERSION_FILE = 'AppVersion.xml';

  UPDATE_ROOT_DIR = 'gis_mgt_update';
  UPDATE_TEMP_DIR = 'update';

  DEFAULT_INSTALLER_FILE_NAME = 'copyandrun.exe';

type
  // ������ �������� ����������
  TUpdateCore = class
  private
    FProjectName: string;

    FSettingsDir: string;
    FAppDir: string;
    FUpdateRootDir: string;
    FUpdateTempDir: string;
    FInstallerFileName: string;
    FExecuteParams: string;

    FHost: string;
    FPort: integer;

    FVersion: integer;
	  FNeedReset: Boolean;

    procedure readSettings();

    procedure readVersion();
    procedure saveVersion(aDir: string; aVersion: integer);

    function  getCurrentVersionAsFormattedString(): string;
  public
    constructor Create(aAppDir, aSettingsDir, aInstallerFileName: string; aExecuteParams: string = '');

    function  getLastVersion(): integer;
    procedure getVersionList(aList: TStrings);
    function  getVersionAsFormattedString(aVersion: integer): string;    
    procedure update(aVersion: integer);

    //�������� ���������� ������ ���������������� ��������
    procedure saveResetDone();

    property Host: string read FHost write FHost;
    property Port: integer read FPort write FPort;

    property CurrentVersion: integer read FVersion;
    property NeedReset: Boolean read FNeedReset write FNeedReset;
    property CurrentVersionString: string read getCurrentVersionAsFormattedString;

    property ServerLastVersion: integer read getLastVersion;
  end;

implementation

uses tools7z;

{ TUpdateCore }

{**********************************************************************************************
* Create
***********************************************************************************************}
constructor TUpdateCore.Create(aAppDir, aSettingsDir, aInstallerFileName, aExecuteParams: string);
begin
  FSettingsDir := GetFullPathName(killTrailingSlash(aSettingsDir));
  FAppDir := GetFullPathName(killTrailingSlash(aAppDir));

  FUpdateRootDir := killTrailingSlash(GetTempDirectoryPath()) + '\' +
                    UPDATE_ROOT_DIR  + '\' +
                    IntToStr(abs(CRC32(FAppDir)));
  FUpdateTempDir := FUpdateRootDir + '\' + UPDATE_TEMP_DIR;

  FInstallerFileName := aInstallerFileName;
  FExecuteParams := aExecuteParams;

  FVersion := 0;
  FNeedReset := false;
  FProjectName := 'UNKNOWN';

  readSettings();
  readVersion();

  // ������� ��������� ���������� ����������
  ForceDirectories(FUpdateRootDir);
  
  DeleteDir(FUpdateTempDir);
end;

{**********************************************************************************************
* readSettings
***********************************************************************************************}
procedure TUpdateCore.readSettings();
var
  xml: TNativeXml;
  node: TXmlNode;
  fileName: string;
begin
  // �������� ������ ��� ����� ��������
  fileName := FSettingsDir + '\' + UPDATE_SETTINGS_FILE;

  if (not FileExists(fileName)) then
    raise Exception.Create('���� �������� ���������� �� ����������: ' + fileName);

  xml := TNativeXml.Create();

  // ������ ���� ��������
  try
    try
      xml.LoadFromFile(fileName);

    except
      raise Exception.Create('�� ������� ��������� ���� �������� ���������� ' + fileName);
      exit;
    end;

    // �������� ���� ��������
    node := xml.Root.NodeByName('project');

    if (node = nil) or
       (not node.HasAttribute('name')) then
      raise Exception.Create('����������� ���� ������� � ����� �������� ' + fileName);

    FProjectName := node.ReadAttributeString('name');

    // �������� ���� ��������
    node := xml.Root.NodeByName('server');

    if (node = nil) or
       (not node.HasAttribute('host')) or
       (not node.HasAttribute('port')) then
      raise Exception.Create('����������� ���� �������� ���������� � ����� �������� ' + fileName);

    // ��������� ���� � ����
    FHost := node.ReadAttributeString('host');
    FPort := node.ReadAttributeInteger('port');

  finally
    FreeAndNil(xml);
  end;
end;

{**********************************************************************************************
* readVersion
***********************************************************************************************}
procedure TUpdateCore.readVersion();
var
  xml: TNativeXml;
  node: TXmlNode;
  fileName: string;
  needreset: Integer;
begin
  // �������� ������ ��� �����
  fileName := FAppDir + '\' + APPLICATION_VERSION_FILE;

  if (not FileExists(fileName)) then
    exit;

  xml := TNativeXml.Create();

  // ������ ����
  try
    xml.LoadFromFile(fileName);

    // �������� ����
    node := xml.Root.NodeByName('app');

    if (node = nil) or
       (not node.HasAttribute('version')) then
      exit;

    // ��������� ���� � ����
    FVersion := node.ReadAttributeInteger('version');

    //��������� ���� ������ ���������������� ��������
    if (node.HasAttribute('needreset')) then
    begin
      // ��������� ���� � ����
      needreset := node.ReadAttributeInteger('needreset');
      //���� � AppVersion needupdate = 1 - �� ��� ������� ������������� �������� 
      if (needreset = 1) then
        FNeedReset := True
      else
        FNeedReset := false;
    end;

  finally
    FreeAndNil(xml);
  end;
end;

{**********************************************************************************************
* getVersionAsFormattedString
***********************************************************************************************}
function TUpdateCore.getVersionAsFormattedString(aVersion: integer): string;
const
  VERSION_TPL = '%s.%s.%s.%s';
begin
  if (aVersion = 0) then
  begin
    Result := Format(VERSION_TPL, [0, 0, 0, 0]);
    exit;
  end;

  Result := IntToStr(aVersion);
  Result := Format(VERSION_TPL, [Result[1], Result[2], Result[3], Result[4]]);
end;

{**********************************************************************************************
* getCurrentVersionAsFormatedString
***********************************************************************************************}
function TUpdateCore.getCurrentVersionAsFormattedString(): string;
begin
  Result := getVersionAsFormattedString(FVersion);
end;

{**********************************************************************************************
* saveVersion
***********************************************************************************************}
procedure TUpdateCore.saveVersion(aDir: string; aVersion: integer);
var
  xml: TNativeXml;
  node: TXmlNode;
  index: integer;
begin
  if (not DirectoryExists(aDir)) then
    raise Exception.Create('��������� ��� ���������� ���������� �����������: ' + aDir);

  xml := TNativeXml.Create();

  try
    xml.EncodingString := 'windows-1251';
    xml.Root.Name := 'UpdateInfo';

    index := xml.Root.NodeAdd(TXmlNode.Create(xml));
    node := xml.Root.Nodes[index];
    node.Name := 'app';

    node.WriteAttributeInteger('version', aVersion);
    
    //��������� �� ������������� ������ ���������������� �������� ����� ����������
    node.WriteAttributeInteger('needreset', 1);

    xml.SaveToFile(aDir + '\' + APPLICATION_VERSION_FILE);
  finally
    xml.Free();
  end;
end;

{**********************************************************************************************
* saveResetDone
***********************************************************************************************}
procedure TUpdateCore.saveResetDone();
var
  xml: TNativeXml;
  node: TXmlNode;
  index: integer;
begin
  if (not DirectoryExists(FAppDir)) then
    raise Exception.Create('����������� �������� ����������: ' + FAppDir);

  xml := TNativeXml.Create();

  try
    xml.EncodingString := 'windows-1251';
    xml.Root.Name := 'UpdateInfo';

    index := xml.Root.NodeAdd(TXmlNode.Create(xml));
    node := xml.Root.Nodes[index];
    node.Name := 'app';

    node.WriteAttributeInteger('version', CurrentVersion);
    
    //��������, ��� ����� �������� ������� ���������
    node.WriteAttributeInteger('needreset', 0);

    xml.SaveToFile(FAppDir + '\' + APPLICATION_VERSION_FILE);

  finally
    xml.Free();
  end;
end;

{**********************************************************************************************
* getLastVersion
***********************************************************************************************}
function TUpdateCore.getLastVersion(): integer;
var
  httpClient: TIdHTTP;
  url: string;
begin
  Result := 0;

  url := 'http://' + FHost + ':' + IntToStr(FPort) + '/getlastversion?project=' + FProjectName;
  httpClient := TIdHTTP.Create(nil);

  try
    try
      httpClient.Get(url);

      if httpClient.ResponseCode <> 200 then
        raise Exception.Create('�� ������� �������� ��������� ������ � ������� ����������, ����� �������: ' +
                               IntToStr(httpClient.ResponseCode) + '. ' + httpClient.ResponseText);

      Result := StrToInt(httpClient.Response.RawHeaders.Values['version']);

    except
      on e: EIdHTTPProtocolException do
        raise Exception.Create('�� ������� �������� ��������� ������ � ������� ����������, ����� �������: ' +
                               e.ErrorMessage + ' (' + e.Message + ')');

    end;

  finally
    FreeAndNil(httpClient);
  end;
end;

{**********************************************************************************************
* getVersionList
***********************************************************************************************}
procedure TUpdateCore.getVersionList(aList: TStrings);
var
  httpClient: TIdHTTP;
  url: string;
begin
  if (aList = nil) then
    raise Exception.Create('���������� ������ ��� ���������� ��������� ������ ����.');

  aList.Clear();

  url := 'http://' + FHost + ':' + IntToStr(FPort) + '/getversionlist?project=' + FProjectName;
  httpClient := TIdHTTP.Create(nil);

  try
    try
      httpClient.Get(url);

      if httpClient.ResponseCode <> 200 then
        raise Exception.Create('�� ������� �������� ������ ������ � ������� ����������, ����� �������: ' +
                               IntToStr(httpClient.ResponseCode) + '. ' + httpClient.ResponseText);

      aList.CommaText := httpClient.Response.RawHeaders.Values['versionlist'];

    except
      on e: EIdHTTPProtocolException do
        raise Exception.Create('�� ������� �������� ������ ������ � ������� ����������, ����� �������: ' +
                               e.ErrorMessage + ' (' + e.Message + ')');
    end;

  finally
    FreeAndNil(httpClient);
  end;
end;

{**********************************************************************************************
* update
***********************************************************************************************}
procedure TUpdateCore.update(aVersion: integer);
var
  httpClient: TIdHTTP;
  url: string;
  fileName: string;
  oStream: TFileStream;
begin
  // �������� ����
  httpClient := TIdHTTP.Create(nil);
  httpClient.ReadTimeout := 2000;

  url := 'http://' + FHost + ':' + IntToStr(FPort) + '/getfile?project=' + FProjectName + '&version=' + IntToStr(aVersion);
  httpClient := TIdHTTP.Create(nil);

  try
    fileName := FUpdateRootDir + '\' + FormatDateTime('yyyy_mm_dd__hh_nn_ss', Now()) + '_' + IntToStr(aVersion) + '.7z';
    ostream := TFileStream.Create(fileName, fmCreate);

    try
      httpClient.Get(url, ostream);

    finally
      FreeAndNil(ostream);
    end;

    // ���� ������ ������� ���-�� ��������
    if httpClient.ResponseCode <> 200 then
    begin
      DeleteFile(PChar(fileName));
      raise Exception.Create('�� ������� �������� ������������� ������ � ������� ����������, ����� �������: ' +
                             IntToStr(httpClient.ResponseCode) + '. ' + httpClient.ResponseText);
    end;

  finally
    FreeAndNil(httpClient);
  end;

  // ��������������� ���� � ����������
  DeleteDir(FUpdateTempDir);
  ForceDirectories(FUpdateTempDir);
  decompress7Z(fileName, FUpdateTempDir);
  DeleteFile(PChar(fileName));

  // �������� � ���������� ���� ������
  saveVersion(FUpdateTempDir, aVersion);

  fileName := FUpdateTempDir + '\' + FInstallerFileName;

  if (not FileExists(fileName)) then
  begin
    fileName := FAppDir + '\' + FInstallerFileName;

    if (not FileExists(fileName)) then
      Exception.Create('�� ������� ����� ����������� ���� ��������� ����������: ' + FInstallerFileName);
  end;

  // ��������� ����������
  ShellExecute(0, nil, PChar(fileName),
               PChar(AnsiQuotedStr(FUpdateTempDir, '"') + ' ' +
               AnsiQuotedStr(FAppDir, '"') + ' ' +
               AnsiQuotedStr(ExtractFileName(ParamStr(0)), '"') + ' ' +
               FExecuteParams), nil, SW_HIDE);

  // ������� ���������� ��� ����������
  Application.Terminate();
end;

end.
