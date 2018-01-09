unit mgtCore;

interface

uses
  Classes, Controls, Forms, Windows, Graphics, SysUtils, dialogs, Math, ShellAPI,
  sysAppInfo, uJSON, toolsJSON, Variants, dxGDIPlusClasses,
  cxFilter, dxCore, cxDBData, cxDBLookupComboBox, cxLookupDBGrid, cxDropDownEdit,
  DXVCLGridsUtils, DXVCLExtendedGrid, dxBar,
  Mappl,
  nativeXml, sqlLoader, idHttp,
  uniUtils, uniMessageBox, uniHashContainer, uniConnectionContainer, toolsString,
  maps, mapsSys,
  sysLogger,
  dbaCore, dbConn, dbaTypes, dbaTools, toolsFile,
  AuthTypes, AuthParams, AuthAuthorization, frmDXAuthorization, AuthAdministration, AuthUsers, AuthRoles, AuthMGTPermissions,
  trsTicketGISMGT, trsAdminAPI, trsClientAPI, trsTypes, trsObjectDataMap, trsObject, trsObjectData, trsObjectDataMappl,
  trsObjectGISMGT, trsTicket, trsObjectLink, trsAttachment,
  mgtTypes, mgtEntities, mgtDataModule, mgtClasses, mgtStartupImage, mgtMail,
  mapObject, MapplTypes, toolsTime, uniBaseTypes,
  updateCore, mgtRouteTrajectory, mgtAttachmentManager, uniClause, MapplFieldTypes, mgtSplash,
  corelGenerator, trafaretStopNames
;

const
  GRID_SELECT_PREFIX = 'Select';
  MAP_SELECT_PREFIX = 'Map';

  // �������� �����, ������� ��������� get_objects
  OBJECT_IN_TICKET = '__in_ticket';         // ������ � ������
  OBJECT_OPERATION = '__object_operation';  // �������� ��� ��������

  // ������ ��� �������������� (��� �������� Visible ��������� Devexpress)
  // �� boolean � ������������ ��� TDxBarItemVisible
  mapBoolean2TDxBarItemVisible: array[boolean] of TDxBarItemVisible = (ivNever, ivAlways);

type
  //TCardOperations = array[EMgtCard] of TtrsOperationSet;
  // = array[TMgtDatasource] of TtrsOperationSet;

{**********************************************************************************************
* TMgtCore
* ���� ���������� ��� ����������� -
* ������ � ���� ��������� ���������, ��������� Mappl � ����������� � �.�.
***********************************************************************************************}
  TMgtCore = class
  private
    // ���� ������������� ����������
//    FStartupImage: TFMgtStartupImage;

    // ���������� ����������
    FUpdateCore: TUpdateCore;

    // ������ ����������
    FAppVersion: string;

    // �������� ������ ���������� (�� �������� �������) - ����������� �� Settings.xml
    FflTest: boolean;

    // �����������
    FAuthSettings: TExtAuthSettings;
    FAuth: TFExtDXAuthorization;
    FUserPermissions: TMGTPermissions;

    // �����
    FMail: TMailSettings;

    // ��������� ������� � ������ �������� ���������� � ���������������� �����������
    FExpParamsServiceHost : string;
    FExpParamsServicePort : integer;
    // ������ ��� ������� � ������ �������� ��
    httpClient: TIdHTTP;

    // ������ �����
    FMapProject: string;

    // ���������� � ��
    FDbaCore: TDbaCore;
    FConn: TDbaConnection;

    // ����� ��� ������ �������� �� ����� �� ������
    FsqlLoader: TSqlLoader;

    // ��������� �������
    FsqlParams: TMapStrings;

    // ��� ��� ���������� �������� �� ��������� �����
    FmapSQL: TMapStrings;

    // �����, �������������� �������
    FAdmin         : TExtAdministration;
    FAdminEngine   : TtrsAdmin;
    FClientEngine  : TtrsClient;
    FTicket        : TtrsGISMGTTicket;

    // ������ ������������ ��������
    FCommitedTicketList: TStrings;

    // ������� ������� ������
    FOnTicketCommit: TNotifyEvent;

    // ������� ��������� ������ ������
    FOnTicketChange: TtrsTicketChangedEvent;

    // ��������� ������� � ������ ������
    FbufferCardAlias : string;      // ����� �������� � ������
    FbufferObjectMuid : int64;

    // ��������� ����������
    FMapTrajectories: TMapObjects;
    // ��������� ��������������� ������������
    FMapRefbooks: TMapInt64;

    // ��������� ��������� ����������� ������������ ����������
    FPostTrajUniqueMuids: TMapInt64;

    // ��������� ��� �������� ����������, ��� ���������� �����.
    FMapViewFilterTrajContainer: TMapInt64;

  private   // functions
    procedure readSettings();

    // ��������� ����� MyConnections.xml
    procedure OnAuthSetSelectedConnStringHandler(aConnAliases: TConnectionAliases; var aConnAlias: string);

    // �������� ������ � �����
    function  AddObjectToTicketInternal(aDataSource: TMgtDatasource; aMuid: Int64; aOperation: TtrsOperation;
                                        aFlSilent: Boolean = false): TtrsGISMGTObject; overload;
    function  AddObjectToTicketInternal(aDataSource: TMgtDatasource; aMuid: Int64; aOperation: TtrsOperation; aInitialData: string;
                                aVersion: Integer): TtrsGISMGTObject; overload;
    function  AddObjectToTicketInternal(aRefbookMuid, aMuid: Int64; aOperation: TtrsOperation; aInitialData: string;
                                aVersion: Integer): TtrsGISMGTObject; overload;

    // ��������� ������� ����� ��� ����������� � ������
    // P.S. ���� �����, �� ��������� ����� ������� � ��������� �����
    function  validateObjectBeforeAdd(aDataSource: TMgtDatasource; aMuid: Int64; aOperation: TtrsOperation): boolean;
    function  validateReferenceBeforeDelete(aDataSource: TMgtDatasource; aMuid: Int64; vList: TMapIntegers): integer;
    function  validateStopBeforeDelete(aMuid: Int64; vList: TMapIntegers): boolean;
    function  validateStopPlaceBeforeDelete(aMuid: Int64; vList: TMapIntegers): boolean;
    function  validateRouteVariantBeforeDelete(aMuid: Int64; vList: TMapIntegers): boolean;
    function  validateTerminalPointZoneBeforeDelete(aMuid: Int64; vList: TMapIntegers): boolean;
    function  validateTerminalStationBeforeDelete(aMuid: Int64; vList: TMapIntegers): boolean;
    function  validateOrderBeforeDelete(aMuid: Int64; vList: TMapIntegers): boolean;
    function  validateStopPavilionOrderBeforeDelete(aMuid: Int64; vList: TMapIntegers): boolean;
    function  validateCompensatoryPointBeforeDelete(aMuid: Int64; vList: TMapIntegers): boolean;

    procedure ClearMapTrajectories();

    procedure addSectionToList(aSectionMuid: int64; vStrDirList: TMapIntegers);
    procedure addNodeToList(aNodeMuid: int64; vStrDirList: TMapIntegers);

    // ��������������� ����������
    procedure AddMapRouteStoredFiltration();
    procedure AddMapStopPlacesStoredFiltration();

    function  getFilterItemList(aRoot: TcxFilterCriteriaItemList; aDisplayValues: TMapStrings): TcxFilterCriteriaItemList;
    procedure AddGridRouteStoredFiltration();
    procedure AddGridStopStoredFiltration(aDataSet: TMgtDataset);
    procedure AddGridRouteTrajectoryStoredFiltration(aDataSet: TMgtDataset);
    procedure AddGridRouteNullTrajectoryStoredFiltration();
    procedure AddTOStoredFiltration(aDataSet: TMgtDataset); overload;

    // �������� ���������� � ��������� ��� ���������� �����
    procedure AddRTToMapFilterContainer (aRouteTrajectoryMuid: string);
    // ������� ���������� �� ����������
    procedure DeleteRTFromMapFilterContainer (aRouteTrajectoryMuid: string);

  public
    // ����������
    StoredFiltration: TMgtStoredFiltration;
	// ��� ������ ���� ???
    PopupMessage: string;
  public    // functions
    constructor Create();
    destructor  Destroy(); override;

    function  doAuth(): boolean;
    function  init(): boolean;
    function  update(): boolean;

    function  checkForUpdate(): boolean;

    procedure showPopupMessage(aText: string = '�������� �����������...');
    procedure hidePopupMessage();

    // �������� ������ �� ������ �� ������� ����� � �������������� �����������
    // ������� ���� (������� ����� ���� ������������ ������� ��������)
    function  getCoreSQL(aSqlAlias: string; aSqlParams: TMapStrings = nil): string;
    // ������� �� ��������� xml (������� ����� ���� ������������ ������� ��������)
    function  getSimpleSQL(aSqlAlias: string; aSqlParams: TMapStrings = nil; aXML:string = ''): string;
    // ������� ��� �������� ������� ��������
    function  getGridSQL(aDataSet: TMgtDataset; aSqlParams: TMapStrings = nil): string;
    // ������� ��� ������(?) �� �����
    function  getMapSQL(aDataSet: TMgtDataset; aMUID: int64; aMapPostfix: string = ''): string; overload;
    function  getMapSQL(aSqlAlias: string; aSqlParams: TMapStrings = nil): string; overload;
    // ������� ��� ��������� �� ����� ��������
    function  getMapObjectsSQL(aSqlAlias: string; aMUID: int64): string;
    // ������� ��� ����������� ���������������� �����������
    function  getExpParamsSQL(aSqlAlias: string; aSqlParams: TMapStrings = nil): string;

    function  getReportsSQL(aSqlAlias: string; aSqlParams: TMapStrings = nil): string;
    function  getCardsSQL(aSqlAlias: string; aSqlParams: TMapStrings = nil): string;
    function  getSignpostsSQL(aSqlAlias: string; aSqlParams: TMapStrings = nil): string;

    // �������� ����� ��� ���������� ����������
    function  getPrintPath(): string;

    // �������� ������ ����� ���������, ��������� �� ��� ���������
    function  getDatasourceFields(aDataSource: TMgtDatasource): TMapStrings;

    // �������� ������ �������� ��� ��������� ������� ��� ��������� �� �����
    procedure getObjectsForHighlight(var List: TMapInt64; aDataSet: TMgtDataset; aMUID: int64; aMapPostfix: string = '');

    // �������� ������ � �������
    function  ReCreateTicket(flAfterCommit: boolean = false): TtrsGISMGTTicket;
    function  LoadBadTicket(): TtrsGISMGTTicket;
    procedure DeleteTicket();
    function  CommitTicket(aTicket: TtrsGISMGTTicket = nil): boolean;

    procedure DoOnTicketChange(aChangeType: TtrsChangeType; aTicket: TtrsTicket; aObject: TtrsObject;
                               aObjectData: TtrsObjectData; aObjectLink: TtrsObjectLink; aAttachment: TtrsAttachment);


    // �������� ������ � ��������� �� ��������
    procedure loadLookupCombobox(aCombo: TcxLookupComboBox; aKeyField, aValueFields: string; aDataSet: TMgtDataset;
                                 aOnChange: TNotifyEvent = nil; aFlSilent: boolean = false);
    // ���������� OnChange ���������� ��� ����������
    procedure lookupComboboxPropertiesChangeSilent(Sender: TObject);
    procedure lookupComboboxPropertiesChange(Sender: TObject);

    // ��������� ���������� � MapTrajectories
    // �� ������ ������ ������� ��������� ������ ���� � �� ��� ������
    function  AddTrajectory(aMuid: int64; aTrajectoryType: EMgtRouteTrajectoryType = rttUndefined; aFlLoad: boolean = true): TMgtRouteTrajectory;
    // ����� ������� ���������� ������� � �����
    // ����������� ������� ����������� ������� � ������
    function  AddObjectToTicket(aDataSource: TMgtDatasource; aMuid: Int64; aOperation: TtrsOperation): TtrsGISMGTObject; overload;
    // ������ �������. ��������� ������ � ���������� �����, ��� �������
    function  AddObjectToTicketSimple(aTicket: TtrsGISMGTTicket; aDataSource: TMgtDatasource; aMuid: Int64;
                                      aOperation: TtrsOperation): TtrsGISMGTObject;

    // �������� ������ �� ������� �������
    procedure AddObjectLinks(aDataSource: TMgtDatasource; aMuid: Int64);
    // �������� ������ ������ �� ������ �������
    procedure AddObjectLink(aDataSourceFrom: TMgtDatasource; aMuidFrom: Int64;
                            aDataSourceTo: TMgtDatasource; aMuidTo: Int64; aLinkType: TtrsLinkTypeSet = [tltCascade, tltCommit]);
    // ������� ������ �� ������ ������� �� ����������
    procedure DeleteObjectLink(aDataSourceFrom: TMgtDatasource; aMuidFrom: Int64;
                               aDataSourceTo: TMgtDatasource; aMuidTo: Int64);

    // �������� ������ �� ����� �������-������� � �����������
    procedure AddStopPlaceTrajectoryObjectLinks(aStopPlaceMuid: Int64);
    // �������� ������ �� ����� �������-������� � ���������� ����������
    procedure AddStopPlaceTrajectoryObjectLink(aStopPlaceMuid, aTrajectoryMuid: Int64);
    // �������� ������ �� ���� ����� � ���������� ����������
    procedure AddGraphSectionTrajectoryObjectLink(aGraphSectionMuid, aTrajectoryMuid: Int64);

    // �������� slave data � �������
    procedure AddSlaveDataToTicket(aObject: TtrsGISMGTObject; aDataSource: TMgtDatasource; aMuid: Int64; aOperation: TtrsOperation;
                                  aData: TMapStrings);
    procedure DeleteObjectFromTicket(aDataSource: TMgtDatasource; aMuid: Int64);

    // �������� ������ ������ � ������� �� ���������
    function  GetFirstTicketObjectByDatasource(aDatasource: TMgtDatasource): TtrsGISMGTObject;

    // ����c��� �������� ���� � �����
    // ���������
    procedure setFieldValue(aObjectData: TtrsObjectData; aField: string; aValue: string; aFlNullable: boolean = true); overload;
    // �������������
    procedure setFieldValue(aObjectData: TtrsObjectData; aField: string; aValue: integer;
                            aFlSigned: boolean = true; aFl0AsNull: boolean = false); overload;
    // ���������
    procedure setFieldValue(aObjectData: TtrsObjectData; aField: string; aValue: boolean); overload;
    // ��� ������ (�������� <=0 �������� �� NULL)
    procedure setFieldValue(aObjectData: TtrsObjectData; aField: string; aValue: int64); overload;
    // ����
    procedure setFieldValue(aObjectData: TtrsObjectData; aField: string; aValue: TDateTime; aFlOnlyDate: boolean = false); overload;

    // �������� ������ �������� (� �������� ��������� �����) �� ���������� ������� �� ��
    function  getObjectsFromDB(aDataSource: TMgtDatasource; aSqlCondition : string; aFields: array of string) : TlkJSONlist;

    // �������� ������ �������� (� �������� ��������� �����) �� ���������� �������
    // �� �� ��������� ��������� �� ������, ���� ����
    // ����������� ���������� � ������ ����� 'muid'
    // ���� aFlKeepDeleted = true, �� ������� � ������ �� �������� �� ����� ����������� �� ������
    function getObjects(aDataSource: TMgtDatasource;  aConditionField, aConditionValue : string;
      aFields: array of string; aFlKeepDeleted : boolean = false;
      aSortField : string = ''; aSortMode: EMgtJsonListSortModes = jsmString) : TlkJSONlist;

    procedure mergeObjectsWithTicket(resObjectList: TlkJSONlist; aDataSource: TMgtDatasource;
                                     aConditionField, aConditionValue : string; aFields: array of string;
                                     aFlKeepDeleted : boolean = false);
    procedure sortJsonList(resObjectList: TlkJSONlist; aSortField : string; aSortMode: EMgtJsonListSortModes);

    // ��������� Initial Data ������� � ���� JSON ������
    function  getInitialData(aDataSource: TMgtDatasource; aMuid: int64; var vVersion: integer): String;

    function  getDelimitedText(aStringList: TStringList; aDelimiter: Char): string;

    // �������� �������� ���� ��� ���������� ������� �� ������
    // ����������, ������ �� ������
    function  getObjectValueFromTicket(aDataSource: TMgtDatasource; aMuid : int64; aField: string;
                                       var resValue: string) : boolean; overload;
    // ����������, ������ �� ������
    function  getObjectValueFromTicket(aTableName: string; aMuid : int64; aField: string;
                                       var resValue: string) : boolean; overload;

    // �������� �������� ����� ��� ���������� ������� �� ������
    // ����������, ������ �� ������
    function  getObjectValuesFromTicket(aDataSource: TMgtDatasource; aMuid : int64;
                                        FieldList: TMapStrings) : boolean; overload;
    // ����������, ������ �� ������
    function  getObjectValuesFromTicket(aTableName: string; aMuid : int64;
                                        FieldList: TMapStrings) : boolean; overload;

    // �������� �������� ���� ��� ���������� ������� �� ��
    // ����������, ������ �� ������
    function  getObjectValueFromDB(aDataSource: TMgtDatasource; aMuid : int64; aField: string;
                                   var resValue: string) : boolean; overload;
    // ����������, ������ �� ������
    function  getObjectValueFromDB(aTableName: string; aMuid : int64; aField: string;
                                   var resValue: string) : boolean; overload;

    // �������� �������� ����� ��� ���������� ������� �� ��
    // ����������, ������ �� ������
    function  getObjectValuesFromDB(aDataSource: TMgtDatasource; aMuid : int64;
                                    FieldList: TMapStrings) : boolean; overload;
    // ����������, ������ �� ������
    function  getObjectValuesFromDB(aTableName: string; aMuid : int64;
                                    FieldList: TMapStrings) : boolean; overload;

    // �������� �������� ���� ��� ���������� ������� (�� �� ��� ������, ���� ����)
    function  getObjectValue(aDataSource: TMgtDatasource; aMuid : int64; aField: string; aDefaultValue: string = '') : string; overload;

    // �������� �������� ���� ��� ���������� ������� (�� �� ��� ������, ���� ����)
    function  getObjectValue(aTableName: string; aMuid : int64; aField: string; aDefaultValue: string = '') : string; overload;

    // �������� �������� blob ���� ��� ���������� ������� (�� �� ��� ������, ���� ����) � �������� ���-� � �����
    function  getObjectValueBlob(aDataSource: TMgtDatasource; aMuid : int64; aField: string; var vStream: TMemoryStream) : boolean; overload;

    // �������� �������� blob ���� ��� ���������� ������� (�� �� ��� ������, ���� ����) � �������� ���-� � �����
    function  getObjectValueBlob(aTableName: string; aMuid : int64; aField: string; var vStream: TMemoryStream) : boolean; overload;

    // �������� �������� blob ���� ��� ���������� ������� �� ��
    // ����������, ������ �� ������
    function getObjectValueBlobFromDB(aDataSource: TMgtDatasource; aMuid: int64; aField: string; var vStream: TMemoryStream): boolean; overload;

    // �������� �������� blob ���� ��� ���������� ������� �� ��
    // ����������, ������ �� ������
    function getObjectValueBlobFromDB(aTableName: string; aMuid: int64; aField: string; var vStream: TMemoryStream): boolean; overload;

    // �������� �������� ����� ��� ���������� ������� (�� �� ��� ������, ���� ����)
    function  getObjectValues(aDataSource: TMgtDatasource; aMuid : int64; FieldList: TMapStrings) : boolean; overload;

    // �������� �������� ����� ��� ���������� ������� (�� �� ��� ������, ���� ����)
    function  getObjectValues(aTableName: string; aMuid : int64; FieldList: TMapStrings) : boolean; overload;

    // ��������� ������������� ������� � ��������� ��������������� � ��
    function isObjectExistsInDB(aDataSource: TMgtDatasource; aMuid : int64) : Boolean; overload;
    function isObjectExistsInDB(aTableName: string; aMuid : int64) : Boolean; overload;

    // �������� ������ ������������ ������� �� ��
    function  getObjectFullNameFromDB(aTableName: string; aMuid : int64): string;
    // �������� ������ ������������ �������
    function  getObjectFullName(aTableName: string; aMuid : int64): string; overload;
    // �������� ������ ������������ �������
    function  getObjectFullName(aDataSource: TMgtDatasource; aMuid : int64): string; overload;

    // �������� ������������ ��������� �� ����� ����� �������-�������
    function  getStopPlaceName(aStopPlaceMuid: int64; aFlNameForTerminalPoint : boolean = false) : string;
    // �������� ������ ������������ ����� �������-�������
    function  getStopPlaceCaption(aStopPlaceMuid: int64; aFlNameForTerminalPoint : boolean = false) : string;
    // �������� ����� � ����������� �������� �� ����� ����� �������-�������
    function  getStopPlaceStreetDirection(aStopPlaceMuid: int64; var vStreet: string; var vDirection: string) : boolean;
    // �������� ������������ ���� ����������� ����������
    function  getStopPlaceTransportKindName(aHasBus, aHasTrolley, aHasTram: boolean; flShort: boolean = false): string;
    // �������� ������ (���������) �������� �� ������� ��� � �������� "��������"
    function  getRouteStatusByState(aState: EMgtRouteState; aFlTemporary: boolean): EMgtRouteState2;
    // �������� ������ �������� �������� �� ����� ��������
    function  getRouteVariantStateByDates(aStartDate, aEndDate: TDate): string;
    // �������� ������ ����������� ����� ���������
    function  getStopPavilionFullInventoryNumber(aStopPavilionMuid: int64): string;
    // ��������� ������ �� ��������� � �����-���� ���������� �������� "�����������"
    procedure stopPlaceHasRoutesByAgency(aStopPlaceMuid: int64; var flHasMGTRoutes, flHasCommercialRoutes: boolean);
    procedure stopPlacesHasRoutesByAgency(aStopPlaces: TStringList; var flHasMGTRoutes, flHasCommercialRoutes: boolean);

    // ����������� ������� (�������� � ����� �� ����������), ��������� � ���������� ��������
    function  copyRouteVariant(aFromMuid, aToRouteMuid: int64): TtrsGISMGTObject;
    // ����������� ���� (�������� � ����� �� ����������), ��������� � ���������� ��������
    function  copyRouteRound(aFromMuid, aToVariantMuid: int64): TtrsGISMGTObject;
    // ����������� ������� ���� (�������� � ����� �� ����������), ��������� � ���������� ��������
    function  copyRouteNullRound(aFromMuid, aToVariantMuid: int64): TtrsGISMGTObject;
    // ����������� ���������� (�������� � ����� �� ����������), ��������� � ���������� �����
    function  copyRouteTrajectory(aFromMuid, aToRoundMuid: int64; aTrajectoryRound : EMgtRouteTrajectoryRoundType; aTrajectoryType: EMgtRouteTrajectoryType = rttUndefined): TtrsGISMGTObject;
    // ����������� ����� ��� ���������� ���������, ��� ���������� ������������� �������
    procedure copyLnkObjects(aObject: TtrsGISMGTObject; aDataSource: TMgtDatasource; aParentField: string; aFromParentMuid, aToParentMuid: int64);

    // �������� ������� ������� ��� ��������� ��� ��������
    procedure updateRouteByVariantDates(aMuid: int64);

    // ������������� �������� �������
    procedure onObjectDeleting(aDatasource: TMgtDatasource; aMuid: int64);
    procedure onParkDeleting(aMuid: int64);
    procedure onOrderDeleting(aMuid: int64);
    procedure onStopDeleting(aMuid: int64);
    procedure onStopPlaceDeleting(aMuid: int64);
    procedure onStopPavilionDeleting(aMuid: int64);
    procedure onStopPavilionOrderDeleting(aMuid: int64);
    procedure onRouteDeleting(aMuid: int64);
    procedure onRouteVariantDeleting(aMuid: int64);
    procedure onRouteRoundDeleting(aMuid: int64);
    procedure onRouteNullRoundDeleting(aMuid: int64);
    procedure onRouteTrajectoryDeleting(aMuid: int64);
    procedure onSubwayStationDeleting(aMuid: int64);
    procedure onDisplayPanelDeleting(aMuid: int64);
    procedure onDisplayPanelServiceContractDeleting(aMuid: int64);
    procedure onWifiEquipmentDeleting(aMuid: int64);
    procedure onVideoCameraDeleting(aMuid: int64);
    procedure onWifiEquipmentServiceContractDeleting(aMuid: int64);

    // �������� ����������� ������
    procedure onAttachmentsDeleting( aMuid: int64; aDatasource: TMgtDatasource );

    // ������������� �������� ������� �� �������
    procedure onBeforeObjectDeleteFromTicket(aDatasource: TMgtDatasource; aMuid: int64);
    procedure onBeforeStopPlaceDeleteFromTicket(aMuid: int64);
    procedure onBeforeRouteTrajectoryDeleteFromTicket(aMuid: int64);
    // ������������� �������� ������� �� �������
    procedure onAfterObjectDeleteFromTicket(aDatasource: TMgtDatasource; aMuid: int64);

    // �������� �� �� �������
    procedure deleteStopFromTicket(aMuid: int64);
    // ��������� ��������� ��� � ��
    function  checkStopPlaceInsideStop(aStopPlaceMuid, aStopMuid: int64): boolean;

    // ������� ���� (�������� � ����� �� ��������) � ���������� ����������
    procedure deletePark(aMuid: int64);
    // ������� ������� (�������� � ����� �� ��������) � ���������� ����������
    procedure deleteRoute(aMuid: int64);
    // ������� ������� (�������� � ����� �� ��������) � ���������� ����������
    procedure deleteRouteVariant(aMuid: int64);
    // ������� ���� (�������� � ����� �� ��������) � ���������� ����������
    procedure deleteRouteRound(aMuid: int64);
    // ������� ������� ���� (�������� � ����� �� ��������) � ���������� ����������
    procedure deleteRouteNullRound(aMuid: int64);
    // ������� ���������� (�������� � ����� �� ��������) � ���������� ����������
    procedure deleteRouteTrajectory(aMuid: int64);
    // ������� ����� (�������� � ����� �� ��������) ��� ���������� ���������, ��� ���������� ������������� �������
    procedure deleteLnkObjects(aObject: TtrsGISMGTObject; aDataSource: TMgtDatasource; aParentField: string; aParentMuid: int64);

    // ������� ������������ ����� (�������� � ����� �� ��������) � ���������� ����������
    procedure deleteStop(aMuid: int64);
    // ������� ����� �������-������� (�������� � ����� �� ��������) � ���������� ����������
    procedure deleteStopPlace(aMuid: int64);

    // ������� �������� �������� (�������� � ����� �� ��������) � ���������� ����������
    procedure deleteStopPavilion(aMuid: int64);

    // �������� � ������ � ������������� ���������� �� �������������� ���������
    procedure RebuildTrajectoriesByStopPlace(aStopPlaceMuid: int64; aRebuildSet : EMgtRouteTransportKindsSet = []);
    // �������� � ������ � ������������� ���������� �� �������������� ������������� ������
    procedure RebuildTrajectoriesByStop(aStopMuid: int64);
    // �������� � ������ � ������������� ���������� �� �������������� ������
    procedure RebuildTrajectoriesByGraphSection(aGraphSectionMuid: int64; RebuildTransportTypeSet : EMgtRouteTransportKindsSet);
    // �������� � ������ � ������������� ���������� �� �������������� ����
    procedure RebuildTrajectoriesByGraphNode(aGraphNodeMuid: int64);
    // ��������� ������� ����������, ����������� �������������
    function CheckIncorrctTrajectories(): boolean;

    // �������� � ����� ���������� � ����������� ��
    procedure RebuildTrajectories(aMuidList: TMapInt64);
    // ����������� �� ������
    procedure RebuildAllTrajectories();
    // ����������� ���� ����������� ���
    procedure RecalculateTrajectoriesHash();

    procedure RebuildNullTrajectories();

    // ��������� ������� ���� ������������ ������� (������)
    procedure CreateAllStopGraphics();
    // ������������� ��� ��� � �����
    procedure ReattachAllStopPlacesToGraph();

    // �������� �������� � ������
    procedure AddStopPavilionToPosterApplication(aStopPavilionMuid: int64);

    // ���� ���� - ��������, ����� ������� ���������� �� ����������
    procedure ProcessRTInMapFilterContainer (aRouteTrajectoryMuid: int64);

    // �������� ���������
    procedure ClearMapFilterContainer ();

    ///////////////////////////////
    // ������ �������������� �����
    ///////////////////////////////
    // ������� ���� ����� - ��������� ��� ����������� ��������� ����� ��������� ����
    // ������������� ���� ������ ���� ������, ��� ���������� � ����� ��� ������ ���� � �������
    procedure SplitSection(aEditedSection, aNewSection: int64);
    // ���������� ���� ����� - ��������� ��� ����������� ��������� ����� ����������� ���
    // ������������� ���� ������ ���� ������, ��� ���������� � ����� ��� ������ ���� � �������
    procedure UniteSection(aEditedSection, aRemovedSection: int64);

    // ����������� ��������� �� ������ ���� ����� � ������
    procedure CopyNodeData(aSrcNodeMuid, aDestNodeMuid: int64);
    // ����������� ��������� �� ������� ������ ����� � ������
    procedure CopySectionData(aSrcSectionMuid, aDestSectionMuid: int64);

    //������������� ��������� � �����
    procedure ReboundStopPlacesToGraph(aSectionMuid, aNewSectionMuid: Int64; aSectionLength: double);
    //������������� ��������� � ����������� �����
    procedure ReboundStopPlacesToTramGraph(aSectionMuid, aNewSectionMuid: Int64; aSectionLength: double);

    //������������� ����� � ������� ����
    function RecalcOffsetStopPlacesToGraph(aSectionMuid : int64)  : boolean;
    //������������� ����� � ���������� ����
    function RecalcOffsetStopPlacesToTramGraph(aSectionMuid : int64)  : boolean;
    ///////////////////////////////
    // �����: ������ �������������� �����
    ///////////////////////////////

    // ��������� ���������� ����� ������� ��� ����� �������
    // ��������� ����� �������� �� ����� ����������
    // (���� ��� ���������� ��������� ����� ������������ �������� ��������, ����� -1)
    function getDBMainRouteMuidByTrajectoryMuid(aTrajectoryMuid: int64): int64;

    // ��������� ��������� ���������� � ������ ������� (����� getObjects � getObjectValues)
    // �������� ��� ���������� �� ����������
    function GetTrajectoryTransportKind(ATrajectoryMuid: Int64): EMgtRouteTransportKind;
    // �������� ���� ����� �� ����������
    function GetTrajectoryRoundMuid(ATrajectoryMuid: Int64; var VRoundMuid: int64): EMgtRouteTrajectoryRoundType;
    // �������� ���� �������� �������� �� ����������
    function GetTrajectoryVariantMuid(ATrajectoryMuid: Int64; var VVariantMuid: int64; out Datasource: TMgtDatasource): Integer;
    // �������� ���� �������� �� ����������
    function GetTrajectoryRouteMuid(ATrajectoryMuid: Int64; var VRouteMuid: int64): Boolean;
    // �������� ����� ������ � �������� ���������� ����� ��� �������� ����� (���� ��� ����� ���������� -1)
    procedure GetTrajectoriesByRoundMuid(aRoundMuid: int64; aRoundType: EMgtRouteTrajectoryRoundType; var VForwardTrajectoryMuid: int64; var VBackwardTrajectoryMuid: int64);
    // �������� ����� ���������� �� ����� ������
    function GetTrajectoriesBySectionMuid(aSectionMuid: string; vTrajMuids: TMapInt64; flAppendToList : Boolean = false): Integer;
    // �������� ����� ���� �������-������� ����������
    function GetStopPlaces(aTrajectoryMuid: int64): TMapInt64;
    // �������� ���� ������������ �������� ��������
    function GetRouteCurrentVariant(ARouteMuid: Int64): int64;
    // �������� ���� ��������� ����� ('00') �� ����� ��������
    function GetRouteMainRoundByVariant(AVariantMuid: Int64): int64;
    // �������� ���� ��������� ����� ('00') ������������ �������� �� ����� ��������
    function GetRouteMainRound(ARouteMuid: Int64): int64;
    // �������� ����� ���� ������� ������ ������������ �������� �� ����� ��������
    function GetRouteRounds(ARouteMuid: Int64; aRoundType: EMgtRouteTrajectoryRoundType; vRoundMuids: TMapInt64): integer;
    // �������� ���� ���������� ��������� ����� ('00') ������������ �������� �� ����� ��������
    function GetRouteTrajectoryMuid(ARouteMuid: Int64; ADirection: EMgtRouteTrajectoryType): int64;

    // �������� ������� � ����� ������
    function GetGraphSectionsByGraphNode(aNodeMuid: int64; aSectionMuids: TMapInt64): integer;

    // �������� ������ �� ������
    function GetObjectFromTicket(aDataSource: TMgtDatasource; aMuid: int64): TtrsGISMGTObject; overload;
    function GetObjectFromTicket(aTableName: string; aMuid: int64): TtrsGISMGTObject; overload;
    function GetRefbookMUID(aDataSource: TMgtDatasource): int64; overload;
    function GetRefbookMUID(aTableName: string): int64; overload;

    // ������������� ������� ���� ������������ ������, (���������) ����������� � ����� �������-�������
    procedure CreateStopZone(aStopPlaceMuid: int64);
    // ������������� ������� ������������ ����� �� ����� �������-�������
    function  CreateStop(aStopPlaceMuid: int64): int64;
    // ������������� ������� ������� ������������� ������
    function  CreateStopGraphics(aStopMuid: int64; vStopGraphics: TMapObjectStructure): boolean;
    // ������� ������������ �����, ���� �� ������
    // aDeletedStopPlaceMuid - ���� ���������� ���.
    procedure DeleteStopIfNeeded(aStopMuid: int64);
    // ������������ ���� ��� "����� - ����������� ��������" �� ���������� ���
    procedure formStrDirListBySPCoords(aPoint: TDoublePoint; vStrDirList: TMapIntegers);
    // �������� ����������
    procedure updateStoredFiltration(flRefreshMap: boolean = true);
    procedure AddTOStoredFiltration(aGridView: TcxGridExtendedDBTableView; aFlStringFilter: boolean = false); overload;

    // ��������� ���������� �� �������� (��� ������ ������ �������� ��)
    function loadRouteSchedules(aRouteErmID: integer; aFullReload: boolean) : boolean;

    // �������� ���-�� ������� �� ������������ ����������, ��������� ������� ������������
    function getUserTaskCount(): integer;


    //------------Trafarets---------------------------------------------------//
    procedure createJsonsAllTrafaretsInTasks();
    //------------Trafarets---------------------------------------------------//

    //------------Utils-------------------------------
    // �������� ��������� ������������� ���� ������
    function  GetDaysCaption( aDaysStr : string ) : string;
    // ��������� �������� Variant ����������
    function  varIsValid(aVal : Variant; aAllowBlank: boolean = false) : boolean;
    // ������� 2 ����� � ���������
    // proportion: 0 - 100 (100: Result = Color1 ; 0: Result = Color2)
    function  blendColors(Color1, Color2: TColor; proportion: Byte): TColor;
    // �������� ��� ������������ �� �����
    function  getUserFIO(aUserMuid: int64; aFullName: boolean = false): string;
    // �������� ��� ���������� �� �����
    function  getInstallerFIO(aInstallerMuid: int64; aFullName: boolean = false): string;
    // ����������� ������� ��� ������ ��������� ���� ����� ����
    function  getBlobString(vStream: TMemoryStream; vFieldName: string): string;
    // ��������� ���� CDR � ����� � ����������� ������
    procedure loadLayoutByPath(aCDRPath, aPreviewPath: string; var vCDR: TMemoryStream; var vPreview: TdxPNGImage);
    // �������� �������� ����������
    function  getTempDirectory(aSubdirectory: string = ''): string;
    // ������� ���������� � ����������
    procedure openDirectory(aDirectory: string);

    // ���������� ����� ��� ������� ������
    procedure setNullRoundParkAsStopPlace();

    // ���������� ��������� ��� ������� ������
    procedure setNullRoundStopPlace();

    procedure DecodeFile (fileName : string);
    procedure EncodeFile (fileName : string);

    function getFileXMLByURL( aURL : string; aHttpClient : TIdHTTP; aFlRetry : boolean = true ) : TNativeXml;

    procedure checkStopPlacesGraphSections();
    procedure checkTrajectoryGraphSections();
    function  compareValueChange(src, dest : string; aLevel : integer): string; overload;
    function  compareValueChange(src, dest : int64; aLevel : integer): string; overload;
    function  compareValueChange(src, dest : boolean; aLevel : integer): string; overload;
    function  compareValueChange(src, dest : TDate; aLevel : integer): string; overload;
    function  getSpace(aLevel : integer): string;

  public  // property
    // �����������
    property authSettings: TExtAuthSettings read FAuthSettings;
    property auth: TFExtDXAuthorization read FAuth;
    property userPermissions: TMGTPermissions read FUserPermissions;    

    // �����
    property mail: TMailSettings read FMail;

    // ��������� ������� � ������ �������� ���������� � ���������������� �����������
    property expParamsServiceHost : string read FExpParamsServiceHost;
    property expParamsServicePort : integer read FExpParamsServicePort;

    // ������ �����
    property mapProject: string read FMapProject;

    // ���������� � ��
    property dbaCore: TDbaCore read FDbaCore;
    property conn: TDbaConnection read FConn;

    // ��������� ������� � ������ ������
    property bufferCardAlias : string read FbufferCardAlias write FbufferCardAlias;
    property bufferObjectMuid : int64 read FbufferObjectMuid write FbufferObjectMuid;

    property CommitedTicketList: TStrings read FCommitedTicketList;
    property MapTrajectories: TMapObjects read FMapTrajectories;

    // �����
    property Ticket: TtrsGISMGTTicket read FTicket;
    // ClientEngine
    property ClientEngine: TtrsClient read FClientEngine;
    // AdminEngine
    property AdminEngine: TtrsAdmin read FAdminEngine;
    // ���� ����������
    property UpdateCore: TUpdateCore read FUpdateCore;
    // ������ ����������
    property AppVersion: string read FAppVersion;
    // �������� ������
    property flTest: boolean read FflTest;

    // ������� ��������� �������
    property OnTicketCommit: TNotifyEvent read FOnTicketCommit write FOnTicketCommit;
    // ������� ��������� ������ �������
    property OnTicketChange: TtrsTicketChangedEvent read FOnTicketChange write FOnTicketChange;

    // ��������� ��� ��������
    property sqlParams: TMapStrings read FsqlParams;

    // ��������� ��������� ����������� ������������ ����������
    property PostTrajUniqueMuids: TMapInt64 read FPostTrajUniqueMuids;

    // ��������� ��� �������� ����������, ��� ���������� �����.
    property MapViewFilterTrajContainer: TMapInt64 read FMapViewFilterTrajContainer;

  end;

var
  core: TMgtCore;
  generator: TCorelGenerator;

implementation

uses cardFormsManager, mapplCore, main, trsObjectMap, IdHTTPHeaderInfo,
  MapObjectBase, spTask, trafaretDataConverter, spSignpostInTask;

var
  // ������� jsonListObjectsCompare ���������� ������� �� �������� ����, ���������� � ������ ����������
  // (�� ����� �� �������� ��� ��������)
  jsonListSortField : string = '';
  // ��� ���������� ��� jsonListObjectsCompare (�� ����� �� �������� ��� ��������)
  jsonListSortMode: EMgtJsonListSortModes = jsmString;

{**********************************************************************************************
* jsonListObjectsCompare
// ������� ��� ��������� �������� TLkJsonList, ������� �� �������� � getObjects
***********************************************************************************************}
function jsonListObjectsCompare(Item1, Item2: Pointer): Integer;
var
  jsonObject1, jsonObject2 : TlkJSONobject;
  val1, val2: string;
begin
  Result := 0;

  if jsonListSortField = '' then
    exit;

  jsonObject1 := TlkJSONobject(Item1);
  jsonObject2 := TlkJSONobject(Item2);
  val1 := jsonObject1.asString[jsonListSortField];
  val2 := jsonObject2.asString[jsonListSortField];

  case jsonListSortMode of
    jsmString:
      Result := CompareStr(val1, val2);
    jsmInteger:
    // AsInteger (��� � AsInt64) � lkJSON �� �������� ���������, ������ ����� �������, �� ������ �� ��������
      Result := CompareValue(StrToInt(val1), StrToInt(val2));
    jsmStringAsNumber:
    // AsInteger (��� � AsInt64) � lkJSON �� �������� ���������, ������ ����� �������, �� ������ �� ��������
      Result := compareStringAsNumber(val1, val2);
  end;
end;

{ TMgtCore }

{**********************************************************************************************
* Create
***********************************************************************************************}
constructor TMgtCore.Create();
begin
//  FStartupImage := TFMgtStartupImage.Create(nil);
  CreateSplashScreen(Application, nil);

  FAuthSettings := nil;
  FAuth := nil;

  FMail := TMailSettings.Create();

  FExpParamsServiceHost := '';
  FExpParamsServicePort := 0;
  httpClient := TIdHTTP.Create(nil);
  httpClient.Request.BasicAuthentication := false;

  FsqlLoader := TSqlLoader.Create();
  FmapSQL := TMapStrings.Create();
  FsqlParams := TMapStrings.Create(mcReplace);
  FsqlParams.CaseSensitive := False;

  FCommitedTicketList := TStringList.Create();
  FMapTrajectories := TMapObjects.Create(mcReplace, true);
  FMapRefbooks := TMapInt64.Create();

  {$IFDEF GRAPH_POST_COMMIT}
  FPostTrajUniqueMuids  := TMapInt64.Create(mcIgnore,True);
  {$ENDIF}

  FMapViewFilterTrajContainer := TMapInt64.Create(mcIgnore,True);

  FAdmin := nil;
  FAdminEngine := nil;
  FClientEngine := nil;
  FTicket := nil;
  FUpdateCore := nil;
  FAppVersion := '0 (�� ���������)';
  FflTest := false;
end;

{**********************************************************************************************
* Destroy
***********************************************************************************************}
destructor TMgtCore.Destroy();
begin
  FreeAndNil(FmapSQL);
//  FreeAndNil(FStartupImage);

  FreeAndNil(FMail);
  FreeAndNil(FAuthSettings);
  FreeAndNil(FAuth);
  FreeAndNil(httpClient);

  FreeAndNil(FsqlParams);
  FreeAndNil(FsqlLoader);

  FreeAndNil(FAdmin);
  FreeAndNil(FClientEngine);
  FreeAndNil(FAdminEngine);
  FreeAndNil(FTicket);

  FreeAndNil(FCommitedTicketList);
  FreeAndNil(FMapTrajectories);
  FreeAndNil(FMapRefbooks);

  FreeAndNil(FUpdateCore);

  {$IFDEF GRAPH_POST_COMMIT}
  FreeAndNil(FPostTrajUniqueMuids);
  {$ENDIF}

  FreeAndNil (FMapViewFilterTrajContainer);
  
  inherited;
end;

{**********************************************************************************************
* doAuth
***********************************************************************************************}
function TMgtCore.doAuth(): boolean;
begin
  Result := false;

  // ����������� ����
  //DecodeFile(GetFullPathWithoutRelativePathSymbols(SETTINGS_DIR) + '\' + AUTH_ALL_CONNECTIONS_FILE);
  try
    FAuthSettings := TExtAuthSettings.Create('MGT', SETTINGS_DIR, [MAPPL_PERMISSIONS, MGT_PERMISSIONS], false, true, OnAuthSetSelectedConnStringHandler);

    if (FAuthSettings.flError) then
      exit;

    FAuth := TFExtDXAuthorization.CreateForm(FAuthSettings);

    //���������� ������� ������ ���������������� ��������, ���� ��� ������
    if (Assigned(FUpdateCore)) and (FUpdateCore.NeedReset = True) then
    begin
      //�������� ����� ���������� �������� - ������ ���������,
      //���� �� ���� ������� �� TFExtDXAuthorization ����������� ��� ������ 
      FAuth.CBResetAppParams.Enabled := false;
      FAuth.CBResetAppParams.Checked := true; 
    end;

    /////////////////////////////////////////////////////////////////////////
    // ���� � ��� ��� ������� ����� ����������, �� ��� ��������� ���� ��������
    // � ���� ������������ vcl ��������� ����� �� ������� ������ �����...
    // �� CustomForm.ShowModal() - ��� ��������� Show,
    // ������� ������ ����������� ��������� ����� � ��������� �������
    // ������ �� �������� Z-order ���� ��������.
    // ����� ���� ����� �� ���� �������������� ��� ��� ����� ������ Parent,
    // � ��� �� � ��� � ���
    if Application.MainForm = nil then
    begin
      // �������
      // ��������� �������� ������������
      // ������������� ����� ������� ����������
      // � ��������� �������� ����� ��������
                           
      SetForegroundWindow(FAuth.Handle);
    end;
    /////////////////////////////////////////////////////////////////////////

    FAuth.ShowModal();

    if (FAuth.AuthResult <> 0) then
      exit;

    FillMGTPermissionsByUser(FAuth.User, FAuthSettings, @FUserPermissions);

    FAdmin := TExtAdministration.Create(FAuthSettings);
    FAdminEngine := TtrsAdmin.Create(FAdmin, FAuthSettings.SelectedConnString, FAuthSettings.Schema, FAuth.User.MUID);
    FAdmin.reload();
    FAdminEngine.reload();
    FClientEngine := TtrsClient.Create(FAdminEngine, 'gis_mgt');

    Result := true;
  finally
    // �������� �������
    //EncodeFile(GetFullPathWithoutRelativePathSymbols(SETTINGS_DIR) + '\' + AUTH_ALL_CONNECTIONS_FILE);
  end;          


end;

{**********************************************************************************************
* OnAuthSetSelectedConnStringHandler
***********************************************************************************************}
procedure TMgtCore.OnAuthSetSelectedConnStringHandler(aConnAliases: TConnectionAliases; var aConnAlias: string);
begin
  // ���� ���� ���� ���� �������� ����������, �� ����� ������
  if aConnAliases.KeysAliases.Count > 0 then
  begin
    aConnAlias := aConnAliases.ConnAlias[aConnAliases.KeysAliases.keys[0]];

    if (aConnAliases.AllConnections.ConnectionInfo[aConnAlias] <> nil) then
      TDbaCore.GetGlobalCore().Resolver.Aliases.addItem('local', aConnAliases.AllConnections.ConnectionInfo[aConnAlias].ConnString)
    else
      showDialog(dtError, dbsOK, '���������� � ������� ' + aConnAlias + ' �� ������� � ������ ����������.');
  end;
end;

{**********************************************************************************************
* init
***********************************************************************************************}
function TMgtCore.init(): boolean;
begin
  Result := false;
  bufferCardAlias := '';
  FbufferObjectMuid := -1;

  try
    FMail.readFile(MAIL_SETTINGS_FILE);

  except
    on e: Exception do
    begin
      showDialog(dtError, dbsOK, '������ ��� ������ �������� ��������� �������', e.Message);
      exit;
    end;
  end;

  try
    readSettings();

  except
    on e: Exception do
    begin
      showDialog(dtError, dbsOK, '������ ��� ������ �������� ����������', e.Message);
      exit;
    end;
  end;

  FDbaCore := TDBACore.GetGlobalCore();
  FConn := FdbaCore.addConnection(authSettings.SelectedConnString, false);

  if (FConn = nil) then
  begin
    showDialog(dtError, dbsOK, '������ ��� �������� ���������� ������ TDbConnection');
    exit;
  end;

  if (FConn.connect() <> 0) then
  begin
    showDialog(dtError, dbsOK, '�� ������� ���������� ����������� � ������� ����', 'Connstring: ' + authSettings.connString);
    exit;
  end;

  try
    ReCreateTicket();
  except
    on e: Exception do
    begin
      showDialog(dtError, dbsOk, '������ ��� ������������� ������ ���������', e.Message);
      exit;
    end;
  end;

  StoredFiltration.flIsUpdating := false;

  Result := true;
end;

{**********************************************************************************************
* update
***********************************************************************************************}
function TMgtCore.update(): boolean;
var
  dir: string;
  version: integer;
begin
  Result := false;
  showPopupMessage('�������� ����������...');

  try
    try
      dir := killTrailingSlash(ExtractFileDir(ParamStr(0)));

      if not Assigned(FUpdateCore) then
        try
          FUpdateCore := TUpdateCore.Create(dir, dir + '\' + SETTINGS_DIR, DEFAULT_INSTALLER_FILE_NAME, APP_RESET_PARAMS);

        except
          on e: Exception do
            raise EMgtEurekaLogIgnoredException.Create(e.Message);
        end;

      FAppVersion := IntToStr(FUpdateCore.CurrentVersion);

      if (Length(FAppVersion) >= 4) then
        FAppVersion := FAppVersion[1] + '.' + FAppVersion[2] + '.' + FAppVersion[3] + FAppVersion[4];

      try
        version := FUpdateCore.ServerLastVersion;

      except
        on e: Exception do
        begin
          showDialog(dtError, dbsOK, '������ �������� ������� ����� ������ ��� "�����������".', e.message);
          exit;
        end;
      end;

      try
        if (FUpdateCore.CurrentVersion < version) then
        begin
          Result := true;
          
          showPopupMessage('��������� ����� ������...');
          FUpdateCore.update(version);
        end;

      except
        on e: Exception do
          showDialog(dtError, dbsOK, '�� ������� ��������� ���������� ������ ��� "�����������".', e.message);
      end;

    except
      on e: Exception do
        showDialog(dtError, dbsOK, '�� ������� ��������� ������ ���������� ������ ��� "�����������".', e.Message);
    end;

  finally
    hidePopupMessage();
  end;
end;

{**********************************************************************************************
* checkForUpdate
***********************************************************************************************}
function TMgtCore.checkForUpdate(): boolean;
var
  dir: string;
  version: integer;
begin
  Result := false;

  try
    dir := killTrailingSlash(ExtractFileDir(ParamStr(0)));
    if not Assigned(FUpdateCore) then
      FUpdateCore := TUpdateCore.Create(dir, dir + '\' + SETTINGS_DIR, DEFAULT_INSTALLER_FILE_NAME, APP_RESET_PARAMS);

    try
      version := FUpdateCore.ServerLastVersion;

    except
      on e: Exception do
        raise EMgtEurekaLogIgnoredException.Create(e.Message);
    end;

    if (FUpdateCore.CurrentVersion < version) then
      Result := true;

  except
    on e: Exception do
      raise EMgtEurekaLogIgnoredException.Create(e.Message);
  end;
end;

{**********************************************************************************************
* readSettings
***********************************************************************************************}
procedure TMgtCore.readSettings();
var
  xml: TNativeXml;
  node: TXmlNode;

begin
  xml := TNativeXml.Create();

  try
    try
      xml.LoadFromFile(SETTINGS_FILE);
    except
      on e: Exception do
        raise EMgtException.Create('������ ��� ������ ����� �������� ' + SETTINGS_FILE + #13#10 + e.Message);
    end;

    // �������� ������
    FflTest := xml.Root.ReadAttributeBool('test', false);

    // ��������� �����
    node := xml.Root.NodeByName('map');
    if (node = nil) then
      raise EMgtException.Create('������ ��� ������ ����� ��������: � �������� ���� ����� �������� ��� ���� `map`');

    FMapProject := GetFullPathWithoutRelativePathSymbols(node.ReadAttributeString('project', ''));

    if (FMapProject = '') then
      raise EMgtException.Create('������ ��� ������ ����� ��������: � ���� `map` �� ����� ������� `project`');

    // ��������� ������ ��� �������� ����������
    node := xml.Root.NodeByName('ExpParamsService');
    //if (node = nil) then
      //raise EMgtException.Create('������ ��� ������ ����� ��������: � �������� ���� ����� �������� ��� ���� `ExpParamsService`');

    if (node <> nil) then
    begin
      FExpParamsServiceHost := node.ReadAttributeString('host', '');
      FExpParamsServicePort := node.ReadAttributeInteger('port', 0);
    end;

    //if (FExpParamsServiceHost = '') or (FExpParamsServicePort = 0) then
      //raise EMgtException.Create('������ ��� ������ ����� ��������: �� ������� ������� �������� `host` � `port` ���� `ExpParamsService`');

  finally
    FreeAndNil(xml);
  end;
end;

{**********************************************************************************************
* showPopupMessage
***********************************************************************************************}
procedure TMgtCore.showPopupMessage(aText: string);
begin
  PopupMessage:=aText;
  ShowSplashScreen(aText);
end;

{**********************************************************************************************
* hidePopupMessage
***********************************************************************************************}
procedure TMgtCore.hidePopupMessage();
begin
  PopupMessage:='';
  HideSplashScreen();
end;

{**********************************************************************************************
* getCoreSQL
***********************************************************************************************}
function TMgtCore.getCoreSQL(aSqlAlias: string; aSqlParams: TMapStrings): string;
begin
  Result := FsqlLoader.getSqlByAlias(ExtractFilePath(ParamStr(0)) + CORE_SQL_FILE, aSqlAlias, aSqlParams);

  if Result = '' then
    raise Exception.Create('������ ��� ��������� sql-������� � ������� ' + aSqlAlias + ' �� ����� ' + CORE_SQL_FILE);
end;

{**********************************************************************************************
* getGridSQL
***********************************************************************************************}
function TMgtCore.getGridSQL(aDataSet: TMgtDataset; aSqlParams: TMapStrings): string;
var
  i: integer;
begin

  for i := 0 to aDataSet.SQLConditions.count - 1 do
    aSqlParams.addItem(aDataSet.SQLConditions.keys[i], aDataSet.SQLConditions.items[i]);

  Result := FsqlLoader.getSqlByAlias(ExtractFilePath(ParamStr(0)) + GRID_SQL_FILE,
                                     GRID_SELECT_PREFIX + '_' + aDataSet.Alias, aSqlParams);
end;

{**********************************************************************************************
* getCardsSQL
***********************************************************************************************}
function TMgtCore.getCardsSQL(aSqlAlias : string; aSqlParams: TMapStrings): string;
begin
  Result := FsqlLoader.getSqlByAlias(ExtractFilePath(ParamStr(0)) + CARDS_SQL_FILE, aSqlAlias, aSqlParams);

  if Result = '' then
    raise Exception.Create('������ ��� ��������� sql-������� � ������� ' + aSqlAlias + ' �� ����� ' + CARDS_SQL_FILE);
end;

{**********************************************************************************************
* getSignpostsSQL
***********************************************************************************************}
function TMgtCore.getSignpostsSQL(aSqlAlias: string; aSqlParams: TMapStrings = nil): string;
begin
  Result := FsqlLoader.getSqlByAlias(ExtractFilePath(ParamStr(0)) + SIGNPOSTS_SQL_FILE, aSqlAlias, aSqlParams);

  if Result = '' then
    raise Exception.Create('������ ��� ��������� sql-������� � ������� ' + aSqlAlias + ' �� ����� ' + SIGNPOSTS_SQL_FILE);
end;

{**********************************************************************************************
* getReportsSQL
***********************************************************************************************}
function TMgtCore.getReportsSQL(aSqlAlias: string; aSqlParams: TMapStrings = nil): string;
begin
  Result := FsqlLoader.getSqlByAlias(ExtractFilePath(ParamStr(0)) + REPORTS_SQL_FILE, aSqlAlias, aSqlParams);

  if Result = '' then
    raise Exception.Create('������ ��� ��������� sql-������� � ������� ' + aSqlAlias + ' �� ����� ' + REPORTS_SQL_FILE);
end;

{**********************************************************************************************
* getMapObjectsSQL
***********************************************************************************************}
function TMgtCore.getMapObjectsSQL(aSqlAlias: string; aMUID: int64): string;
begin
  FsqlParams.Clear();
  FsqlParams.itemsByKey['muid'] := IntToStr(aMUID);
  Result := FsqlLoader.getSqlByAlias(ExtractFilePath(ParamStr(0)) + MAP_OBJECTS_SQL_FILE, aSqlAlias, FsqlParams);

  if Result = '' then
    raise Exception.Create('������ ��� ��������� sql-������� � ������� ' + aSqlAlias + ' �� ����� ' + MAP_OBJECTS_SQL_FILE);
end;

{**********************************************************************************************
* getMapSQL
***********************************************************************************************}
function TMgtCore.getMapSQL(aDataSet: TMgtDataset; aMUID: int64; aMapPostfix: string): string;
var
  key: string;
begin
  // ��� �������
  key := MAP_SELECT_PREFIX + aMapPostfix + '_' + aDataSet.Alias;

  // ���� �� �����������
  if (not FmapSQL.hasKey(key)) then
  begin
    Result := FsqlLoader.getSqlByAlias(ExtractFilePath(ParamStr(0)) + MAP_SQL_FILE, key);
    FmapSQL.addItem(key, Result);
  end
  else
    Result := FmapSQL.itemsByKey[key];

  // ����������� �������� MUID-�
  Result := StringReplace(Result, '$muid', IntToStr(aMUID), [rfReplaceAll]);
end;

{**********************************************************************************************
* getMapSQL
***********************************************************************************************}
function  TMgtCore.getMapSQL(aSqlAlias: string; aSqlParams: TMapStrings = nil): string;
begin
  Result := FsqlLoader.getSqlByAlias(ExtractFilePath(ParamStr(0)) + MAP_SQL_FILE, aSqlAlias, aSqlParams);
end;

{**********************************************************************************************
* getExpParamsSQL
***********************************************************************************************}
function TMgtCore.getExpParamsSQL(aSqlAlias: string; aSqlParams: TMapStrings): string;
begin
  Result := FsqlLoader.getSqlByAlias(ExtractFilePath(ParamStr(0)) + EXP_PARAMS_SQL_FILE, aSqlAlias, aSqlParams);

  if Result = '' then
    raise Exception.Create('������ ��� ��������� sql-������� � ������� ' + aSqlAlias + ' �� ����� ' + EXP_PARAMS_SQL_FILE);
end;

{**********************************************************************************************
* getPrintPath
***********************************************************************************************}
function TMgtCore.getPrintPath(): string;
var
  xml: TNativeXml;
  node: TXmlNode;
  vPath: string;
begin
  Result := '';
  xml := TNativeXml.Create();

  try
    try
      vPath := ExtractFilePath(ParamStr(0)) + PRINT_SETTINGS_FILE;
      xml.LoadFromFile(vPath);

    except
      on e: Exception do

        raise Exception.Create('������ ��� ������ ����� �������� ' + vPath + #13#10 + e.Message);
    end;

    node := xml.Root.NodeByName('settings');

    if node = nil then
    begin
      //raise Exception.Create('������ ��� ��������� sql-������� � ������� ' + aAlias + ' �� ����� ' + aFile);
      exit;
    end;

    Result := node.AttributeByName['path'];
  finally
    FreeAndNil(xml);
  end;
end;

{**********************************************************************************************
* getDatasourceFields
// �������� ������ ����� ���������, ����� ���������
***********************************************************************************************}
function TMgtCore.getDatasourceFields(aDataSource: TMgtDatasource): TMapStrings;
var
  dbaFields : TStrings;
  systemField : string;
  i, j : integer;

begin
  dbaFields := TStringList.Create();

  if not conn.GetTableFields( aDataSource.TableName, dbaFields, fncLower) then
    raise EMgtException.Create('������ ��� ��������� ������ ����� ������� ' + aDataSource.TableName);

  // ������� �� ������ ����� ���������
  for i := Low(mgtSystemFields) to High(mgtSystemFields) do
  begin
    systemField := Lowercase(mgtSystemFields[i]);

    j := dbaFields.IndexOf(systemField);
    if j <> -1 then
    begin
      if systemField = Lowercase(dbaMapplFieldNames[mfLine]) then
        dbaFields[j] := MOS_TAG       
      else
        dbaFields.Delete(j);
    end;
  end;

  Result := TMapStrings.Create();
  Result.Assign(dbaFields);

  dbaFields.Free();
end;

{**********************************************************************************************
* getObjectsForHighlight
***********************************************************************************************}
procedure TMgtCore.getObjectsForHighlight(var List: TMapInt64; aDataSet: TMgtDataset; aMUID: int64; aMapPostfix: string);
var
  sql: string;
  dbRes: TDBResult;
begin
  if (List = nil) then
    exit;

  List.Clear();

  sql := getMapSQL(aDataSet, aMUID, aMapPostfix);

  if (sql = '') then
  begin
    if (aDataSet.MapDatasource <> '') then
      List.addItem( mgtDatasources[aDataSet.MapDatasource].TableName, aMUID);

    exit;
  end;

  if (conn.QueryOpen(sql, dbRes, true) < 0) then
  begin
    showDialog(dtError, dbsOK, '�� ������� ��������� SQL-������.', sql);
    exit;
  end;

  dbRes.initMapFields();

  while (dbRes.Fetch()) do
    List.addItem(dbRes.asString(0), dbRes.asInt64(1));

  FreeAndNil(dbRes);
end;

{**********************************************************************************************
* DoOnTicketChange
***********************************************************************************************}
procedure TMgtCore.DoOnTicketChange(aChangeType: TtrsChangeType;
  aTicket: TtrsTicket; aObject: TtrsObject; aObjectData: TtrsObjectData;
  aObjectLink: TtrsObjectLink; aAttachment: TtrsAttachment);
begin
  if Assigned(FOnTicketChange) then
    FOnTicketChange(aChangeType, aTicket, aObject, aObjectData, aObjectLink, aAttachment);
end;

{**********************************************************************************************
* ReCreateTicket
***********************************************************************************************}
function TMgtCore.ReCreateTicket(flAfterCommit: boolean): TtrsGISMGTTicket;
begin
  FreeAndNil(FTicket);

  FTicket := TtrsGISMGTTicket.Create(FConn, FAuthSettings.Schema, TICKET_PROCESS_MUID, FAuth.User, FAppVersion);
  FTicket.OnChange := DoOnTicketChange;
  ClearMapTrajectories();
  Result := FTicket;

  if (cardsManager.forms.Count > 0) or (cardsManager.signpostForms.Count > 0) then
  begin
    showPopupMessage('���������� �������� ��������...');

    try
      try
        if (flAfterCommit) then
          cardsManager.reloadOpenFormsAfterCommit()
        else
          cardsManager.reloadOpenFormsAfterClear();
      except
        on e: Exception do
          showDialog(dtError, dbsOK, '������ ��� ���������� ��������', e.message);
      end;

    finally
      hidePopupMessage();
    end;
  end;

  if Assigned(mapCore) then
  begin
    mapCore.StopEditing(false);
    mapCore.TicketsManager.ClearBatchEditObjects();
  end;

  if Assigned(FOnTicketChange) then
    FOnTicketChange(tctTicketReloaded, FTicket, nil, nil, nil, nil);
end;

{**********************************************************************************************
* LoadBadTicket
***********************************************************************************************}
function TMgtCore.LoadBadTicket(): TtrsGISMGTTicket;
var
  Tickets: TStringList;
begin
  Result := nil;
  Tickets := TStringList.Create();

  try
    FAdminEngine.getPendingTickets(Tickets);

    if (Tickets.Count = 0) then
      exit;

    FreeAndNil(FTicket);

    FTicket := TtrsGISMGTTicket.Create(FConn, FAuthSettings.Schema, StrToInt64(Tickets[0]), FAppVersion);
    FTicket.Take(FTicket.CreatorMUID);
    FTicket.OnChange := DoOnTicketChange;
    ClearMapTrajectories();
    Result := FTicket;

    showPopupMessage('���������� �������� ��������...');

    try
      try
        cardsManager.reloadOpenFormsByTicket();

      except
        on e: Exception do
          showDialog(dtError, dbsOK, '������ ��� ���������� ��������', e.message);
      end;
    finally
      hidePopupMessage();
    end;

    if Assigned(mapCore) then
    begin
      mapCore.StopEditing(false);
      mapCore.TicketsManager.ClearBatchEditObjects();
    end;

    if Assigned(FOnTicketChange) then
      FOnTicketChange(tctTicketReloaded, FTicket, nil, nil, nil, nil);

  finally
    FreeAndNil(Tickets);
  end;
end;

{**********************************************************************************************
* DeleteTicket
***********************************************************************************************}
procedure TMgtCore.DeleteTicket();
var
  body: TStrings;
begin
  if not Assigned(FTicket) then
    exit;

  try
    try
      showPopupMessage('�������� ������ ���������...');
      FTicket.Delete(FTicket.CreatorMUID);

    except
      on e: Exception do
      begin
        body := TStringList.Create();
        body.Add('������������� �������: ' + IntToStr(FTicket.MUID) + #13#10);
        body.Add('TUID �������: ' + FTicket.TUID + #13#10);
        body.Add('������������: ' + FAuth.User.FIO + #13#10);
        body.Add('����: ' + FormatDateTime('yyyy-mm-dd hh:nn:ss', Now()) + #13#10);
        body.Add(e.Message);
        FMail.sendMail('������ ��� �������� ������ ��������� ' + FTicket.TUID + ' ������������� ' + FAuth.User.FIO, body);
        FreeAndNil(body);

//        hidePopupMessage();

        showDialog(dtError, dbsOK, '������ ��� �������� ������ ���������. ����� ����� ��������� ��������������� �������.', e.Message);

        ReCreateTicket(true);
        exit;
      end;
    end;

    ReCreateTicket(true);

  finally
    hidePopupMessage();
  end;
end;

{**********************************************************************************************
* CommitTicket
***********************************************************************************************}
function TMgtCore.CommitTicket(aTicket: TtrsGISMGTTicket): boolean;
var
  body: TStrings;
  tct: TtrsGISMGTTicket;
begin
  Result := false;

  if (aTicket = nil) then
    tct := FTicket
  else
    tct := aTicket;

  if not Assigned(tct) then
    exit;

  if tct.Objects.Count = 0 then
    exit;

  showPopupMessage('���������� ���������...');
  try
    try
      tct.MakeTransition(ttForward);
      Result := true;
//      hidePopupMessage();

    except
      on e: Exception do
      begin
        body := TStringList.Create();
        body.Add('������������� �������: ' + IntToStr(tct.MUID) + #13#10);
        body.Add('TUID �������: ' + tct.TUID + #13#10);
        body.Add('������������: ' + FAuth.User.FIO + #13#10);
        body.Add('����: ' + FormatDateTime('yyyy-mm-dd hh:nn:ss', Now()) + #13#10);
        body.Add(e.Message);
        FMail.sendMail('������ ��� ���������� ������ ��������� ' + tct.TUID + ' ������������� ' + FAuth.User.FIO, body);
        FreeAndNil(body);

//        hidePopupMessage();

        showDialog(dtError, dbsOK, '������ ��� ���������� ������ ���������. ����� ����� ��������� ��������������� �������.', e.Message);

        // ���� ������ - ��� ��������, �� ����������� ���
        if (tct = FTicket) then
          ReCreateTicket(true);

        exit;
      end;
    end;     

    if (tct.State = tcsAccepted) and Assigned(FOnTicketCommit) then
    begin
      showPopupMessage('���������� ������...');
      FCommitedTicketList.Add(IntToStr(tct.MUID));
      FOnTicketCommit(tct);
//    sleep(1); // ��� ������-�� ��������, �����-�� ���� � ��������...
//      hidePopupMessage();
    end;

  finally
    // ���� ������ - ��� ��������, �� ����������� ���
    if (tct = FTicket) then
      ReCreateTicket(true);

    hidePopupMessage();
  end;
end;

{**********************************************************************************************
* validateObjectBeforeAdd
***********************************************************************************************}
function TMgtCore.validateObjectBeforeAdd(aDataSource: TMgtDatasource; aMuid: Int64; aOperation: TtrsOperation): boolean;
var
  i, vCount: integer;
  vMsg: string;
  vObjectList: TMapIntegers;
  vLinksTo: TMgtLinkList;
begin
  vMsg := '';
  vCount := 0;
  Result := true;
  vObjectList := TMapIntegers.Create();
  try
    //����������� ������� ��������� � �������� ����������� �������
    case aOperation of
      oAdd:
      begin
      ///////////////////////
      end;
      oEdit:
      begin
      ///////////////////////
      end;
      oDelete:
      begin
        if aDataSource.Alias = 'Stops' then
        begin
          if not validateStopBeforeDelete(aMuid, vObjectList) then
            vMsg := '���������� ������� ������������ ����� � ��������������� ' + IntToStr(aMuid) +
                    '. �� ����������� ����� �������-������� ������������ ��������� ������:';
        end
        else if aDataSource.Alias = 'StopPlaces' then
        begin
          if not validateStopPlaceBeforeDelete(aMuid, vObjectList) then
            vMsg := '���������� ������� ����� �������-������� � ��������������� ' + IntToStr(aMuid) +
                    '. �� ������ ������ ������������ ��������� ������:';
        end
        else if aDataSource.Alias = 'RouteVariants' then
        begin
          if not validateRouteVariantBeforeDelete(aMuid, vObjectList) then
            vMsg := '���������� ������� ������� � ��������������� ' + IntToStr(aMuid) +
                    '. ������ ������� ������ ����������� � ��������:';
        end
        else if aDataSource.Alias = 'TerminalPointZones' then
        begin
          if not validateTerminalPointZoneBeforeDelete(aMuid, vObjectList) then
            vMsg := '���������� ������� ���� �������-������� � ��������������� ' + IntToStr(aMuid) +
                    '. �� ������ ������ ������������ ��������� ������:';
        end
        else if aDataSource.Alias = 'TerminalStations' then
        begin
          if not validateTerminalStationBeforeDelete(aMuid, vObjectList) then
            vMsg := '���������� ������� �������� ������� � ��������������� ' + IntToStr(aMuid) +
                    '. �� ������ ������ ������������ ��������� ������:';
        end
        else if aDataSource.Alias = 'Orders' then
        begin
          if not validateOrderBeforeDelete(aMuid, vObjectList) then
            vMsg := '���������� ������� ������ � ��������������� ' + IntToStr(aMuid) +
                    '. �� ������ ������ ������������ ��������� ������:';
        end
        else if aDataSource.Alias = 'StopPavilionOrders' then
        begin
          if not validateStopPavilionOrderBeforeDelete(aMuid, vObjectList) then
            vMsg := '���������� ������� ������ ��������� � ��������������� ' + IntToStr(aMuid) +
                    '. �� ������ ������ ������������ ��������� ������:';
        end
        else if aDataSource.Alias = 'CompensatoryPoints' then
        begin
          if not validateCompensatoryPointBeforeDelete(aMuid, vObjectList) then
            vMsg := '���������� ������� ����� ���������������� �������� � ��������������� ' + IntToStr(aMuid) +
                    '. �� ������ ������ ������������ ��������� ������:';
        end
        else
        // ��������� �����������
        begin
          vLinksTo := mgtLinks.getLinksTo(aDataSource.Alias);

          if vLinksTo.Count > 0 then
          begin
            vCount := validateReferenceBeforeDelete(aDataSource, aMuid, vObjectList);
            if vCount > 0 then
              vMsg := '���������� ������� ������ ����������� ' + aDataSource.Caption + ' � ��������������� ' + IntToStr(aMuid) +
                      '. �� ������ ���������� ��������� ������:';          
          end;

          vLinksTo.Free();
          // �� ������������ ������ �������� ������ ��������������� ������������
          (*
            if mgtRefLinksTo.hasKey(aDataSource) then
            begin
              vCount := validateReferenceBeforeDelete(aDataSource, aMuid, vObjectList);
              if vCount > 0 then
                vMsg := '���������� ������� ������ ����������� ' + aDataSource.Caption + ' � ��������������� ' + IntToStr(aMuid) +
                        '. �� ������ ���������� ��������� ������:';
            end;
          *)

        end;
      end;
    end;
    
    if vCount = 0 then
      vCount := vObjectList.Count;

    if vCount > 0 then
    begin
      Result := false;
      for i := 0 to vCount - 1 do
      begin
        if i = 10 then
        begin
          vMsg := vMsg + #13#10 + '��� ' + IntToStr(vCount - 10) + ' ������(-��)...' ;
          break;
        end;

        vMsg := vMsg + #13#10;
        vMsg := vMsg + GetMgtDataSourceById( vObjectList.items[i] ).Caption  + ': ' + vObjectList.keys[i];
      end;
      // �� ��������� ��������� � ������ ����� 50 ��������
      if vCount > MAX_TICKET_OBJECTS_ADD then
        showDialog(dtAlert, dbsOK, vMsg)
      else
      begin
        vMsg := vMsg + #13#10 + '�� ������ �������� ������ ������� � ������ �� ��������������?';
        if showDialog(dtAlert, dbsYesNoCancel, vMsg) = ID_YES then
        begin
          core.showPopupMessage('���������� �������� � ������...');
          try
            for i := 0 to vObjectList.Count - 1 do
              AddObjectToTicket( GetMgtDataSourceById( vObjectList.items[i] ), StrToInt64(vObjectList.keys[i]), oEdit);
          finally
            core.hidePopupMessage();
          end;
        end;
      end;
    end;
  finally
    FreeAndNil(vObjectList);
  end;
end;


{**********************************************************************************************
* validateReferenceBeforeDelete
***********************************************************************************************}
function TMgtCore.validateReferenceBeforeDelete(aDataSource: TMgtDatasource; aMuid: Int64; vList: TMapIntegers): integer;
var
  vLinksFrom :  TMgtLinkList;
  vResList: TlkJSONlist;
  vObj: TlkJSONobject;
  i, j: integer;
  datasource: TMgtDatasource;
begin
  vList.Clear();
  Result := 0;

  vLinksFrom := mgtLinks.getLinksFrom(aDataSource.Alias);

  for i := 0 to vLinksFrom.Count - 1 do
  begin
    datasource := mgtDatasources[vLinksFrom.items[i].DatasourceFrom];

    vResList := getObjects(datasource, vLinksFrom.items[i].FieldFrom, IntToStr(aMuid), ['muid']);
    Result := vResList.Count + Result;

    for j := 0 to vResList.Count - 1 do
    begin
      if vList.Count > MAX_TICKET_OBJECTS_ADD then
        break;

      vObj := vResList.asObject[j];
      vList.addItem(vObj.asString['muid'], datasource.ID);
    end;
    vResList.Free();
  end;

  vLinksFrom.Free();
end;

{**********************************************************************************************
* validateStopBeforeDelete
***********************************************************************************************}
function TMgtCore.validateStopBeforeDelete(aMuid: Int64; vList: TMapIntegers): boolean;
var
  vStopList: TlkJSONlist;
  vObj: TlkJSONobject;
  i: integer;
begin
  vList.Clear();
  Result := false;

  vStopList := getObjects(mgtDatasources[ 'StopPlaces' ], 'stop_muid', IntToStr(aMuid), ['muid']);

  try
    //������ ���������� �����
    if not Assigned(vStopList) then
      exit;

    for i := 0 to vStopList.Count - 1 do
    begin
      vObj := vStopList.asObject[i];

      validateStopPlaceBeforeDelete(StrToInt64(vObj.asString['muid']), vList);
    end;
    Result := vList.Count <= 0;
  finally
    FreeAndNil(vStopList);
  end;
end;

{**********************************************************************************************
* validateStopPlaceBeforeDelete
***********************************************************************************************}
function TMgtCore.validateStopPlaceBeforeDelete(aMuid: Int64; vList: TMapIntegers): boolean;
var
  i, j: integer;
  vObj: TlkJSONobject;
  vListTraj: TlkJSONlist;
  vListRoundA, vListRoundB: TlkJSONlist;
  vListNullRound1, vListNullRound2, vListNullRound3: TlkJSONlist;
  vListInstallations, vListPavilions: TlkJSONlist;
  datasource: TMgtDatasource;
begin
// !Todo: ���������, ��� ��������� ���������, ���� ���� ����������� �� (� ������ � �� ���� ���������� ����� ������)

  Result := true;
  vListTraj := getObjects(mgtDatasources[ 'LnkRouteTrajectoriesStopPlaces' ], 'stop_place_muid', IntToStr(aMuid), ['muid', 'route_trajectory_muid']);
  vListRoundA := getObjects(mgtDatasources[ 'RouteRounds' ], 'stop_place_A_muid', IntToStr(aMuid), ['muid']);
  vListRoundB := getObjects(mgtDatasources[ 'RouteRounds' ], 'stop_place_B_muid', IntToStr(aMuid), ['muid']);
  vListNullRound1 := getObjects(mgtDatasources[ 'RouteNullRounds' ], 'stop_place_1_muid', IntToStr(aMuid), ['muid']);
  vListNullRound2 := getObjects(mgtDatasources[ 'RouteNullRounds' ], 'stop_place_2_muid', IntToStr(aMuid), ['muid']);
  vListNullRound3 := getObjects(mgtDatasources[ 'RouteNullRounds' ], 'stop_place_3_muid', IntToStr(aMuid), ['muid']);
  vListInstallations := getObjects(mgtDatasources[ 'StopPavilionInstallations' ], 'stop_place_muid', IntToStr(aMuid), ['muid']);

  //������ ���������� ������
  try
    if not Assigned(vListTraj) or
       not Assigned(vListRoundA) or not Assigned(vListRoundB) or
       not Assigned(vListNullRound1) or not Assigned(vListNullRound2) or not Assigned(vListNullRound3) or
       not Assigned(vListInstallations) then
      exit;

    datasource := mgtDatasources[ 'RouteTrajectories' ];
    for i := 0 to vListTraj.Count - 1 do
    begin
      Result := false;
      vObj := vListTraj.asObject[i];
      vList.addItem(vObj.asString['route_trajectory_muid'], datasource.ID);
    end;

    datasource := mgtDatasources[ 'RouteRounds' ];
    for i := 0 to vListRoundA.Count - 1 do
    begin
      Result := false;
      vObj := vListRoundA.asObject[i];
      vList.addItem(vObj.asString['muid'], datasource.ID);
    end;

    datasource := mgtDatasources[ 'RouteRounds' ];
    for i := 0 to vListRoundB.Count - 1 do
    begin
      Result := false;
      vObj := vListRoundB.asObject[i];
      vList.addItem(vObj.asString['muid'], datasource.ID);
    end;

    datasource := mgtDatasources[ 'RouteNullRounds' ];
    for i := 0 to vListNullRound1.Count - 1 do
    begin
      Result := false;
      vObj := vListNullRound1.asObject[i];
      vList.addItem(vObj.asString['muid'], datasource.ID);
    end;

    datasource := mgtDatasources[ 'RouteNullRounds' ];
    for i := 0 to vListNullRound2.Count - 1 do
    begin
      Result := false;
      vObj := vListNullRound2.asObject[i];
      vList.addItem(vObj.asString['muid'], datasource.ID);
    end;

    datasource := mgtDatasources[ 'RouteNullRounds' ];
    for i := 0 to vListNullRound3.Count - 1 do
    begin
      Result := false;
      vObj := vListNullRound3.asObject[i];
      vList.addItem(vObj.asString['muid'], datasource.ID);
    end;

    datasource := mgtDatasources[ 'StopPavilions' ];
    for i := 0 to vListInstallations.Count - 1 do
    begin
      vObj := vListInstallations.asObject[i];
      vListPavilions := core.getObjects(datasource, 'current_stop_pavilion_installation_muid', vObj.asString['muid'],
                        ['muid']);

      for j := 0 to vListPavilions.Count - 1 do
      begin
        Result := false;
        vObj := vListPavilions.asObject[j];
        vList.addItem(vObj.asString['muid'], datasource.ID);
      end;
      vListPavilions.Free();
    end;

  finally
    vListTraj.Free();
    vListRoundA.Free();
    vListRoundB.Free();
    vListNullRound1.Free();
    vListNullRound2.Free();
    vListNullRound3.Free();
    vListInstallations.Free();
  end;
end;

{**********************************************************************************************
* validateRouteVariantBeforeDelete
***********************************************************************************************}
function TMgtCore.validateRouteVariantBeforeDelete(aMuid: Int64; vList: TMapIntegers): boolean;
var
  routeMuid, currentVariantMuid: string;
  vRoutes: TlkJSONlist;
begin
  vList.Clear();
  Result := true;
  // �� �������� �������� �������
  try
    routeMuid := getObjectValue( mgtDatasources[ 'RouteVariants' ], aMuid, 'route_muid');

    vRoutes := getObjects( mgtDatasources[ 'Routes' ] , 'muid', routeMuid, ['muid', 'current_route_variant_muid']);

    // ������� ��������� �� ��������� �������
    if vRoutes.Count < 1 then
      exit;

    currentVariantMuid := vRoutes.asObject[0].asString['current_route_variant_muid'];
    // �������� ������� �� �����
    if currentVariantMuid = '' then
      exit;

    if StrToInt64(currentVariantMuid) = aMuid then
    begin
      Result := false;
      vList.addItem(routeMuid, mgtDatasources[ 'Routes' ].ID);
    end;
  finally
    FreeAndNil(vRoutes);
  end;
end;

{**********************************************************************************************
* validateTerminalPointZoneBeforeDelete
***********************************************************************************************}
function TMgtCore.validateTerminalPointZoneBeforeDelete(aMuid: Int64; vList: TMapIntegers): boolean;
var
  i: integer;
  vObj: TlkJSONobject;
  vListRoundA, vListRoundB: TlkJSONlist;
  datasource: TMgtDatasource;
begin
  vList.Clear();
  Result := false;
  datasource := mgtDatasources[ 'RouteRounds' ];
  vListRoundA := getObjects(datasource, 'terminal_zone_A_muid', IntToStr(aMuid), ['muid']);
  vListRoundB := getObjects(datasource, 'terminal_zone_B_muid', IntToStr(aMuid), ['muid']);

  //������ ���������� ������
  try
    if not Assigned(vListRoundA) or not Assigned(vListRoundB) then
      exit;

    if vListRoundA.Count > 0 then
      for i := 0 to vListRoundA.Count - 1 do
      begin
        Result := false;
        vObj := vListRoundA.asObject[i];
        vList.addItem(vObj.asString['muid'], datasource.ID);
      end;

    if vListRoundB.Count > 0 then
      for i := 0 to vListRoundB.Count - 1 do
      begin
        Result := false;
        vObj := vListRoundB.asObject[i];
        vList.addItem(vObj.asString['muid'], datasource.ID);
      end;

    Result := vList.Count <= 0;
  finally
    vListRoundA.Free();
    vListRoundB.Free();
  end;
end;

{**********************************************************************************************
* validateTerminalStationBeforeDelete
***********************************************************************************************}
function TMgtCore.validateTerminalStationBeforeDelete(aMuid: Int64; vList: TMapIntegers): boolean;
var
  i: integer;
  vObj: TlkJSONobject;
  vListRoundA, vListRoundB, vListRoundC: TlkJSONlist;
  datasource: TMgtDatasource;
begin
  vList.Clear();
  Result := false;
  datasource := mgtDatasources[ 'RouteRounds' ];
  vListRoundA := getObjects(datasource, 'terminal_station_A_muid', IntToStr(aMuid), ['muid']);
  vListRoundB := getObjects(datasource, 'terminal_station_B_muid', IntToStr(aMuid), ['muid']);
  vListRoundC := getObjects(datasource, 'terminal_station_C_muid', IntToStr(aMuid), ['muid']);

  //������ ���������� ������
  try
    if not Assigned(vListRoundA) or
       not Assigned(vListRoundB) or
       not Assigned(vListRoundC) then
      exit;

    if vListRoundA.Count > 0 then
      for i := 0 to vListRoundA.Count - 1 do
      begin
        Result := false;
        vObj := vListRoundA.asObject[i];
        vList.addItem(vObj.asString['muid'], datasource.ID);
      end;

    if vListRoundB.Count > 0 then
      for i := 0 to vListRoundB.Count - 1 do
      begin
        Result := false;
        vObj := vListRoundB.asObject[i];
        vList.addItem(vObj.asString['muid'], datasource.ID);
      end;

    if vListRoundC.Count > 0 then
      for i := 0 to vListRoundC.Count - 1 do
      begin
        Result := false;
        vObj := vListRoundC.asObject[i];
        vList.addItem(vObj.asString['muid'], datasource.ID);
      end;

    Result := vList.Count <= 0;
  finally
    vListRoundA.Free();
    vListRoundB.Free();
    vListRoundC.Free();
  end;
end;

{**********************************************************************************************
* validateOrderBeforeDelete
***********************************************************************************************}
function TMgtCore.validateOrderBeforeDelete(aMuid: Int64; vList: TMapIntegers): boolean;
var
  i: integer;
  vObj: TlkJSONobject;
  vListRoutesOpen, vListRoutesClose, vListRouteVariants, vListStopPlaces: TlkJSONlist;
  datasource: TMgtDatasource;
begin
  vList.Clear();
  Result := false;
  vListRoutesOpen    := getObjects( mgtDatasources[ 'Routes' ], 'open_order_muid', IntToStr(aMuid), ['muid']);
  vListRoutesClose   := getObjects( mgtDatasources[ 'Routes' ], 'close_order_muid', IntToStr(aMuid), ['muid']);
  vListRouteVariants := getObjects( mgtDatasources[ 'RouteVariants' ], 'order_muid', IntToStr(aMuid), ['muid']);
  vListStopPlaces    := getObjects( mgtDatasources[ 'StopPlaces' ], 'order_muid', IntToStr(aMuid), ['muid']);

  //������ ���������� ������
  try
    if not Assigned(vListRoutesOpen) or
       not Assigned(vListRoutesClose) or
       not Assigned(vListRouteVariants) or
       not Assigned(vListStopPlaces) then
      exit;

    datasource := mgtDatasources[ 'Routes' ];
    if vListRoutesOpen.Count > 0 then
      for i := 0 to vListRoutesOpen.Count - 1 do
      begin
        Result := false;
        vObj := vListRoutesOpen.asObject[i];
        vList.addItem(vObj.asString['muid'], datasource.ID);
      end;

    datasource := mgtDatasources[ 'Routes' ];
    if vListRoutesClose.Count > 0 then
      for i := 0 to vListRoutesClose.Count - 1 do
      begin
        Result := false;
        vObj := vListRoutesClose.asObject[i];
        vList.addItem(vObj.asString['muid'], datasource.ID);
      end;

    datasource := mgtDatasources[ 'RouteVariants' ];
    if vListRouteVariants.Count > 0 then
      for i := 0 to vListRouteVariants.Count - 1 do
      begin
        Result := false;
        vObj := vListRouteVariants.asObject[i];
        vList.addItem(vObj.asString['muid'], datasource.ID);
      end;

    datasource := mgtDatasources[ 'StopPlaces' ];
    if vListStopPlaces.Count > 0 then
      for i := 0 to vListStopPlaces.Count - 1 do
      begin
        Result := false;
        vObj := vListStopPlaces.asObject[i];
        vList.addItem(vObj.asString['muid'], datasource.ID);
      end;
      
    Result := vList.Count <= 0;
  finally
    vListRoutesOpen.Free();
    vListRoutesClose.Free();
    vListRouteVariants.Free();
    vListStopPlaces.Free();
  end;
end;

{**********************************************************************************************
* validateStopPavilionOrderBeforeDelete
***********************************************************************************************}
function TMgtCore.validateStopPavilionOrderBeforeDelete(aMuid: Int64; vList: TMapIntegers): boolean;
var
  i: integer;
  vObj: TlkJSONobject;
  vListInstallations: TlkJSONlist;
  datasource: TMgtDatasource;
begin
  vList.Clear();
  Result := false;
  datasource := mgtDatasources[ 'StopPavilionInstallations' ];
  vListInstallations := getObjects(datasource, 'installation_order_muid', IntToStr(aMuid), ['muid']);


  //������ ���������� ������
  try
    if not Assigned(vListInstallations) then
      exit;

    if vListInstallations.Count > 0 then
      for i := 0 to vListInstallations.Count - 1 do
      begin
        Result := false;
        vObj := vListInstallations.asObject[i];
        vList.addItem(vObj.asString['muid'], datasource .ID);
      end;

    Result := vList.Count <= 0;
  finally
    vListInstallations.Free();
  end;
end;

{**********************************************************************************************
* validateCompensatoryPointBeforeDelete
***********************************************************************************************}
function TMgtCore.validateCompensatoryPointBeforeDelete(aMuid: Int64; vList: TMapIntegers): boolean;
var
  i: integer;
  vObj: TlkJSONobject;
  vListTraj: TlkJSONlist;
  datasource: TMgtDatasource;
begin
  vList.Clear();
  Result := false;
  vListTraj := getObjects( mgtDatasources[ 'LnkRouteTrajectoriesCompensatoryPoints' ], 'compensatory_point_muid', IntToStr(aMuid), ['muid', 'route_trajectory_muid']);

  //������ ���������� ������
  try
    if not Assigned(vListTraj) then
      exit;
    datasource := mgtDatasources[ 'RouteTrajectories' ];
    if vListTraj.Count > 0 then
      for i := 0 to vListTraj.Count - 1 do
      begin
        Result := false;
        vObj := vListTraj.asObject[i];
        vList.addItem(vObj.asString['route_trajectory_muid'], datasource.ID);
      end;

    Result := vList.Count <= 0;
  finally
    vListTraj.Free();
  end;
end;

{**********************************************************************************************
* loadLookupCombobox
***********************************************************************************************}
procedure TMgtCore.loadLookupCombobox(aCombo: TcxLookupComboBox; aKeyField, aValueFields: string; aDataSet: TMgtDataset;
                                      aOnChange: TNotifyEvent; aFlSilent: boolean);
var
  col : TcxLookupDBGridColumn;
begin
  // ����������� ���������� � runtime (����� � ������-����� ������)
  aCombo.Properties.ListFieldNames := aValueFields;
  aCombo.Properties.ListFieldIndex := 0;

  // �������� ����
  aCombo.Properties.KeyFieldNames := aKeyField;

  aCombo.Properties.IncrementalSearch := false;
  aCombo.Properties.IncrementalFiltering := false;    // ���������� ���� ������

  aCombo.Properties.DropDownListStyle := lsEditList;

  // ���������� �� ������� �������
  if aCombo.Properties.ListColumns.Count > 0 then
  begin
    col :=  aCombo.Properties.ListColumns.Items[0];
    col.SortOrder := soAscending;
  end;
  // ��������
  FMain.loadDataSet(aDataSet);
  aCombo.Properties.ListSource := aDataSet.compDataSource;

  if Assigned(aOnChange) then
    aCombo.Properties.OnChange := aOnChange
  else
  begin
    if aFlSilent then
      aCombo.Properties.OnChange := LookupComboboxPropertiesChangeSilent
    else
      aCombo.Properties.OnChange := LookupComboboxPropertiesChange;
  end;
end;

{**********************************************************************************************
* lookupComboboxPropertiesChangeSilent
***********************************************************************************************}
procedure TMgtCore.lookupComboboxPropertiesChangeSilent(Sender: TObject);
var
  cb: TcxLookupComboBox;
  newRowCount: integer;
begin
  cb := TcxLookupComboBox(Sender);

  ApplySearchFilter(cb.Properties.DataController, cb.Properties.ListFieldNames, cb.Text);
  newRowCount := cb.Properties.DataController.FilteredRecordCount;
  if newRowCount > 8 then
    newRowCount := 8
  else if newRowCount <= 0 then
    newRowCount := 1;
  cb.Properties.DropDownRows := newRowCount;
end;

{**********************************************************************************************
* lookupComboboxPropertiesChange
***********************************************************************************************}
procedure TMgtCore.lookupComboboxPropertiesChange(Sender: TObject);
var
  cb: TcxLookupComboBox;
begin
  lookupComboboxPropertiesChangeSilent(Sender);

  cb := TcxLookupComboBox(Sender);
  // �������� Validate, �.�. � OnValidate ������������ �� �� ������� �����
  cb.ValidateEdit;
end;

{**********************************************************************************************
* AddTrajectory
***********************************************************************************************}
function TMgtCore.AddTrajectory(aMuid: int64; aTrajectoryType: EMgtRouteTrajectoryType; aFlLoad: boolean): TMgtRouteTrajectory;
begin
  Result := TMgtRouteTrajectory(FMapTrajectories.itemsByKey[IntToStr(aMuid)]);

  if not Assigned(Result) then
  begin
    Result := TMgtRouteTrajectory.Create(aMuid, FTicket, aTrajectoryType, aFlLoad);
    FMapTrajectories.addItem(IntToStr(aMuid), Result);
  end
  else
  begin
    Result.Ticket := FTicket;
    Result.TrajectoryType := aTrajectoryType;
  end;
end;

function TMgtCore.AddObjectToTicket(aDataSource: TMgtDatasource; aMuid: Int64; aOperation: TtrsOperation): TtrsGISMGTObject;
begin
  Result := AddObjectToTicketInternal(aDataSource, aMuid, aOperation);
end;

{**********************************************************************************************
* AddObjectToTicketInternal
***********************************************************************************************}
function TMgtCore.AddObjectToTicketInternal(aDataSource: TMgtDatasource; aMuid: Int64; aOperation: TtrsOperation;
                                            aFlSilent: Boolean): TtrsGISMGTObject;
var
  vInitialText: string;
  vVersion: integer;
  vRefbookMUID: Int64;
  vOperation: TtrsOperation;      // �������� � ������
begin
  Result := nil;
  if not validateObjectBeforeAdd(aDataSource, aMuid, aOperation) then
    exit;

  vInitialText := '';
  vVersion := -1;
  vRefbookMUID := GetRefbookMUID(aDataSource);
  Result := TtrsGISMGTObject(FTicket.GetObjectByRefbookAndMUID(vRefbookMUID, aMuid));
  if Assigned(Result) then
  begin
    vOperation := Result.Operation;
    if vOperation = aOperation then
      exit;

    // ������ ��� ��� �������� � ����� � ������ ���������
    if aOperation = oAdd then
      raise EMgtException.Create('���������� �������� ������ � ������ � ��������� add. ������ ��� �������� � ������ � ������ ���������');

    if vOperation = oAdd then
    begin
      if aOperation = oDelete then
      begin
        DeleteObjectFromTicket(aDataSource, aMuid);
        Result := nil;
      end;

      exit;
    end
    else
      DeleteObjectFromTicket(aDataSource, aMuid);
  end;

  if aOperation <> oAdd then
    vInitialText := getInitialData(aDataSource, aMuid, vVersion);

  try
    Result := AddObjectToTicketInternal(vRefbookMUID, aMuid, aOperation, vInitialText, vVersion);
    if Assigned(Result) then
      AddObjectLinks(aDataSource, aMuid);
    if aDataSource.Alias = 'RouteTrajectories' then
      AddTrajectory(aMuid);

    if (aOperation = oDelete) and (not aFlSilent) then
      onObjectDeleting(aDataSource, aMuid);

    cardsManager.reloadFormByTicket(aDataSource, aMuid);
  except
    on e: Exception do
      raise EMgtException.Create(e.Message);
  end;
end;

{**********************************************************************************************
* AddObjectToTicket
***********************************************************************************************}
function TMgtCore.AddObjectToTicketInternal(aDataSource: TMgtDatasource; aMuid: Int64; aOperation: TtrsOperation; aInitialData: string;
                                            aVersion: Integer): TtrsGISMGTObject;
var
  vRefbookMUID: Int64;
begin
  vRefbookMUID := GetRefbookMUID(aDataSource);
  Result := AddObjectToTicketInternal(vRefbookMUID, aMuid, aOperation, aInitialData, aVersion);
end;

{**********************************************************************************************
* AddObjectToTicketInternal
***********************************************************************************************}
function TMgtCore.AddObjectToTicketInternal(aRefbookMuid, aMuid: Int64; aOperation: TtrsOperation; aInitialData: string;
                                            aVersion: Integer): TtrsGISMGTObject;
begin
  try
    Result := TtrsGISMGTObject(FTicket.AddObject(aRefbookMuid, aMuid, aOperation, aInitialData, aVersion));
  except
    on e: Exception do
    begin
      //showDialog(dtError, dbsOK, e.Message);
      raise EMgtException.Create(e.Message);
    end;
  end;
end;

{**********************************************************************************************
* AddObjectToTicketSimple
***********************************************************************************************}
function TMgtCore.AddObjectToTicketSimple(aTicket: TtrsGISMGTTicket; aDataSource: TMgtDatasource; aMuid: Int64;
                                    aOperation: TtrsOperation): TtrsGISMGTObject;
var
  vRefbookMUID: Int64;
  vInitialData: string;
  vVersion: integer;
begin
  Result := nil;

  if not Assigned(aTicket) then
    exit;

  try
    vRefbookMUID := GetRefbookMUID(aDataSource);

    Result := TtrsGISMGTObject(aTicket.GetObjectByRefbookAndMUID(vRefbookMUID, aMuid));

    if not Assigned(Result) then
    begin
      vInitialData := '';
      vVersion := -1;

      if aOperation <> oAdd then
        vInitialData := core.getInitialData(aDataSource, aMuid, vVersion);

      Result := TtrsGISMGTObject(aTicket.AddObject(vRefbookMUID, aMuid, aOperation, vInitialData, vVersion));
    end;

  except
    on e: Exception do
      raise EMgtException.Create(e.Message);
  end;
end;

{**********************************************************************************************
* AddObjectLinks
***********************************************************************************************}
procedure TMgtCore.AddObjectLinks(aDataSource: TMgtDatasource; aMuid: Int64);
var
  vLinksFrom, vLinksTo : TMgtLinkList;
  vLink: TMgtLink;
  i, j: integer;
  vMuid: int64;
  vValue: string;
  vList: TlkJSONlist;
  vObj: TlkJSONobject;
  vLinkType : TtrsLinkTypeSet;
begin
  vLinkType := [tltCascade, tltCommit];

  // �������� ����� � ������� ���������
  vLinksFrom := mgtLinks.getLinksFrom(aDataSource.Alias);
  for i := 0 to vLinksFrom.Count - 1 do
  begin
    vLink := vLinksFrom.items[i];

    // �������� �������� �������
    vValue := getObjectValue(aDataSource, aMuid, vLink.FieldFrom);
    if vValue = '' then
      Continue;

    vList := getObjects( mgtDatasources[vLink.DatasourceTo], vLink.FieldTo, vValue, ['muid'], true);

    // ����������� �����
    if Assigned(vList) then
    begin
      for j := 0 to vList.Count - 1 do
      begin
        vObj := vList.asObject[j];
        vMuid := StrToInt64(vObj.asString['muid']);

        // ������������ ������ ������
        // ���� ������ � ���� �� ����, �� ������� ������
        if (aDataSource.Alias = 'GraphSections') and (vLink.DatasourceTo = 'GraphNodes') then
          vLinkType := vLinkType + [tltDenyDelete];

        AddObjectLink(aDataSource, aMuid, mgtDatasources[vLink.DatasourceTo], vMuid, vLinkType);
      end;
    end;

    FreeAndNil(vList);
  end;
  vLinksFrom.Free();

  // �������� ����� �� ������ ��������
  vLinksTo := mgtLinks.getLinksTo(aDataSource.Alias);
  for i := 0 to vLinksTo.Count - 1 do
  begin
    vLink := vLinksTo.items[i];

    // �������� �������� �������
    vValue := getObjectValue(aDataSource, aMuid, vLink.FieldTo);
    if vValue = '' then
      Continue;

    vList := getObjects( mgtDatasources[vLink.DatasourceFrom], vLink.FieldFrom, vValue, ['muid'], true);

    // ����������� �����
    if Assigned(vList) then
    begin
      for j := 0 to vList.Count - 1 do
      begin
        vObj := vList.asObject[j];
        vMuid := StrToInt64(vObj.asString['muid']);

        // ������������ �������� ������
        AddObjectLink(mgtDatasources[vLink.DatasourceFrom], vMuid, aDataSource, aMuid, vLinkType);
      end;
    end;

    FreeAndNil(vList);
  end;
  vLinksTo.Free();
end;

{**********************************************************************************************
* AddObjectLink
***********************************************************************************************}
procedure TMgtCore.AddObjectLink(aDataSourceFrom: TMgtDatasource; aMuidFrom: Int64;
                                 aDataSourceTo: TMgtDatasource; aMuidTo: Int64;
                                 aLinkType: TtrsLinkTypeSet);
var
  vRefFrom, vRefTo : int64;
  vTicketObjFrom, vTicketObjTo: TtrsObject;
begin
  vRefFrom := GetRefbookMUID(aDataSourceFrom);
  vRefTo   := GetRefbookMUID(aDataSourceTo);

  vTicketObjFrom := FTicket.GetObjectByRefbookAndMUID(vRefFrom, aMuidFrom);
  vTicketObjTo   := FTicket.GetObjectByRefbookAndMUID(vRefTo, aMuidTo);

  if (not Assigned(vTicketObjFrom)) or (not Assigned(vTicketObjTo)) then
    exit;

  if (vTicketObjFrom.Operation = oDelete) and (vTicketObjTo.Operation = oDelete) then
    aLinkType := [tltCascade, tltCommit, tltDenyDelete];

  FTicket.AddObjectLink(vRefFrom, aMuidFrom, vRefTo, aMuidTo, aLinkType);
end;

{**********************************************************************************************
* DeleteObjectLink
***********************************************************************************************}
procedure TMgtCore.DeleteObjectLink(aDataSourceFrom: TMgtDatasource; aMuidFrom: Int64;
                                    aDataSourceTo: TMgtDatasource; aMuidTo: Int64);
var
  vRefFrom, vRefTo : int64;
begin
  vRefFrom := GetRefbookMUID(aDataSourceFrom);
  vRefTo   := GetRefbookMUID(aDataSourceTo);

  FTicket.DeleteObjectLink(vRefFrom, aMuidFrom, vRefTo, aMuidTo);
end;

{**********************************************************************************************
* AddStopPlaceTrajectoryObjectLinks
***********************************************************************************************}
procedure TMgtCore.AddStopPlaceTrajectoryObjectLinks(aStopPlaceMuid: Int64);
var
  vListTraj: TlkJSONlist;
  vTraj: TlkJSONobject;
  vTrajMuid, vLastMuid: int64;
  i: integer;
  vRefFrom, vRefTo: int64;
  vTicketObjFrom, vTicketObjTo: TtrsObject;
begin
  vRefFrom := GetRefbookMUID( mgtDatasources[ 'RouteTrajectories' ] );
  vRefTo   := GetRefbookMUID( mgtDatasources[ 'StopPlaces' ] );

  vTicketObjTo := FTicket.GetObjectByRefbookAndMUID(vRefTo, aStopPlaceMuid);
  // ���� ��������� ��� � �������, ����� ������� �� ����
  if not Assigned(vTicketObjTo) then
    exit;

  vListTraj := getObjects( mgtDatasources[ 'LnkRouteTrajectoriesStopPlaces' ], 'stop_place_muid', IntToStr(aStopPlaceMuid),
    ['muid', 'route_trajectory_muid'], true);

  try
    // ����������� �����
    if Assigned(vListTraj) then
    begin
      vLastMuid := -1;
      for i := 0 to vListTraj.Count - 1 do
      begin
        vTraj := vListTraj.asObject[i];
        vTrajMuid := StrToInt64(vTraj.asString['route_trajectory_muid']);
        // ���������� ������������ ���, ��������
        if vLastMuid = vTrajMuid then
          Continue;

        vLastMuid := vTrajMuid;

        vTicketObjFrom := FTicket.GetObjectByRefbookAndMUID(vRefFrom, vTrajMuid);

        if vTicketObjFrom <> nil then
          FTicket.AddObjectLink(vRefFrom, vTrajMuid, vRefTo, aStopPlaceMuid, [tltCascade, tltCommit, tltDenyDelete]);
      end;
    end;
  finally
    FreeAndNil(vListTraj);
  end;
end;

{**********************************************************************************************
* AddStopPlaceTrajectoryObjectLink
***********************************************************************************************}
procedure TMgtCore.AddStopPlaceTrajectoryObjectLink(aStopPlaceMuid, aTrajectoryMuid: Int64);
var
  vRefFrom, vRefTo: int64;
  vTicketObjFrom, vTicketObjTo: TtrsObject;
begin
  vRefFrom := GetRefbookMUID( mgtDatasources[ 'RouteTrajectories' ]);
  vRefTo :=   GetRefbookMUID( mgtDatasources[ 'StopPlaces' ]);

  vTicketObjFrom := FTicket.GetObjectByRefbookAndMUID(vRefFrom, aTrajectoryMuid);
  vTicketObjTo := FTicket.GetObjectByRefbookAndMUID(vRefTo, aStopPlaceMuid);

  // ���� ��������� ��� ��������� ��� � �������, ����� ������� �� ����
  if not Assigned(vTicketObjFrom) or not Assigned(vTicketObjTo) then
    exit;

  FTicket.AddObjectLink(vRefFrom, aTrajectoryMuid, vRefTo, aStopPlaceMuid, [tltCascade, tltCommit, tltDenyDelete]);
end;

{**********************************************************************************************
* AddGraphSectionTrajectoryObjectLink
***********************************************************************************************}
procedure TMgtCore.AddGraphSectionTrajectoryObjectLink(aGraphSectionMuid, aTrajectoryMuid: Int64);
var
  vRefFrom, vRefTo: int64;
  vTicketObjFrom, vTicketObjTo: TtrsObject;
begin
  vRefFrom := GetRefbookMUID( mgtDatasources[ 'RouteTrajectories' ] );
  vRefTo   := GetRefbookMUID( mgtDatasources[ 'GraphSections' ] );

  vTicketObjFrom := FTicket.GetObjectByRefbookAndMUID(vRefFrom, aTrajectoryMuid);
  vTicketObjTo   := FTicket.GetObjectByRefbookAndMUID(vRefTo, aGraphSectionMuid);

  // ���� ���� ��� ��������� ��� � �������, ����� ������� �� ����
  if not Assigned(vTicketObjFrom) or not Assigned(vTicketObjTo) then
    exit;

  FTicket.AddObjectLink(vRefFrom, aTrajectoryMuid, vRefTo, aGraphSectionMuid, [tltCascade, tltCommit, tltDenyDelete]);
end;
{**********************************************************************************************
* AddSlaveDataToTicket
***********************************************************************************************}
procedure TMgtCore.AddSlaveDataToTicket(aObject: TtrsGISMGTObject; aDataSource: TMgtDatasource; aMuid: Int64;
                                       aOperation: TtrsOperation; aData: TMapStrings);
var
  i: integer;
  vInitialText, vDataText: string;
  vVersion: integer;
  vPhysicalTableMUID: Int64;
  vOperation: TtrsOperation;
  vObjData: TtrsObjectData;
begin
  if not Assigned(aObject) then
    exit;

  vInitialText := '';
  vVersion := -1;

  vPhysicalTableMUID := FClientEngine.getPhysicalTableMUID(aDataSource.TableName);

  // ���� ���� ������ � slave data
  vObjData := nil;
  for i := 0 to aObject.SlaveData.Count - 1 do
  begin
    vObjData := aObject.SlaveData.items[i];
    if (vObjData.Datasource = aDataSource.TableName) and (vObjData.ObjMUID = aMuid) then
      break
    else
      vObjData := nil;
  end;

  if Assigned(vObjData) then
  begin
    vOperation := vObjData.Operation;
    if vOperation = aOperation then
      exit;

    // ������ ��� ��� �������� � ����� � ������ ���������
    if aOperation = oAdd then
      raise EMGTException.Create('���������� �������� ������ � ������ � ��������� add. ������ ��� �������� � ������ � ������ ���������');

    if vOperation = oAdd then
    begin
      if aOperation = oDelete then
        aObject.DeleteSlaveData(vPhysicalTableMUID, aMuid);

      exit;
    end
    else
      aObject.DeleteSlaveData(vPhysicalTableMUID, aMuid);
  end;

  if aOperation <> oAdd then
    vInitialText := getInitialData(aDataSource, aMuid, vVersion);

  // ��������� JSON ������
  vDataText := '';
  if Assigned(aData) then
    for i := 0 to aData.Count - 1 do
    begin
      if i > 0 then
        vDataText := vDataText + ',';
      vDataText := vDataText + FConn.quoteValue(aData.keys[i]) + ':' + FConn.quoteValue(aData.items[i]);
    end;
  vDataText := '{' + vDataText + '}';

  aObject.AddSlaveData(vPhysicalTableMUID, aMuid, aOperation, vInitialText, vVersion, vDataText);
end;

{**********************************************************************************************
* DeleteObjectFromTicket
***********************************************************************************************}
procedure TMgtCore.DeleteObjectFromTicket(aDataSource: TMgtDatasource; aMuid: Int64);
var
  vRefbookMUID: Int64;
begin
  try
    onBeforeObjectDeleteFromTicket(aDataSource, aMuid);

    vRefbookMUID := GetRefbookMUID(aDataSource);
    FTicket.DeleteObject(vRefbookMUID, aMuid);
    // ������� ������ �������, �� ���� ������� ������ � �����
    mapCore.TicketsManager.ChangeBatchEditObject(aDataSource.TableName, aMuid, '');
  except
    on e: Exception do
      showDialog(dtAlert, dbsOK, e.Message);
  end;
end;

{**********************************************************************************************
* GetFirstTicketObjectByDatasource
***********************************************************************************************}
function TMgtCore.GetFirstTicketObjectByDatasource(aDatasource: TMgtDatasource): TtrsGISMGTObject;
var
  i: integer;
  vRefbookMuid: int64;
  vObject: TtrsGISMGTObject;
begin
  Result := nil;

  vRefbookMUID := GetRefbookMUID(aDataSource);

  for i := 0 to FTicket.Objects.Count - 1 do
  begin
    vObject := TtrsGISMGTObject(FTicket.Objects.items[i]);
    if vRefbookMuid = vObject.RefbookMUID then
    begin
      Result := vObject;
      break;
    end;
  end;
end;

{**********************************************************************************************
* setFieldValue
// ����c��� �������� ���� � �����
***********************************************************************************************}
procedure TMgtCore.setFieldValue(aObjectData: TtrsObjectData; aField: string; aValue: string; aFlNullable: boolean);
begin
  if not Assigned(aObjectData) then
    raise Exception.Create('� setFieldValue ������� ������ ������ TtrsObjectData');

  if (aValue = '') and (aFlNullable) then
    aValue := NULL_VALUE;

  aObjectData.SetFieldValue(aField, aValue);
end;

{**********************************************************************************************
* setFieldValue
// ����c��� �������� ���� � �����
// Signed - ����������� integer (��������������� ������������� ����� � NULL)
// 0AsNull - ������ ����� 0 �� NULL
***********************************************************************************************}
procedure TMgtCore.setFieldValue(aObjectData: TtrsObjectData; aField: string; aValue: integer; aFlSigned, aFl0AsNull: boolean);
var
  value : string;
begin
  if ((not aFlSigned) and (aValue < 0)) or
     (aFl0AsNull and (aValue = 0)) then
    value := NULL_VALUE
  else
    value := IntToStr(aValue);
  
  setFieldValue(aObjectData, AField, value);
end;

{**********************************************************************************************
* setFieldValue
// ����c��� �������� ���� (boolean) � �����
***********************************************************************************************}
procedure TMgtCore.setFieldValue(aObjectData: TtrsObjectData; aField: string; aValue: boolean);
var
  value : string;
begin
  value := IntToStr( Integer(aValue) );

  setFieldValue(aObjectData, AField, value);
end;

{**********************************************************************************************
* setFieldValue
// ����c��� �������� ���� � �����
// ��� ������ (�������� <=0 �������� �� NULL)
***********************************************************************************************}
procedure TMgtCore.setFieldValue(aObjectData: TtrsObjectData; aField: string; aValue: int64);
var
  value : string;
begin
  if aValue <= 0 then    // ���� ������� ���� <=0
    value := NULL_VALUE
  else
    value := IntToStr(aValue);

  setFieldValue(aObjectData, aField, value);
end;

{**********************************************************************************************
* setFieldValue
***********************************************************************************************}
procedure TMgtCore.setFieldValue(aObjectData: TtrsObjectData; aField: string; aValue: TDateTime; aFlOnlyDate: boolean);
var
  value : string;
begin
  if aValue <= 0 then    // ���� ���� ������������
    value := NULL_VALUE
  else if (aFlOnlyDate) then
    value := DateToMySqlStr(aValue)
  else
    value := DateTimeToMySqlStr(aValue);

  setFieldValue(aObjectData, aField, value);
end;

{**********************************************************************************************
* getObjectsFromDB
// �������� ������ �������� (� �������� ��������� �����) �� ���������� ������� �� ��
***********************************************************************************************}
function TMgtCore.getObjectsFromDB(aDataSource: TMgtDatasource; aSqlCondition : string; aFields: array of string) : TlkJSONlist;
var
  sql: string;
  dbRes: TDBResult;
  fieldsStr : string;
  i : integer;

  jsonObj : TlkJSONobject;
begin
  if (aDataSource = nil) then
    raise Exception.Create('getObjectsFromDB: �� ����� �������� aDataSource ');

  Result := nil;
  fieldsStr := '';
  
  for i := Low(aFields) to High(aFields) do
  begin
    if i <> 0 then
      fieldsStr := fieldsStr + ', ';

    fieldsStr := fieldsStr + conn.quoteName(aFields[i]);
  end;

  FsqlParams.Clear();
  FsqlParams.itemsByKey['fields'] := fieldsStr;
  FsqlParams.itemsByKey['datasource'] := aDataSource.TableName;
  if conn.CheckFieldExistence(aDataSource.TableName, 'sign_deleted') = 0 then
    FsqlParams.itemsByKey['sign_deleted'] := 'sign_deleted = 0'
  else
    FsqlParams.itemsByKey['sign_deleted'] := '1 = 1';
  FsqlParams.itemsByKey['condition'] := aSqlCondition;

  sql := getCardsSql('get_objects', FsqlParams);
  try
    if (conn.QueryOpen(sql, dbRes, false) <> 0) then
      raise EMgtException.Create('������ ��� ��������� �������� �� ��.' + #13#10 + sql);

    Result := TlkJSONlist.Create();

    dbRes.initMapFields();
    while dbRes.Fetch() do
    begin
      jsonObj := TlkJSONobject.Create();

      if dbRes.getFieldNum('muid') = - 1 then
        raise Exception.Create('� ������� �� ������� ���� muid.' + #13#10 + sql);

      jsonObj.asString['muid'] := dbRes.asString('muid');
      for i := Low(aFields) to High(aFields) do
      begin
        jsonObj.asString[ aFields[i] ] := dbRes.asString( aFields[i] );
      end;

      Result.AddObject(jsonObj);
    end;
  finally
    FreeAndNil(dbRes);
  end;
end;

{**********************************************************************************************
* getObjects
// �������� ������ �������� (� �������� ��������� �����) �� ���������� �������
// �� �� ��������� ��������� �� ������, ���� ����
// ����������� ���������� � ������ ����� 'muid'
// ���� aFlKeepDeleted = true, �� ������� � ������ �� �������� �� ����� ����������� �� ������
***********************************************************************************************}
function TMgtCore.getObjects(aDataSource: TMgtDatasource; aConditionField, aConditionValue : string;
  aFields: array of string; aFlKeepDeleted: boolean = false;
  aSortField : string = ''; aSortMode: EMgtJsonListSortModes = jsmString) : TlkJSONlist;
var
  conditionStr : string;
begin
  if (aDataSource = nil) then
    raise Exception.Create('getObjects: �� ����� �������� aDataSource ');

  conditionStr := conn.quoteName(aConditionField) + ' = ' + conn.quoteValue(aConditionValue);
  Result := getObjectsFromDB(aDataSource, conditionStr, aFields);

  if Result = nil then    // ������ ��� ������ �������� �� ��
    exit;

  // ��������� ��������� �� �� � �������
  mergeObjectsWithTicket(Result, aDataSource, aConditionField, aConditionValue, aFields, aFlKeepDeleted);

  // ��������� ������
  if aSortField <> '' then
    sortJsonList(Result, aSortField, aSortMode);
end;

{**********************************************************************************************
* mergeObjectsWithTicket
***********************************************************************************************}
procedure TMgtCore.mergeObjectsWithTicket(resObjectList: TlkJSONlist; aDataSource: TMgtDatasource;
                                         aConditionField, aConditionValue : string; aFields: array of string;
                                         aFlKeepDeleted : boolean);
var
  i, j, k : integer;
  obj: TtrsObject;
  objData, objDataFound: TtrsObjectData;
  jsonObj : TlkJSONobject;
  vDatasource: string;
  muid : int64;
// ��������� �������� ����� � ������� JSON �� ��������� ObjectData ������
  procedure updateJsonObjectByObjectData(aJsonObject: TlkJSONobject; aObjectData: TtrsObjectData);
  var
    i : integer;
    val : string;
  begin
    // ��������� �������� ����� � ������� �� ������
    for i := Low(aFields) to High(aFields) do
    begin
      val := aObjectData.getActualValue( aFields[i], FIELD_NOT_FOUND );

      if val = FIELD_NOT_FOUND then
        continue;

      if val = NULL_VALUE then
        val := '';
      aJsonObject.asString[ aFields[i] ] := val;
    end;
  end;

  // ��������� � ��������, ���� ����� ������ ������ � ���������
  procedure checkAndAddObjectByObjectData(aObjectData: TtrsObjectData);
  var
    jsonObj : TlkJSONobject;
  begin
    if (aObjectData.Datasource = aDataSource.TableName)
    and (
      // ����� ������
      ((aObjectData.Operation = oAdd) and (aObjectData.getActualValue(aConditionField) = aConditionValue))
      // ������ ��������������, � � ���� ��������� ��������, �� �������� �������� ������, �� ������ ���      
      or ( (aObjectData.Operation = oEdit)
          and (aObjectData.GetInitialValue(aConditionField) <> aConditionValue)
          and (aObjectData.GetValue(aConditionField) = aConditionValue) )
    ) then
    begin
      jsonObj := TlkJSONobject.Create();
      updateJsonObjectByObjectData(jsonObj, aObjectData);

      jsonObj.asBoolean[ OBJECT_IN_TICKET ] := True;
      jsonObj.asInteger[ OBJECT_OPERATION ] := Integer( aObjectData.Operation );

      resObjectList.AddObject(jsonObj);
    end;
  end;
begin
  if (aDataSource = nil) then
    raise Exception.Create('mergeObjectsWithTicket: �� ����� �������� aDataSource ');

  if not Assigned(resObjectList) then
    raise EMgtException.Create('�� ������ ������ ��������!');

  // ���������� ������� �� ��, ����������� ��������� �� ������ (�������������� � ��������)
  i := 0;
  while i < resObjectList.Count do     // ���� �� �������� �� ��
  begin
    jsonObj := resObjectList.asObject[i];
    vDatasource := jsonObj.asString['datasource'];
    if vDatasource = '' then
      vDatasource := aDataSource.TableName;

    if not jsonObj.hasField('muid') then
      raise Exception.Create('� ������ ����� ��� ��������� �� ������ �� ������� ���� muid.');

    muid := StrToInt64( jsonObj.asString['muid'] );

    // ��������� ��������� ���� - ������� ������� � ������, ��������
    jsonObj.asBoolean[ OBJECT_IN_TICKET ] := false;
    jsonObj.asInteger[ OBJECT_OPERATION ] := -1;

    // ���� ������ � ������ (�� �������������� � ��������)
    for j := 0 to Ticket.Objects.Count - 1 do
    begin
      obj := Ticket.Objects.items[j];

      // ���� � maindata
      objData := obj.MainData;
      objDataFound := nil;
      if (objData.Datasource = vDatasource)
      and (objData.ObjMUID = muid)
      and (objData.Operation in [oEdit, oDelete]) then
        objDataFound := objData;

      if (objDataFound = nil) then
      begin
        // ���� � slavedata
        for k := 0 to obj.SlaveData.Count - 1 do
        begin
          objData := obj.SlaveData.items[k];
          if (objData.Datasource = vDatasource)
          and (objData.ObjMUID = muid) 
          and (objData.Operation in [oEdit, oDelete]) then
          begin   // �����
            objDataFound := objData;
            break;
          end;
        end;
      end;

      if Assigned(objDataFound) then  // ������ ������
      begin
        // ��������� ��������� ���� - ������� ������� � ������, ��������
        jsonObj.asBoolean[ OBJECT_IN_TICKET ] := True;
        jsonObj.asInteger[ OBJECT_OPERATION ] := Integer( objDataFound.Operation );

        if (objDataFound.Operation = oEdit) then
        begin

          if objDataFound.GetActualValue(aConditionField) <> aConditionValue then
          begin
            // � ������� ���������� �������� ����, �� �������� �������� ������� => ������� �� �����������
            resObjectList.Delete(i);   // ������� ������ �� ������ �������� �� (���� �� ����� ���� "��������� ���������")
            dec(i);
          end
          else
          begin
            // ��������� �������� ����� � ������� �� ������
            updateJsonObjectByObjectData(jsonObj, objDataFound);
          end;
        end
        else if (objData.Operation = oDelete) and (not aFlKeepDeleted) then   //
        begin
          // ������� ������ �� ������ �������� �� (���� �� ����� ���� "��������� ���������")
          resObjectList.Delete(i);
          dec(i);
        end;

        break;
      end;
    end;

    inc(i);
  end;

  // ���������� ������� � ������ �� ����������
  for j := 0 to Ticket.Objects.Count - 1 do
  begin
    obj := Ticket.Objects.items[j];
    
    // ���� � maindata
    objData := obj.MainData;

    checkAndAddObjectByObjectData(objData);

    // ���� � slavedata
    for k := 0 to obj.SlaveData.Count - 1 do
    begin
      objData := obj.SlaveData.items[k];

      checkAndAddObjectByObjectData(objData);
    end;
  end;
end;

{**********************************************************************************************
* sortJsonList
***********************************************************************************************}
procedure TMgtCore.sortJsonList(resObjectList: TlkJSONlist; aSortField: string; aSortMode: EMgtJsonListSortModes);
begin
  if aSortField = '' then
    exit;

  jsonListSortField := aSortField;
  jsonListSortMode := aSortMode;
  resObjectList.sort( jsonListObjectsCompare );
  jsonListSortField := '';
  jsonListSortMode := jsmString;
end;

{**********************************************************************************************
* getInitialData
***********************************************************************************************}
function TMgtCore.getInitialData(aDataSource: TMgtDatasource; aMuid: int64; var vVersion: integer): string;
var
  sql, field, value: string;
  dbRes: TDBResult;
  i, lCode: integer;
  data: TStringList;
  mos: TMapObjectStructure;
begin
  Result := '{}';
  vVersion := -1;
  FsqlParams.Clear;
  FsqlParams.addItem('datasource', aDataSource.TableName);
  FsqlParams.addItem('muid_value', IntToStr(aMuid));
  if conn.CheckFieldExistence(aDataSource.TableName, 'sign_deleted') = 0 then
    FsqlParams.itemsByKey['sign_deleted'] := 'sign_deleted = 0'
  else
    FsqlParams.itemsByKey['sign_deleted'] := '1 = 1';

  sql := getCardsSQL('initial_data', FsqlParams);
  if (conn.QueryOpen(sql, dbRes, false) <> 0) then
      raise EMgtException.Create('������ ��� ������ ������ �������' + #13#10 + sql);

  try
    if not dbRes.Fetch() then
      raise Exception.Create('�� ������ ������' + #13#10
        + '�������: ' + aDataSource.TableName + #13#10
        + '�������������: ' + IntToStr(aMuid)
      );

    data := TStringList.Create();
    data.Delimiter := ',';
    for i := 0 to dbRes.numFields - 1 do
    begin
      field := Lowercase(dbRes.fieldNames[i]);
      if dbRes.isNotNull(i) then
        value := dbRes.asString(i)
      else
        value := NULL_VALUE;

      if field = Lowercase(dbaMapplFieldNames[mfLine]) then
      begin
        field := MOS_TAG;
        mos := TMapObjectStructure.Create();
        lCode := aDataSource.layerCode;
        if (lCode > -1) and
           (mos.GetFromBuf(PByte(dbRes.asDataPointer(i)), -1, 0, 0, 0, mapCore.GetCoordsStoreAccuracyCoef(lCode)) > 0)
            and (mos.SubObjectsCount > 0) and (mos.oType > -1) and (mos.oType < 4) then
        begin
          mos.lCode := lCode;
          mos.oMUID := aMuid;
          mos.oStyleID := aDataSource.MapStyleID;
          value := mapCore.GetGeometryAsBase64String(mos, lCode);
        end;
        FreeAndNil(mos);
      end;
      if field = VERSION_FIELD then
        vVersion := dbRes.asInteger(i);
        
      data.Add(FConn.quoteValue(field) + ':' + FConn.quoteValue(value));
    end;
    Result := '{' + getDelimitedText(data, ',') + '}';
    FreeAndNil(data);

  finally
    FreeAndNil(dbRes);
  end;
end;

{**********************************************************************************************
* getDelimitedText
***********************************************************************************************}
function TMgtCore.getDelimitedText(aStringList: TStringList; aDelimiter: Char): string;
var
  i: Integer;
begin
  Result := '';
  if not Assigned(aStringList) then
    exit;

  for i := 0 to aStringList.Count - 1 do
    Result := Result + aStringList.Strings[i] + aDelimiter;
  System.Delete(Result, Length(Result), 1);
end;

{**********************************************************************************************
* getObjectValueFromTicket
// �������� �������� ���� ��� ���������� ������� �� ������ (�� MainData)
// ����������, ������ �� ������
***********************************************************************************************}
function TMgtCore.getObjectValueFromTicket(aDataSource: TMgtDatasource; aMuid : int64; aField: string; var resValue: string) : boolean;
begin
  Result := getObjectValueFromTicket(aDataSource.TableName, aMuid, aField, resValue);
end;

{**********************************************************************************************
* getObjectValueFromTicket
// �������� �������� ���� ��� ���������� ������� �� ������ (�� MainData)
// ����������, ������ �� ������
***********************************************************************************************}
function TMgtCore.getObjectValueFromTicket(aTableName: string; aMuid : int64; aField: string; var resValue: string) : boolean;
var
  FieldList: TMapStrings;
begin
  resValue := '';
  FieldList := TMapStrings.Create();

  FieldList.addItem(aField, '');

  try
    Result := getObjectValuesFromTicket(aTableName, aMuid, FieldList);
    resValue := FieldList.items[0];

  finally
    FreeAndNil(FieldList);
  end;
end;

{**********************************************************************************************
* getObjectValuesFromTicket
***********************************************************************************************}
function TMgtCore.getObjectValuesFromTicket(aDataSource: TMgtDatasource; aMuid: int64; FieldList: TMapStrings): boolean;
begin
  Result := getObjectValuesFromTicket(aDataSource.TableName, aMuid, FieldList);
end;

{**********************************************************************************************
* getObjectValuesFromTicket
***********************************************************************************************}
function TMgtCore.getObjectValuesFromTicket(aTableName: string; aMuid: int64; FieldList: TMapStrings): boolean;
var
  j, k : integer;
  obj: TtrsObject;
  objData, objDataFound: TtrsObjectData;
  resValue: string;
begin
  Result := false;

  // ���� ������ ���� - �������
  if (FieldList = nil) or (FieldList.Count = 0) then
    exit;

  objDataFound := nil;

  // ���� ������ � ������
  for j := 0 to Ticket.Objects.Count - 1 do
  begin
    obj := Ticket.Objects.items[j];

    // ���� � maindata
    objData := obj.MainData;
    if (objData.Datasource = aTableName) and (objData.ObjMUID = aMuid) then
      objDataFound := objData;

    if objDataFound = nil then
    begin
      // ���� � slavedata
      for k := 0 to obj.SlaveData.Count - 1 do
      begin
        objData := obj.SlaveData.items[k];
        if (objData.Datasource = aTableName) and (objData.ObjMUID = aMuid) then
        begin
          objDataFound := objData;
          break;
        end;
      end;
    end;

    if Assigned(objDataFound) then  // ������ ������
    begin
      for k := 0 to FieldList.Count - 1 do
      begin
        resValue := objDataFound.getActualValue( FieldList.keys[k] );

        if resValue = NULL_VALUE then
          resValue := '';

        FieldList.items[k] := resValue;
      end;

      Result := true;

      break;
    end;
  end;
end;

{**********************************************************************************************
* getObjectValueFromDB
// �������� �������� ���� ��� ���������� ������� �� ��
// ����������, ������ �� ������
***********************************************************************************************}
function TMgtCore.getObjectValueFromDB(aDataSource: TMgtDatasource; aMuid : int64; aField: string; var resValue: string) : boolean;
begin
  Result := getObjectValueFromDB(aDataSource.TableName, aMuid, aField, resValue);
end;

{**********************************************************************************************
* getObjectValueFromDB
// �������� �������� ���� ��� ���������� ������� �� ��
// ����������, ������ �� ������
***********************************************************************************************}
function TMgtCore.getObjectValueFromDB(aTableName: string; aMuid: int64; aField: string; var resValue: string): boolean;
var
  FieldList: TMapStrings;
begin
  resValue := '';
  FieldList := TMapStrings.Create();

  FieldList.addItem(aField, '');

  try
    Result := getObjectValuesFromDB(aTableName, aMuid, FieldList);
    resValue := FieldList.items[0];

  finally
    FreeAndNil(FieldList);
  end;
end;

{**********************************************************************************************
* getObjectValuesFromDB
***********************************************************************************************}
function TMgtCore.getObjectValuesFromDB(aDataSource: TMgtDatasource; aMuid: int64; FieldList: TMapStrings): boolean;
begin
  Result := getObjectValuesFromDB(aDataSource.TableName, aMuid, FieldList);
end;

{**********************************************************************************************
* getObjectValuesFromDB
***********************************************************************************************}
function TMgtCore.getObjectValuesFromDB(aTableName: string; aMuid: int64; FieldList: TMapStrings): boolean;
var
  sql, fieldsStr, field: string;
  dbRes: TDBResult;
  vFieldInfo: TDbaFieldInfo;
  i: integer;
begin
  Result := false;

  // ���� ������ ���� - �������
  if (FieldList = nil) or (FieldList.Count = 0) then
    exit;

  fieldsStr := '';

  for i := 0 to FieldList.Count - 1 do
  begin
    if i <> 0 then
      fieldsStr := fieldsStr + ', ';

    field := FieldList.keys[i];
    if field = MOS_TAG then
      field := dbaMapplFieldNames[mfLine];

    fieldsStr := fieldsStr + conn.quoteName(field);
  end;

  FsqlParams.Clear();
  FsqlParams.itemsByKey['fields'] := fieldsStr;
  FsqlParams.itemsByKey['datasource'] := aTableName;
  FsqlParams.itemsByKey['muid_value'] := IntToStr(aMuid);

  { ������ �� ������� sign_deleted - ������ �� ����� ����� �������� ����� ������, � ��� ����� ��������� 
  if conn.CheckFieldExistence(aDataSource, 'sign_deleted') = 0 then
    FsqlParams.itemsByKey['sign_deleted'] := 'sign_deleted = 0'
  else
    FsqlParams.itemsByKey['sign_deleted'] := '1 = 1';
  }

  sql := getCardsSql('get_object_values', FsqlParams);

  if (conn.QueryOpen(sql, dbRes, false) <> 0) then
      raise EMgtException.Create('������ ��� ��������� ������� �� ��.' + #13#10 + sql);

  try
    if dbRes.Fetch() then
    begin
      for i := 0 to FieldList.Count - 1 do
      begin
        if FieldList.keys[i] = MOS_TAG then
          FieldList.items[i] :=
            mapCore.GetGeometryAsBase64String( GetMgtDataSourceByTableName(aTableName), aMuid, PByte(dbRes.asDataPointer(i)), dbRes.sizeValue(i) )
        else
        begin
          field:= '';
          vFieldInfo := dbRes.getFieldAttrs(i);
          if vFieldInfo.flBinary then         // ���� ���� �������� ������ ���������� � ������.
            SetString(field,dbres.asDataPointer(i),dbres.sizeValue(i))
            //TODO: � ��� ���� ���� ������ (null) - ����� ������� '__NULL__'  � �� ������ ������... 
          else
            field:= dbres.asString(i);       // ����� ��� ������...

          FreeAndNil(vFieldInfo);
          FieldList.items[i] := field;
        end
      end;

      Result := true;
    end;

  finally
    FreeAndNil(dbRes);
  end;
end;

{**********************************************************************************************
* getObjectValue
// �������� �������� ���� ��� ���������� ������� (�� �� ��� ������, ���� ����)
***********************************************************************************************}
function TMgtCore.getObjectValue(aDataSource: TMgtDatasource; aMuid : int64; aField: string; aDefaultValue: string) : string;
begin
  Result := getObjectValue(aDataSource.TableName, aMuid, aField, aDefaultValue);
end;

{**********************************************************************************************
* getObjectValue
// �������� �������� ���� ��� ���������� ������� (�� �� ��� ������, ���� ����)
***********************************************************************************************}
function TMgtCore.getObjectValue(aTableName: string; aMuid : int64; aField: string; aDefaultValue: string) : string;
var
  FieldList: TMapStrings;
begin
  Result := aDefaultValue;

  FieldList := TMapStrings.Create();

  FieldList.addItem(aField, aDefaultValue);

  try
    if getObjectValues(aTableName, aMuid, FieldList) then
      Result := FieldList.items[0];

  finally
    FreeAndNil(FieldList);
  end;
end;

{**********************************************************************************************
* getObjectValues
***********************************************************************************************}
function TMgtCore.getObjectValues(aDataSource: TMgtDatasource; aMuid: int64; FieldList: TMapStrings): boolean;
begin
  Result := getObjectValues(aDataSource.TableName, aMuid, FieldList);
end;

{**********************************************************************************************
* getObjectValues
***********************************************************************************************}
function TMgtCore.getObjectValues(aTableName: string; aMuid: int64; FieldList: TMapStrings): boolean;
begin
  Result := false;

  // ���� ������ ���� - �������
  if (FieldList = nil) or (FieldList.Count = 0) then
    exit;

  Result := getObjectValuesFromTicket(aTableName, aMuid, FieldList);

  if Result then
    exit;   // ����� � ������

  Result := getObjectValuesFromDB(aTableName, aMuid, FieldList);

  {
  if not Result then
    raise Exception.Create('�� ������ ������.' + #13#10 +
                           '�������: ' + aDataSource + #13#10 +
                           '�������������: ' + IntToStr(aMuid));
  }
end;

{**********************************************************************************************
* IsObjectExistsInDB
***********************************************************************************************}
function TMgtCore.isObjectExistsInDB(aTableName: string; aMuid : int64) : Boolean;
var Dummy: string;
begin
  Result:= getObjectValueFromDB(aTableName, aMuid, 'muid', Dummy);
end;

{**********************************************************************************************
* isObjectExistsInDB
***********************************************************************************************}
function TMgtCore.isObjectExistsInDB(aDataSource: TMgtDatasource; aMuid : int64) : Boolean;
begin
  Result:= isObjectExistsInDB(aDataSource.TableName, aMuid);
end;

{**********************************************************************************************
* getObjectValueBlob
***********************************************************************************************}
function TMgtCore.getObjectValueBlob(aDataSource: TMgtDatasource; aMuid: int64; aField: string; var vStream: TMemoryStream): boolean;
begin
  Result := getObjectValueBlob(aDataSource.TableName, aMuid, aField, vStream);
end;

{**********************************************************************************************
* getObjectValueBlob
***********************************************************************************************}
function TMgtCore.getObjectValueBlob(aTableName: string; aMuid: int64; aField: string; var vStream: TMemoryStream): boolean;
var
  vResValue: string;
begin
  Result := false;

  // ����� �� ������ - �������
  if not Assigned(vStream) then
    exit;

  Result := getObjectValueFromTicket(aTableName, aMuid, aField, vResValue);

  if Result then
  begin
    // ����� � ������, ������������ � �����
    vStream.Write(Pointer(vResValue)^, Length(vResValue));
    exit;
  end;

  Result := getObjectValueBlobFromDB(aTableName, aMuid, aField, vStream);
end;

{**********************************************************************************************
* getObjectValueBlobFromDB
***********************************************************************************************}
function TMgtCore.getObjectValueBlobFromDB(aDataSource: TMgtDatasource; aMuid: int64; aField: string; var vStream: TMemoryStream): boolean;
begin
  Result := getObjectValueBlobFromDB(aDataSource.TableName, aMuid, aField, vStream);
end;

{**********************************************************************************************
* getObjectValueBlobFromDB
***********************************************************************************************}
function TMgtCore.getObjectValueBlobFromDB(aTableName: string; aMuid: int64; aField: string; var vStream: TMemoryStream): boolean;
var
  sql: string;
  dbRes: TDBResult;
  vSize: integer;
  vData: PByte;
begin
  Result := false;

  // ����� �� ������ - �������
  if not Assigned(vStream) then
    exit;

  FsqlParams.Clear();
  FsqlParams.itemsByKey['fields'] := conn.quoteName(aField);
  FsqlParams.itemsByKey['datasource'] := aTableName;
  FsqlParams.itemsByKey['muid_value'] := IntToStr(aMuid);
  if conn.CheckFieldExistence(aTableName, 'sign_deleted') = 0 then
    FsqlParams.itemsByKey['sign_deleted'] := 'sign_deleted = 0'
  else
    FsqlParams.itemsByKey['sign_deleted'] := '1 = 1';

  sql := getCardsSql('get_object_values', FsqlParams);
  try
    if (conn.QueryOpen(sql, dbRes, false) <> 0) then
      raise EMgtException.Create('������ ��� ��������� ������� �� ��.' + #13#10 + sql);

    if dbRes.Fetch() then
    begin
      vSize := dbRes.sizeValue(0);
      vData := PByte(dbRes.asDataPointer(0));
      vStream.Write(vData^, vSize);
      
      Result := true;
    end;

  finally
    FreeAndNil(dbRes);
  end;
end;

{**********************************************************************************************
* getObjectFullNameFromDB
***********************************************************************************************}
function TMgtCore.getObjectFullNameFromDB(aTableName: string; aMuid : int64): string;
var
  vSql: string;
  dbRes: TDBResult;
begin
  Result := '';

  FsqlParams.Clear();
  FsqlParams.itemsByKey['muid'] := IntToStr(aMuid);
  vSql := FsqlLoader.getSqlByAlias(ExtractFilePath(ParamStr(0)) + MAP_OBJECTS_SQL_FILE, aTableName, FsqlParams);
  if vSql = '' then
      exit;

  if (FConn.QueryOpen(vSql, dbRes, true) <> 0) then
  begin
    showDialog(dtError, dbsOk, '������ ��� ��������� ������������ �������', vSql);
    exit;
  end;

  if dbRes.Fetch() then
  begin
    Result := dbRes.asString(0);
    FreeAndNil(dbRes);
  end;
end;

{**********************************************************************************************
* getObjectFullName
***********************************************************************************************}
function TMgtCore.getObjectFullName(aTableName: string; aMuid: int64): string;
var
  ds: TMgtDatasource;
begin
  Result := '';

  ds := GetMgtDataSourceByTableName(aTableName);
  if (ds = nil) then
    exit;

  Result := getObjectFullName(ds, aMuid);
end;

{**********************************************************************************************
* getObjectFullName
***********************************************************************************************}
function TMgtCore.getObjectFullName(aDataSource: TMgtDatasource; aMuid: int64): string;
var
  FieldList: TMapStrings;
  id, val, val2: string;
  index: integer;
  vObject: TtrsGISMGTObject;
  muid : int64;
begin
  Result := '';

  // ������� ������ �������� ������������ ������� �������� �� ��
  vObject := GetObjectFromTicket(aDataSource, aMuid);
  if Assigned(vObject) and (vObject.Operation <> oAdd) then
    Result := getObjectFullNameFromDB(aDataSource.TableName, aMuid);

  if Result <> '' then
    exit;

  FieldList := TMapStrings.Create();

  if aDataSource.Alias = 'Routes' then
  begin
    FieldList.addItem('number', '');
    FieldList.addItem('route_transport_kind_muid', '');

    if getObjectValues(aDataSource, aMuid, FieldList) then
    begin
      Result := FieldList.items[0];

      id := FieldList.items[1];

      if (id <> '') then
        Result := getObjectValue( mgtDatasources[ 'RefRouteTransportKinds' ], StrToInt64(id), 'short_name') + ' ' + Result;
    end;
  end
  else if aDataSource.Alias = 'RouteVariants' then
  begin
    FieldList.addItem('start_date', '');
    FieldList.addItem('end_date', '');
    FieldList.addItem('route_muid', '');

    if (getObjectValues(aDataSource, aMuid, FieldList)) then
    begin
      Result := FieldList.items[0];

      if (Result <> '') then
      begin
        Result := '(' + Result + ' - ';

        val := FieldList.items[1];

        if (val <> '') then
          Result := Result + val + ')'
        else
          Result := Result + '...)'
      end;

      id := FieldList.items[2];

      if (id <> '') then
        Result := Trim(getObjectFullName(mgtDatasources[ 'Routes' ], StrToInt64(id)) + ' ' + Result);
    end;
  end
  else if aDataSource.Alias = 'RouteRounds' then
  begin
    FieldList.addItem('code', '');
    FieldList.addItem('route_variant_muid', '');
    FieldList.addItem('stop_place_A_muid', '');
    FieldList.addItem('stop_place_B_muid', '');

    if (getObjectValues(aDataSource, aMuid, FieldList)) then
    begin
      Result := FieldList.items[0];

      // �������
      id := FieldList.items[1];

      if (id <> '') then
      begin
        val := getObjectFullName( mgtDatasources[ 'RouteVariants' ], StrToInt64(id));

        if (val <> '') then
          Result := val + ', ' + Result;
      end;

      // ��1
      id := FieldList.items[2];

      if (id <> '') then
      begin
        val := getStopPlaceName(StrToInt64(id), true);

        if (val <> '') then
          Result := Result + ', ' + val;
      end;

      id := FieldList.items[3];

      // ��2
      if (id <> '') then
      begin
        val := getStopPlaceName(StrToInt64(id), true);

        if (val <> '') then
          Result := Result + ' - ' + val;
      end;
    end;
  end
  else if aDataSource.Alias = 'RouteNullRounds' then
  begin
    FieldList.addItem('code', '');
    FieldList.addItem('route_variant_muid', '');
    FieldList.addItem('stop_place_1_muid', '');
    FieldList.addItem('stop_place_2_muid', '');
    FieldList.addItem('stop_place_3_muid', '');
    FieldList.addItem('park_1_muid', '');
    FieldList.addItem('park_2_muid', '');
    FieldList.addItem('park_3_muid', '');

    if (getObjectValues(aDataSource, aMuid, FieldList)) then
    begin
      Result := FieldList.items[0];

      // �������
      id := FieldList.items[1];

      if (id <> '') then
      begin
        val := getObjectFullName( mgtDatasources[ 'RouteVariants' ], StrToInt64(id));

        if (val <> '') then
          Result := val + ', ' + Result;
      end;

      // 1
      id := FieldList.items[2];

      if (id <> '') then
      begin
        val := getStopPlaceName(StrToInt64(id));
        Result := Result + ', ' + val + ' - ';
      end
      else
      begin
        id := FieldList.items[5];
        val := '';

        if (id <> '') then
        begin
          val := getObjectFullName( mgtDatasources[ 'Parks' ], StrToInt64(id));
          Result := Result + ', ' + val + ' - ';
        end;
      end;

      // 1
      id := FieldList.items[3];

      if (id <> '') then
      begin
        val := getStopPlaceName(StrToInt64(id));
        Result := Result + val;
      end
      else
      begin
        id := FieldList.items[6];
        val := '';

        if (id <> '') then
        begin
          val := getObjectFullName(mgtDatasources[ 'Parks' ], StrToInt64(id));
          Result := Result + val;
        end;
      end;

      // 3
      id := FieldList.items[4];

      if (id <> '') then
      begin
        val := getStopPlaceName(StrToInt64(id));
        Result := Result + ' - ' + val;
      end
      else
      begin
        id := FieldList.items[7];
        val := '';

        if (id <> '') then
        begin
          val := getObjectFullName(mgtDatasources[ 'Parks' ], StrToInt64(id));
          Result := Result + ' - ' + val;
        end;
      end;
    end;
  end
  else if aDataSource.Alias = 'RouteTrajectories' then
  begin
    FieldList.addItem('route_round_muid', '');
    FieldList.addItem('route_null_round_muid', '');
    FieldList.addItem('trajectory_type_muid', '');

    if (getObjectValues(aDataSource, aMuid, FieldList)) then
    begin
      id := FieldList.items[2];

      if (id <> '') then
        Result := getObjectFullName( mgtDatasources[ 'RefRouteTrajectoryTypes' ] , StrToInt64(id));

      id := FieldList.items[0];

      if (id <> '') then
      begin
        val := getObjectFullName( mgtDatasources[ 'RouteRounds' ], StrToInt64(id));

        if (val <> '') then
          Result := val + ', ' + Result;
      end
      else
      begin
//            getObjectValueFromTicket(mgtDatasources[ 'RouteTrajectories' ], aMuid, 'length', val);
//            getObjectValueFromDB(mgtDatasources[ 'RouteTrajectories' ], aMuid, 'length', val2);
//
//            Result := UniFormatFloat('0', Abs(Round(UniStrToFloatDef(val, 0) * 1000) - Round(UniStrToFloatDef(val2, 0) * 1000)));

        id := FieldList.items[1];

        if (id <> '') then
        begin
          val := getObjectFullName( mgtDatasources[ 'RouteNullRounds' ], StrToInt64(id));

          if (val <> '') then
            Result := val + ', ' + Result;
        end;
      end;
    end;
  end
  else if aDataSource.Alias = 'Stops' then
  begin
    FieldList.addItem('name', '');
    FieldList.addItem('street_muid', '');
    FieldList.addItem('direction_muid', '');

    if (getObjectValues(aDataSource, aMuid, FieldList)) then
    begin
      Result := FieldList.items[0];

      id := FieldList.items[1];

      if (id <> '') then
        val := getObjectFullName( mgtDatasources[ 'Streets' ], StrToInt64(id))
      else
        val := '';

      id := FieldList.items[2];

      if (id <> '') then
        val2 := getObjectFullName( mgtDatasources[ 'RefMovementDirections' ], StrToInt64(id))
      else
        val2 := '';

      if (val = '') then
        val := val2
      else if (val2 <> '') then
        val := val + ', ' + val2;

      if (val <> '') then
        Result := Result + ' [' + val + ']';
    end;
  end
  else if aDataSource.Alias = 'StopPlaces' then
  begin
    FieldList.addItem('suffix', '');
    FieldList.addItem('stop_muid', '');

    if (getObjectValues(aDataSource, aMuid, FieldList)) then
    begin
      id := FieldList.items[1];

      if (id <> '') then
        Result := getObjectFullName( mgtDatasources[ 'Stops' ], StrToInt64(id))
      else
        Result := '';

      if (FieldList.items[0] <> '') then
      begin
        index := Pos('[', Result);

        if (index = 0) then
          Result := Result + ' ' + FieldList.items[0]
        else
          Result := Copy(Result, 1, index - 1) + FieldList.items[0] + ' ' + Copy(Result, index, Length(Result));
      end;
    end;
  end
  else if aDataSource.Alias = 'StopZones' then
  begin
    id := getObjectValue(aDataSource, aMuid, 'stop_place_muid');

    if (id <> '') then
      Result := getObjectFullName( mgtDatasources[ 'StopPlaces' ], StrToInt64(id));

    Result := '���� ��������� ' + Result;
  end
  else if aDataSource.Alias = 'StopPavilions' then
  begin
    FieldList.addItem('inventory_district', '');
    FieldList.addItem('inventory_year', '');
    FieldList.addItem('inventory_number', '');

    if (getObjectValues(aDataSource, aMuid, FieldList)) then
    begin
      Result := '��������: ' + FieldList.items[0] + '-' + FieldList.items[1] + '-' + FieldList.items[2];
    end;
  end
  else if aDataSource.Alias = 'StopPavilionInstallations' then
  begin
    id := getObjectValue(aDataSource, aMuid, 'installation_date');

    Result := '��������� ���������: ' + id;
  end
  else if aDataSource.Alias = 'TerminalPointZones' then
  begin
    FieldList.addItem('route_muid', '');
    FieldList.addItem('stop_place_muid', '');

    if (getObjectValues(aDataSource, aMuid, FieldList)) then
    begin
      Result := '���� ��: ������� ';
      id := FieldList.items[0];

      if (id <> '') then
        Result := Result + getObjectFullName( mgtDatasources[ 'Routes' ], StrToInt64(id))
      else
        Result := Result + '-';

      Result := Result + ', ��������� ';
      id := FieldList.items[1];

      if (id <> '') then
        Result := Result + getObjectFullName( mgtDatasources[ 'StopPlaces' ], StrToInt64(id))
      else
        Result := Result + '-';
    end;
  end
  else if aDataSource.Alias = 'ParkZones' then
  begin
    id := getObjectValue(aDataSource, aMuid, 'park_muid');

    if (id <> '') then
      Result := getObjectFullName( mgtDatasources[ 'Parks' ], StrToInt64(id));

    Result := '���� ����� ' + Result;
  end
  else if aDataSource.Alias = 'GraphNodes' then
  begin
    FieldList.addItem('street_muid', '');
    FieldList.addItem('direction_muid', '');
    if (getObjectValues(aDataSource, aMuid, FieldList)) then
    begin
      id := FieldList.items[0];

      if (id <> '') then
        val := getObjectFullName( mgtDatasources[ 'Streets' ], StrToInt64(id))
      else
        val := '-';

      id := FieldList.items[1];

      if (id <> '') then
        val2 := getObjectFullName( mgtDatasources[ 'RefMovementDirections' ] , StrToInt64(id))
      else
        val2 := '-';

      if (val = '') then
        Result := val2
      else if (val2 <> '') then
        Result := val + ', ' + val2
      else
        Result := val;
    end;
  end
  else if aDataSource.Alias = 'GraphSections' then
  begin
    FieldList.addItem('startNodeMuid', '');
    FieldList.addItem('endNodeMuid', '');

    if (getObjectValues(aDataSource, aMuid, FieldList)) then
    begin
      id := FieldList.items[0];

      if (id <> '') then
        val := getObjectFullName( mgtDatasources[ 'GraphNodes' ], StrToInt64(id))
      else
        val := '-';

      id := FieldList.items[1];

      if (id <> '') then
        val2 := getObjectFullName( mgtDatasources[ 'GraphNodes' ], StrToInt64(id))
      else
        val2 := '-';

      Result := val;
      if val2 <> '' then
        Result := Result + ' - ' + val2;
    end;
  end
  else if aDataSource.Alias = 'Roads ' then
  begin
    id := getObjectValue(aDataSource, aMuid, 'street_muid');

    if (id <> '') and (id <> '0') then
      Result := getObjectFullName(mgtDatasources[ 'Streets' ], StrToInt64(id));
  end //������ ����
  else if aDataSource.Alias = 'DMR_transport_hubs' then
      Result := getObjectValue(aDataSource, aMuid, 'TPUName')
  else if aDataSource.Alias = 'Boundary_points' then
    Result := '����� � '  +getObjectValue(aDataSource, aMuid, 'no')
  else if aDataSource.Alias = 'Turnouts' then
    Result := '���� � '   +getObjectValue(aDataSource, aMuid, 'no')
  else if aDataSource.Alias = 'Nodes' then
    Result := '���� � '   +getObjectValue(aDataSource, aMuid, 'NODE_NUMBER')
  else if aDataSource.Alias = 'Sites_passport' then
    Result := '������� � '+getObjectValue(aDataSource, aMuid, 'no') //----------
  else if aDataSource.Alias = 'RefSignpostInstallers' then
  begin
    Result := core.getObjectValue(aDataSource, aMuid, 'last_name');
    val := core.getObjectValue(aDataSource, aMuid, 'first_name');
    if val <> '' then
      Result := Result + ' ' + LeftStr(val, 1) + '.';
    val := core.getObjectValue(aDataSource, aMuid, 'patronymic_name');
    if val <> '' then
      Result := Result + ' ' + LeftStr(val, 1) + '.';
  end
  else if aDataSource.Alias = 'CustomContours' then
    Result := '������������ ������: ' + getObjectValue(aDataSource, aMuid, 'name')
  else if aDataSource.Alias = 'Buildings' then
      Result := getObjectValue(aDataSource, aMuid, 'full_address')


    {mtLnkRouteTrajectoriesCheckPoints,
    mtLnkRouteTrajectoriesStopPlaces,
    mtLnkRouteTrajectoriesGraphSections,
    mtLnkParksRoutes,
    mtLnkParksVehicleTypes,
    mtLnkParksAgencies,
    mtLnkCompensatoryRoutesPoints,
    mtRoadsAxis,
    mtRasterImages:
      Result := getObjectValue(aDataSource, aMuid, 'muid');}

  else if aDataSource.Alias = 'EgkoRegions' then
  begin
    id := getObjectValue(aDataSource, aMuid, 'region_muid');

    if (id <> '') then
      Result := getObjectFullName(mgtDatasources['OmkRegions'], StrToInt64(id));
  end
  else if aDataSource.Alias = 'EgkoDistricts' then
  begin
    id := getObjectValue(aDataSource, aMuid, 'district_muid');

    if (id <> '') then
      Result := getObjectFullName(mgtDatasources[ 'OmkDistricts' ], StrToInt64(id));
  end
  else if aDataSource.Alias = 'DisplayPanels' then
    Result := '����� ' + getObjectValue(aDataSource, aMuid, 'code')
  else if aDataSource.Alias = 'WifiEquipment' then
    Result := 'Wifi ' + getObjectValue(aDataSource, aMuid, 'serial_number')
  else if aDataSource.Alias = 'VideoCameras' then
    Result := '����������� ' + getObjectValue(aDataSource, aMuid, 'serial_number')
  else if ( aDataSource.Alias = 'DisplayPanelServiceContracts' )
          or ( aDataSource.Alias = 'WifiEquipmentServiceContracts' ) then
    Result := '�������� ' + getObjectValue(aDataSource, aMuid, 'number')
  else if ( aDataSource.Alias = 'AttachmentsOrders') or
    ( aDataSource.Alias = 'AttachmentsStopPavilionOrders') or
    ( aDataSource.Alias = 'AttachmentsStopPavilions') or
    ( aDataSource.Alias = 'AttachmentsDisplayPanelPhotos') or
    ( aDataSource.Alias = 'AttachmentsDisplayPanelServiceContracts') or
    ( aDataSource.Alias = 'AttachmentsWifiEquipmentPhotos') or
    ( aDataSource.Alias = 'AttachmentsWifiEquipmentServiceContracts') then
  begin
    Result := getObjectValue(aDataSource, aMuid, 'caption');

    if Result = '' then
      Result := getObjectValue(aDataSource, aMuid, 'filename');
  end
  else if aDataSource.Alias = 'StopSchemes' then
    Result := getObjectValue(aDataSource, aMuid, 'name')
  else if aDataSource.Alias = 'PosterApplications' then
    Result := '������ ' + getObjectValue(aDataSource, aMuid, 'name')
  else if aDataSource.Alias = 'SubwayStations' then
    Result := '������� ����� ' + getObjectValue(aDataSource, aMuid, 'name')
  else if aDataSource.Alias = 'SubwayLines' then
    Result := '����� ����� ' + getObjectValue(aDataSource, aMuid, 'name')
  else if aDataSource.Alias = 'lnkSubwayStationsStations' then
  begin
    Result := '��������� ����� ';

    muid := StrToInt64Def( getObjectValue(aDataSource, aMuid, 'subway_staion_1_muid'), -1 );
    if muid <> -1 then
      Result := Result + getObjectValue(mgtDatasources[ 'SubwayStations' ], muid, 'name') + '-';

    muid := StrToInt64Def( getObjectValue(aDataSource, aMuid, 'subway_staion_2_muid'), -1 );
    if muid <> -1 then
      Result := Result + getObjectValue(mgtDatasources[ 'SubwayStations' ], muid, 'name');
  end
  else if aDataSource.Alias = 'SubwayStationEntrances' then
  begin
    Result := '����/����� ������� ����� ';

    muid := StrToInt64Def( getObjectValue(aDataSource, aMuid, 'subway_station_muid'), -1 );
    if muid <> -1 then
      Result := Result + getObjectValue(mgtDatasources[ 'SubwayStations' ], muid, 'name');
  end
  else if aDataSource.Alias = 'SubwayTracks' then
  begin
    Result := '������ ����� ����� ';
    muid := StrToInt64Def( getObjectValue(aDataSource, aMuid, 'subway_line_muid'), -1 );
    if muid <> -1 then
      Result := Result + getObjectValue(mgtDatasources[ 'SubwayLines' ], muid, 'name')
  end
  else if aDataSource.Alias = 'LnkSubwayTracksStation' then
  begin
    Result := '������� � ������ ����� ����� ';

    muid := StrToInt64Def(  getObjectValue(aDataSource, aMuid, 'subway_track_muid'), -1 );
    if muid <> -1 then
    begin
      muid := StrToInt64Def( getObjectValue(mgtDatasources[ 'SubwayTracks' ], muid, 'subway_line_muid'), -1 );
      if muid <> -1 then
        Result := Result + getObjectValue(mgtDatasources[ 'SubwayLines' ], muid, 'name');
    end;
  end
  else if aDataSource.Alias = 'AeroexpressStations' then
    Result := '������� ������������� ' + getObjectValue(aDataSource, aMuid, 'name')
  else if aDataSource.Alias = 'AeroexpressLines' then
    Result := '����� ������������� ' + getObjectValue(aDataSource, aMuid, 'name')
  else if aDataSource.Alias = 'AeroexpressSchedules' then
  begin
    Result := '���������� ������������� ';

    muid := StrToInt64Def( getObjectValue(aDataSource, aMuid, 'aeroexpress_station_muid'), -1 );
    if muid <> -1 then
      Result := Result + getObjectValue(mgtDatasources[ 'AeroexpressStations' ], muid, 'name');
  end
  else
    Result := getObjectValue(aDataSource, aMuid, 'name');

  if (Result = '') then
    Result := '��� ��������';

  FreeAndNil(FieldList);
end;

{**********************************************************************************************
* getStopPlaceName
// �������� ������������ ��������� �� ����� ����� �������-�������
***********************************************************************************************}
function TMgtCore.getStopPlaceName(aStopPlaceMuid: int64; aFlNameForTerminalPoint : boolean = false) : string;
var
  stopMuid: int64;
  field: string;
begin
  Result := '';
  if aStopPlaceMuid <= 0 then
    exit;

  stopMuid := StrToInt64Def( getObjectValue( mgtDatasources[ 'StopPlaces' ], aStopPlaceMuid, 'stop_muid'), -1);
  Result := '����� ���������';
  if stopMuid <= 0 then
    exit;

  field := 'name';
  if aFlNameForTerminalPoint then
    field := 'name_for_terminal_point';

  Result := getObjectValue(mgtDatasources[ 'Stops' ], stopMuid, field);
end;

{**********************************************************************************************
* GetStopPlaceCaption
***********************************************************************************************}
function TMgtCore.getStopPlaceCaption(aStopPlaceMuid: int64; aFlNameForTerminalPoint: boolean): string;
var
  vStopName, vSuffix: string;
begin
  Result := '';

  vStopName := getStopPlaceName(aStopPlaceMuid, aFlNameForTerminalPoint);
  vSuffix   := getObjectValue(mgtDatasources[ 'StopPlaces' ], aStopPlaceMuid, 'suffix');
  Result := vStopName + ' ' + vSuffix;
end;

{**********************************************************************************************
* getStopPlaceStreetDirection
***********************************************************************************************}
function TMgtCore.getStopPlaceStreetDirection(aStopPlaceMuid: int64; var vStreet: string; var vDirection: string) : boolean;
var
  vStopMuid, vMuid: int64;
  vFields: TMapStrings;
begin
  Result := false;
  vStreet := '';
  vDirection := '';
  if aStopPlaceMuid <= 0 then
    exit;

  vStopMuid := StrToInt64Def( getObjectValue(mgtDatasources[ 'StopPlaces' ], aStopPlaceMuid, 'stop_muid'), -1);
  if vStopMuid <= 0 then
    exit;

  vFields := TMapStrings.Create();
  vFields.addItem('street_muid', '');
  vFields.addItem('direction_muid', '');
  try
    if not getObjectValues(mgtDatasources[ 'Stops' ], vStopMuid, vFields) then
      exit;

    vMuid := StrToInt64Def(vFields.itemsByKey['street_muid'], -1);
    if vMuid > 0 then
      vStreet := getObjectValue( mgtDatasources[ 'Streets' ], vMuid, 'short_name');
    vMuid := StrToInt64Def(vFields.itemsByKey['direction_muid'], -1);
    if vMuid > 0 then
      vDirection := getObjectValue( mgtDatasources[ 'RefMovementDirections' ], vMuid, 'short_name');
  finally
    FreeAndNil(vFields);
  end;
end;

{**********************************************************************************************
* getStopPlaceTransportKindName
***********************************************************************************************}
function TMgtCore.getStopPlaceTransportKindName(aHasBus, aHasTrolley, aHasTram, flShort: boolean): string;
begin
  Result := '';
  if flShort then
  begin
    if aHasBus then
      Result := mgtRouteTransportKindShortNames[tkBus];
    if aHasTrolley then
      Result := Result + mgtRouteTransportKindShortNames[tkTrolley];
    if aHasTram then
      Result := Result + mgtRouteTransportKindShortNames[tkTram];

    exit;
  end;

  if aHasBus then
    Result := '�������';

  if aHasTrolley then
  begin
    if Result <> '' then
      Result := Result + ', ' + '����������'
    else
      Result := '����������';
  end;

  if aHasTram then
  begin
    if Result <> '' then
      Result := Result + ', ' + '�������'
    else
      Result := '�������';
  end;

  if Result = '' then
    Result := '�� ��������';
end;

{**********************************************************************************************
* getRouteStatusByState
***********************************************************************************************}
function TMgtCore.getRouteStatusByState(aState: EMgtRouteState; aFlTemporary: boolean): EMgtRouteState2;
begin
  Result := rs2Undefined;

  case aState of
    rsForApproval:
      begin
        Result := rs2ForApproval;
        if aFlTemporary then
          Result := rs2TemporaryClosed;
      end;
    rsOpened:
      begin
        Result := rs2Opened;
        if aFlTemporary then
          Result := rs2TemporaryOpened;
      end;
    rsClosed:
      begin
        Result := rs2Closed;
        if aFlTemporary then
          Result := rs2TemporaryClosed;
      end;
  end;
end;

{**********************************************************************************************
* getRouteVariantStateByDates
***********************************************************************************************}
function TMgtCore.getRouteVariantStateByDates(aStartDate, aEndDate: TDate): string;
begin
  Result := '�� ��������';

  if (aStartDate < 0) or (aStartDate > Date()) then
    Result := '�����������'
  else if (aStartDate > 0) and (aEndDate > 0) and (aEndDate < Date()) then
    Result := '��������'
  else if (aStartDate <= Date()) then
    Result := '�����������';
end;

{**********************************************************************************************
* getStopPavilionFullInventoryNumber
***********************************************************************************************}
function TMgtCore.getStopPavilionFullInventoryNumber(aStopPavilionMuid: int64): string;
var
  vMuid: Int64;
  vNumPreffix: string;
  vFields: TMapStrings;
begin
  Result := '';
  if aStopPavilionMuid <= 0 then
    exit;

  vFields := TMapStrings.Create();
  vFields.addItem('balance_organization_muid', '');
  vFields.addItem('inventory_district', '');
  vFields.addItem('inventory_year', '');
  vFields.addItem('inventory_number', '');

  try
    if not getObjectValues(mgtDatasources[ 'StopPavilions' ], aStopPavilionMuid, vFields) then
      exit;

    vNumPreffix := '';

    vMuid := StrToInt64Def(vFields.itemsByKey['balance_organization_muid'], -1);
    if vMuid > 0 then
      vNumPreffix := core.getObjectValue(mgtDatasources[ 'RefStopPavilionBalanceOrganizations' ], vMuid, 'short_name');

    Result := vFields.itemsByKey['inventory_district'] + '-' + RightStr(vFields.itemsByKey['inventory_year'], 2) +
             '-' + vNumPreffix + vFields.itemsByKey['inventory_number'];
  finally
    FreeAndNil(vFields);
  end;
end;

{**********************************************************************************************
* stopPlaceHasRoutesByAgency
***********************************************************************************************}
procedure TMgtCore.stopPlaceHasRoutesByAgency(aStopPlaceMuid: int64; var flHasMGTRoutes, flHasCommercialRoutes: boolean);
var
  vStopPlaces: TStringList;
begin
  vStopPlaces := TStringList.Create();
  vStopPlaces.Add(IntToStr(aStopPlaceMuid));
  stopPlacesHasRoutesByAgency(vStopPlaces, flHasMGTRoutes, flHasCommercialRoutes);
  FreeAndNil(vStopPlaces);
end;

{**********************************************************************************************
* stopPlacesHasRoutesByAgency
***********************************************************************************************}
procedure TMgtCore.stopPlacesHasRoutesByAgency(aStopPlaces: TStringList; var flHasMGTRoutes, flHasCommercialRoutes: boolean);
var
  vSql: string;
  dbRes: TDBResult;
begin
  flHasMGTRoutes := false;
  flHasCommercialRoutes := false;

  if not Assigned(aStopPlaces) then
    exit;

  if aStopPlaces.Count = 0 then
    exit;

  aStopPlaces.Delimiter := ',';
  FsqlParams.Clear();
  FsqlParams.itemsByKey['stop_places'] := aStopPlaces.DelimitedText;
  vSql := core.getCardsSQL('StopPlaceHasRoutesByAgency', FsqlParams);
  if FConn.QueryOpen(vSql, dbRes, false) <> 0 then
    raise EMgtException.Create('������ ��� ����������� ��������� ��������� � ������� "�����������"');

  if dbRes.Fetch() then
  begin
    flHasMGTRoutes := dbRes.asBool(0);
    flHasCommercialRoutes := dbRes.asBool(1);
  end;

  FreeAndNil(dbRes);
end;

{**********************************************************************************************
* copyRouteVariant
// ����������� ������� (�������� � ����� �� ����������), ��������� � ���������� ��������
***********************************************************************************************}
function TMgtCore.copyRouteVariant(aFromMuid, aToRouteMuid: int64): TtrsGISMGTObject;
var
  copyFields: TMapStrings;
  i : integer;
  jsonObjectList: TlkJSONlist;
  jsonObj : TlkJSONobject;
  muid : int64;
  datasource: TMgtDatasource;
begin
  datasource := mgtDatasources[ 'RouteVariants' ];
  copyFields := getDatasourceFields(datasource);

  // �������� �������� ����� ������� (�� �� � ������)
  getObjectValues(datasource, aFromMuid, copyFields);
  muid := GenerateMUID();
  Result := AddObjectToTicket(datasource, muid, oAdd);

  // �������� ����
  for i := 0 to copyFields.Count - 1 do
  begin
    Result.MainData.SetFieldValue(copyFields.keys[i], copyFields.items[i]);
  end;

  // ����� � ��������� � ������������ ����
  Result.MainData.SetFieldValue('route_muid', IntToStr(aToRouteMuid));
  Result.MainData.SetFieldValue('order_muid', NULL_VALUE);
  Result.MainData.SetFieldValue('start_date', NULL_VALUE);
  Result.MainData.SetFieldValue('end_date', NULL_VALUE);

  // ����������� ����� (��������, �.�. ������ ��� ���������� ������������ ����)
  AddObjectLinks(datasource, muid);

  // �������� �����
  // �������� ����� �������� ��������
  jsonObjectList := core.getObjects(mgtDatasources[ 'RouteRounds' ], 'route_variant_muid', IntToStr(aFromMuid), ['muid']);
  for i := 0 to jsonObjectList.count - 1 do
  begin
    jsonObj := jsonObjectList.asObject[i];
    muid := StrToInt64(jsonObj.asString['muid']);

    // �������� ������ ����, ����������� � ������ ��������
    copyRouteRound(muid, Result.ObjMUID);
  end;
  FreeAndNil(jsonObjectList);

  // �������� ������� �����
  // �������� ����� �������� ��������
  jsonObjectList := core.getObjects(mgtDatasources[ 'RouteNullRounds' ], 'route_variant_muid', IntToStr(aFromMuid), ['muid']);
  for i := 0 to jsonObjectList.count - 1 do
  begin
    jsonObj := jsonObjectList.asObject[i];
    muid := StrToInt64(jsonObj.asString['muid']);

    // �������� ������ ����, ����������� � ������ ��������
    copyRouteNullRound(muid, Result.ObjMUID);
  end;
  FreeAndNil(jsonObjectList);

  copyFields.Free();
end;

{**********************************************************************************************
* copyRouteRound
// ����������� ���� (�������� � ����� �� ����������), ��������� � ���������� ��������
***********************************************************************************************}
function TMgtCore.copyRouteRound(aFromMuid, aToVariantMuid: int64): TtrsGISMGTObject;
var
  copyFields: TMapStrings;
  i : integer;
  jsonObjectList: TlkJSONlist;
  jsonObj : TlkJSONobject;
  muid : int64;
  datasource: TMgtDatasource;

begin
  datasource := mgtDatasources[ 'RouteRounds' ];
  copyFields := getDatasourceFields(datasource);
  // �������� �������� ����� ������� (�� �� � ������)
  getObjectValues(datasource, aFromMuid, copyFields);
  muid := GenerateMUID();
  Result := AddObjectToTicket(datasource, muid, oAdd);

  // �������� ����
  for i := 0 to copyFields.Count - 1 do
  begin
    Result.MainData.SetFieldValue(copyFields.keys[i], copyFields.items[i]);
  end;

  // ����� � ��������� � ������������ ����
  Result.MainData.SetFieldValue('route_variant_muid', IntToStr(aToVariantMuid));
  // ����������� ����� (��������, �.�. ������ ��� ���������� ������������ ����)
  AddObjectLinks(datasource, muid);

  // �������� ����������
  jsonObjectList := core.getObjects(mgtDatasources[ 'RouteTrajectories' ], 'route_round_muid', IntToStr(aFromMuid), ['muid']);
  for i := 0 to jsonObjectList.count - 1 do
  begin
    jsonObj := jsonObjectList.asObject[i];
    muid := StrToInt64(jsonObj.asString['muid']);

    // �������� ������ ����������, ����������� � ������ �����
    copyRouteTrajectory(muid, Result.ObjMUID, rtrRound);
  end;
  FreeAndNil(jsonObjectList);

  copyFields.Free();
end;

{**********************************************************************************************
* copyRouteNullRound
// ����������� ������� ���� (�������� � ����� �� ����������), ��������� � ���������� ��������
***********************************************************************************************}
function TMgtCore.copyRouteNullRound(aFromMuid, aToVariantMuid: int64): TtrsGISMGTObject;
var
  copyFields: TMapStrings;
  i : integer;
  jsonObjectList: TlkJSONlist;
  jsonObj : TlkJSONobject;
  muid : int64;
  datasource: TMgtDatasource;

begin
  datasource := mgtDatasources[ 'RouteNullRounds' ];
  copyFields := getDatasourceFields(datasource);

  // �������� �������� ����� ������� (�� �� � ������)
  getObjectValues(datasource, aFromMuid, copyFields);
  muid := GenerateMUID();
  Result := AddObjectToTicket(datasource, muid, oAdd);

  // �������� ����
  for i := 0 to copyFields.Count - 1 do
  begin
    Result.MainData.SetFieldValue(copyFields.keys[i], copyFields.items[i]);
  end;

  // ����� � ��������� � ������������ ����
  Result.MainData.SetFieldValue('route_variant_muid', IntToStr(aToVariantMuid));
  // ����������� ����� (��������, �.�. ������ ��� ���������� ������������ ����)
  AddObjectLinks(datasource, muid);

  // �������� ����������
  jsonObjectList := core.getObjects(mgtDatasources[ 'RouteTrajectories' ], 'route_null_round_muid', IntToStr(aFromMuid), ['muid']);
  for i := 0 to jsonObjectList.count - 1 do
  begin
    jsonObj := jsonObjectList.asObject[i];
    muid := StrToInt64(jsonObj.asString['muid']);

    // �������� ������ ����������, ����������� � ������ �����
    copyRouteTrajectory(muid, Result.ObjMUID, rtrNullRound);
  end;
  FreeAndNil(jsonObjectList);

  copyFields.Free();
end;

{**********************************************************************************************
* copyRouteTrajectory
// ����������� ���������� (�������� � ����� �� ����������), ��������� � ���������� �����
***********************************************************************************************}
function TMgtCore.copyRouteTrajectory(aFromMuid, aToRoundMuid: int64; aTrajectoryRound : EMgtRouteTrajectoryRoundType; aTrajectoryType: EMgtRouteTrajectoryType = rttUndefined): TtrsGISMGTObject;
var
  copyFields: TMapStrings;
  i : integer;
  muid : int64;
  datasource: TMgtDatasource;

begin
  datasource := mgtDatasources[ 'RouteTrajectories' ];
  copyFields := getDatasourceFields(datasource);
  // �������� �������� ����� ������� (�� �� � ������)
  getObjectValues(datasource, aFromMuid, copyFields);
  muid := GenerateMUID();
  Result := AddObjectToTicket(datasource, muid, oAdd);

  // �������� ����
  for i := 0 to copyFields.Count - 1 do
  begin
    Result.MainData.SetFieldValue(copyFields.keys[i], copyFields.items[i]);
  end;

  // ����� � ������/������� ������
  case aTrajectoryRound of
    rtrUndefined: raise Exception.Create('������� ������������ ��� ����� ��� ����������');
    rtrRound:
      begin
        Result.MainData.SetFieldValue('route_round_muid', IntToStr(aToRoundMuid));
        Result.MainData.SetFieldValue('route_null_round_muid', NULL_VALUE);
      end;
    rtrNullRound:
      begin
        Result.MainData.SetFieldValue('route_round_muid', NULL_VALUE);
        Result.MainData.SetFieldValue('route_null_round_muid', IntToStr(aToRoundMuid));
      end;
  end;
  if aTrajectoryType <> rttUndefined then   // ���� ���� �������� ��� ����������, ����������� ���
    Result.MainData.SetFieldValue('trajectory_type_muid', IntToStr(Integer(aTrajectoryType)) );

  // ����������� ����� (��������, �.�. ������ ��� ���������� ������������ ����)
  AddObjectLinks(datasource, muid);

  // �������� ����� � �����������
  copyLnkObjects(Result, mgtDatasources[ 'LnkRouteTrajectoriesStopPlaces' ], 'route_trajectory_muid', aFromMuid, Result.ObjMUID);
  // �������� ����� � �����������
  copyLnkObjects(Result, mgtDatasources[ 'LnkRouteTrajectoriesCheckPoints' ], 'route_trajectory_muid', aFromMuid, Result.ObjMUID);
  // �������� ����� � ������
  copyLnkObjects(Result, mgtDatasources[ 'LnkRouteTrajectoriesGraphSections' ], 'route_trajectory_muid', aFromMuid, Result.ObjMUID);
  // �������� ����� � ��������
  copyLnkObjects(Result, mgtDatasources[ 'LnkRouteTrajectoriesRegions' ], 'route_trajectory_muid', aFromMuid, Result.ObjMUID);
  // �������� ����� � �������
  copyLnkObjects(Result, mgtDatasources[ 'LnkRouteTrajectoriesStreets' ], 'route_trajectory_muid', aFromMuid, Result.ObjMUID);

  copyFields.Free();
end;

{**********************************************************************************************
* copyLnkObjects
// ����������� ����� ��� ���������� ���������, ��� ���������� ������������� �������
***********************************************************************************************}
procedure TMgtCore.copyLnkObjects(aObject: TtrsGISMGTObject; aDataSource: TMgtDatasource; aParentField: string; aFromParentMuid, aToParentMuid: int64);
var
  copyFields: TMapStrings;
  i : integer;
  jsonObjectList: TlkJSONlist;
  jsonObj : TlkJSONobject;
  muid : int64;
//  obj : TtrsObject;
begin
  copyFields := getDatasourceFields(aDataSource);
  // ����� � ������������ ��������
  copyFields.addItem(aParentField, IntToStr(aToParentMuid));

  jsonObjectList := core.getObjects(aDataSource, aParentField, IntToStr(aFromParentMuid), ['muid']);
  for i := 0 to jsonObjectList.count - 1 do
  begin
    jsonObj := jsonObjectList.asObject[i];
    muid := StrToInt64(jsonObj.asString['muid']);

    // �������� �������� ����� ������� (�� �� � ������)
    getObjectValues(aDataSource, muid, copyFields);
    muid := GenerateMUID();
    // ����� � ������������ ��������
    copyFields.itemsByKey[aParentField] := IntToStr(aToParentMuid);
    AddSlaveDataToTicket(aObject, aDataSource, muid, oAdd, copyFields);
  end;
  FreeAndNil(jsonObjectList);
  FreeAndNil(copyFields);
end;

{**********************************************************************************************
* updateRouteByVariantDates
***********************************************************************************************}
procedure TMgtCore.updateRouteByVariantDates(aMuid: int64);
var
  jsonObjectList: TlkJSONlist;
  jsonObj : TlkJSONobject;
  vRouteObj: TtrsObject;
  vMuid, vRouteMuid: int64;
  vCurDate, vStartDate, vEndDate, vNewEndDate, vNewStartDate: TDateTime;
  vVariantStartDate, vVariantEndDate: TDateTime;
  i: integer;
  vRouteState: EMgtRouteState;
  vRouteState2: EMgtRouteState2;
  vCurrentVariantMuid: string;
  vRouteStateMuid, vSettedVariantMuid: string;
  vFieldList: TMapStrings;
  flHasChanges, flTempRoute, flVariantsInFuture: boolean;
begin
  // �� ��������� - �� ����������� � ��� ��������� ��������
  vRouteState := rsForApproval;
  vCurrentVariantMuid := '';
  vCurDate := Date();
  vRouteMuid := StrToInt64(core.getObjectValue(mgtDatasources[ 'RouteVariants' ], aMuid, 'route_muid', '-1'));
  if vRouteMuid <= 0 then
    exit;

  vRouteObj := GetObjectFromTicket( mgtDatasources[ 'Routes' ], vRouteMuid);
  flHasChanges := false;


  vFieldList := TMapStrings.Create();
  try
    vFieldList.addItem('route_state_muid', '');
    vFieldList.addItem('current_route_variant_muid', '');
    vFieldList.addItem('is_temporary', '');
    vFieldList.addItem('open_date', '');
    vFieldList.addItem('close_date', '');

    core.getObjectValues(mgtDatasources[ 'Routes' ], vRouteMuid, vFieldList);

    vRouteStateMuid := vFieldList['route_state_muid'];
    vSettedVariantMuid := vFieldList['current_route_variant_muid'];
    vStartDate := StrToDateTime(vFieldList['open_date'], 'y/m/d');
    vEndDate := StrToDateTime(vFieldList['close_date'], 'y/m/d');
    flTempRoute := vFieldList['is_temporary'] = '1';
  finally
    FreeAndNil(vFieldList);
  end;

  // ���� ������� ���������, ������ ������ �� ����
  if Assigned(vRouteObj) and (vRouteObj.Operation = oDelete) then
    exit;

  // �������� ������ ���� ��������� ��������
  jsonObjectList := core.getObjects(mgtDatasources[ 'RouteVariants' ], 'route_muid', IntToStr(vRouteMuid),
                                   ['muid', 'start_date', 'end_date']);
  try
    vNewEndDate := NULL_DATE;
    vNewStartDate := NULL_DATE;
    flVariantsInFuture := false;
    for i := 0 to jsonObjectList.count - 1 do
    begin
      jsonObj := jsonObjectList.asObject[i];
      vMuid := StrToInt64(jsonObj.asString['muid']);

      vVariantStartDate := StrToDateTime(jsonObj.asString['start_date'], 'y/m/d');
      vVariantEndDate := StrToDateTime(jsonObj.asString['end_date'], 'y/m/d');

      if ((vNewStartDate = NULL_DATE) or (vNewStartDate > vVariantStartDate)) and (vVariantStartDate > 0) then
        vNewStartDate := vVariantStartDate;

      if vRouteState = rsOpened then
        Continue;

      if ((vVariantEndDate < vCurDate) and (vVariantEndDate > 0)) then
      begin
        if not flVariantsInFuture then
          vRouteState := rsClosed;
        if vVariantEndDate > vNewEndDate then
          vNewEndDate := vVariantEndDate;
        Continue;
      end;

      if ((vVariantStartDate <= vCurDate) and (vVariantStartDate > 0)) and ((vVariantEndDate >= vCurDate) or (vVariantEndDate < 0)) then
      begin
        vRouteState := rsOpened;
        vCurrentVariantMuid := IntToStr(vMuid);
        vNewEndDate := NULL_DATE;
        Continue;
      end;

      //���� �������� � �������
      if ((vVariantStartDate > vCurDate) and (vVariantStartDate > 0)) then
      begin
        vRouteState := rsForApproval;
        flVariantsInFuture := true;
      end;
    end;
  finally
    FreeAndNil(jsonObjectList);
  end;

  // ���� ���������� ������
  if vRouteStateMuid <> IntToStr(Integer(vRouteState)) then
  begin
    if not Assigned(vRouteObj) then
      vRouteObj := core.AddObjectToTicket(mgtDatasources[ 'Routes' ], vRouteMuid, oEdit);

    vRouteState2 := getRouteStatusByState(vRouteState, flTempRoute);
    core.setFieldValue(vRouteObj.MainData, 'route_state_muid', Integer(vRouteState));
    core.setFieldValue(vRouteObj.MainData, 'route_state2_muid', Integer(vRouteState2));
    // ������� �����������
    if vRouteState = rsOpened then
      core.setFieldValue(vRouteObj.MainData, 'close_date', '');
    flHasChanges := true;
  end;

  // ���� ���������� �������
  if vSettedVariantMuid <> vCurrentVariantMuid then
  begin
    if not Assigned(vRouteObj) then
      vRouteObj := core.AddObjectToTicket(mgtDatasources[ 'Routes' ], vRouteMuid, oEdit);
    core.setFieldValue(vRouteObj.MainData, 'current_route_variant_muid', vCurrentVariantMuid);

    flHasChanges := true;
  end;

  // ���� ���������� ���� ��������
  if ((vStartDate > vNewStartDate) or (vStartDate < 0)) and (vNewStartDate > 0) then
  begin
    if not Assigned(vRouteObj) then
      vRouteObj := core.AddObjectToTicket(mgtDatasources[ 'Routes' ], vRouteMuid, oEdit);
    core.setFieldValue(vRouteObj.MainData, 'open_date', DateToMySqlStr(vNewStartDate));
    flHasChanges := true;
  end;
  // ���� ���������� ���� ��������
  if (vEndDate <> vNewEndDate) then
  begin
    if not Assigned(vRouteObj) then
      vRouteObj := core.AddObjectToTicket(mgtDatasources[ 'Routes' ], vRouteMuid, oEdit);
    if (vNewEndDate > 0) then
      core.setFieldValue(vRouteObj.MainData, 'close_date', DateToMySqlStr(vNewEndDate))
    else
      core.setFieldValue(vRouteObj.MainData, 'close_date', '');
    flHasChanges := true;
  end;

  if flHasChanges then
  begin
    core.AddObjectLink(mgtDatasources[ 'Routes' ], vRouteMuid, mgtDatasources['RouteVariants'], aMuid, [tltCascade]);
    cardsManager.reloadFormByTicket(mgtDatasources[ 'Routes' ], vRouteMuid);
  end;
end;

{**********************************************************************************************
* onObjectDeleting
***********************************************************************************************}
procedure TMgtCore.onObjectDeleting(aDatasource: TMgtDatasource; aMuid: int64);
begin
  if aDatasource.Alias = 'Parks' then onParkDeleting(aMuid)
  else if aDatasource.Alias = 'Orders' then onOrderDeleting(aMuid)
  else if aDatasource.Alias = 'Stops' then onStopDeleting(aMuid)
  else if aDatasource.Alias = 'StopPlaces' then onStopPlaceDeleting(aMuid)
  else if aDatasource.Alias = 'StopPavilions' then onStopPavilionDeleting(aMuid)
  else if aDatasource.Alias = 'StopPavilionOrders' then onStopPavilionOrderDeleting(aMuid)
  else if aDatasource.Alias = 'Routes' then onRouteDeleting(aMuid)
  else if aDatasource.Alias = 'RouteVariants' then onRouteVariantDeleting(aMuid)
  else if aDatasource.Alias = 'RouteRounds' then onRouteRoundDeleting(aMuid)
  else if aDatasource.Alias = 'RouteNullRounds' then onRouteNullRoundDeleting(aMuid)
  else if aDatasource.Alias = 'RouteTrajectories' then onRouteTrajectoryDeleting(aMuid)
  else if aDatasource.Alias = 'SubwayStations' then onSubwayStationDeleting(aMuid)
  else if aDatasource.Alias = 'DisplayPanels' then onDisplayPanelDeleting(aMuid)
  else if aDatasource.Alias = 'DisplayPanelServiceContracts' then onDisplayPanelServiceContractDeleting(aMuid)
  else if aDatasource.Alias = 'WifiEquipment' then onWifiEquipmentDeleting(aMuid)
  else if aDatasource.Alias = 'VideoCameras' then onVideoCameraDeleting(aMuid)
  else if aDatasource.Alias = 'WifiEquipmentServiceContracts' then onWifiEquipmentServiceContractDeleting(aMuid)
  else

    exit;
  
  // ������������� �������� �����
  cardsManager.reloadOpenFormsByTicket();
end;

{**********************************************************************************************
* onParkDeleting
***********************************************************************************************}
procedure TMgtCore.onParkDeleting(aMuid: int64);
var
  i : integer;
  jsonObjectList: TlkJSONlist;
  jsonObj : TlkJSONobject;
  muid : int64;
  vObject: TtrsGISMGTObject;
  datasource: TMgtDatasource;
begin
  showPopupMessage('�������� ��� �����...');

  vObject := GetObjectFromTicket(mgtDatasources[ 'Parks' ], aMuid);

  datasource := mgtDatasources[ 'ParkZones' ];
  // ���� ���������
  jsonObjectList := core.getObjects(datasource, 'park_muid', IntToStr(aMuid), ['muid']);
  for i := 0 to jsonObjectList.count - 1 do
  begin
    jsonObj := jsonObjectList.asObject[i];
    muid := StrToInt64(jsonObj.asString['muid']);

    AddObjectToTicket(datasource, muid, oDelete);
  end;
  FreeAndNil(jsonObjectList);

  // ������� ����� � ������ ���������� �������
  deleteLnkObjects(vObject, mgtDatasources[ 'LnkParksVehicleTypes' ], 'park_muid', aMuid);
  // ������� ����� � �������������
  deleteLnkObjects(vObject, mgtDatasources[ 'LnkParksAgencies' ], 'park_muid', aMuid);
  hidePopupMessage ();
end;

{**********************************************************************************************
* onOrderDeleting
***********************************************************************************************}
procedure TMgtCore.onOrderDeleting(aMuid: int64);
begin
  showPopupMessage('�������� ����������...');
  onAttachmentsDeleting(aMuid, mgtDatasources[ 'AttachmentsOrders' ]);
  hidePopupMessage();
end;

{**********************************************************************************************
* onStopDeleting
***********************************************************************************************}
procedure TMgtCore.onStopDeleting(aMuid: int64);
var
  i : integer;
  jsonObjectList: TlkJSONlist;
  jsonObj : TlkJSONobject;
  muid : int64;
  datasource: TMgtDatasource;

begin
  showPopupMessage('�������� ���� �������-�������...');

  datasource := mgtDatasources[ 'StopPlaces' ];
  // �������� ����� ������� �������
  jsonObjectList := core.getObjects(datasource, 'stop_muid', IntToStr(aMuid), ['muid']);
  for i := 0 to jsonObjectList.count - 1 do
  begin
    jsonObj := jsonObjectList.asObject[i];
    muid := StrToInt64(jsonObj.asString['muid']);

    // ������� ������ ����� �������-�������
    AddObjectToTicket(datasource, muid, oDelete);
  end;
  FreeAndNil(jsonObjectList);
  hidePopupMessage ();
end;

{**********************************************************************************************
* onStopPlaceDeleting
***********************************************************************************************}
procedure TMgtCore.onStopPlaceDeleting(aMuid: int64);
var
  i : integer;
  jsonObjectList: TlkJSONlist;
  jsonObj : TlkJSONobject;
  muid, vStopMuid : int64;
  datasource: TMgtDatasource;

begin
  showPopupMessage('�������� ���...');

  vStopMuid := StrToInt64(core.getObjectValue(mgtDatasources[ 'StopPlaces' ], aMuid, 'stop_muid', '-1'));
  AddStopPlaceTrajectoryObjectLinks(aMuid);

  DeleteStopIfNeeded(vStopMuid);

  datasource := mgtDatasources[ 'StopZones' ];
  // ���� ���������
  jsonObjectList := core.getObjects(datasource, 'stop_place_muid', IntToStr(aMuid), ['muid']);
  for i := 0 to jsonObjectList.count - 1 do
  begin
    jsonObj := jsonObjectList.asObject[i];
    muid := StrToInt64(jsonObj.asString['muid']);

    AddObjectToTicketInternal(datasource, muid, oDelete);
  end;
  FreeAndNil(jsonObjectList);

  datasource := mgtDatasources[ 'TerminalPointZones' ];
  // ���� ��
  jsonObjectList := core.getObjects(datasource, 'stop_place_muid', IntToStr(aMuid), ['muid']);
  for i := 0 to jsonObjectList.count - 1 do
  begin
    jsonObj := jsonObjectList.asObject[i];
    muid := StrToInt64(jsonObj.asString['muid']);

    AddObjectToTicketInternal(datasource, muid, oDelete);
  end;
  FreeAndNil(jsonObjectList);
  hidePopupMessage ();
end;

{**********************************************************************************************
* onStopPavilionDeleting
***********************************************************************************************}
procedure TMgtCore.onStopPavilionDeleting(aMuid: int64);
var
  i: integer;
  jsonObjectList: TlkJSONlist;
  jsonObj: TlkJSONobject;
  muid: int64;
  datasource: TMgtDatasource;

begin
  showPopupMessage('�������� ���������...');
  datasource := mgtDatasources[ 'StopPavilionInstallations' ];
  jsonObjectList := core.getObjects(datasource, 'stop_pavilion_muid', IntToStr(aMuid), ['muid']);
  for i := 0 to jsonObjectList.count - 1 do
  begin
    jsonObj := jsonObjectList.asObject[i];
    muid := StrToInt64(jsonObj.asString['muid']);

    AddObjectToTicketInternal(datasource, muid, oDelete);
  end;
  FreeAndNil(jsonObjectList);
 // hidePopupMessage();

  showPopupMessage('�������� ����������...');
  onAttachmentsDeleting(aMuid, mgtDatasources[ 'AttachmentsStopPavilions' ]);
  hidePopupMessage();
end;

{**********************************************************************************************
* onStopPavilionOrderDeleting
***********************************************************************************************}
procedure TMgtCore.onStopPavilionOrderDeleting(aMuid: int64);
begin
  showPopupMessage('�������� ����������...');
  onAttachmentsDeleting(aMuid,mgtDatasources[ 'AttachmentsStopPavilionOrders' ]);
  hidePopupMessage();
end;

{**********************************************************************************************
* onRouteDeleting
***********************************************************************************************}
procedure TMgtCore.onRouteDeleting(aMuid: int64);
var
  i : integer;
  jsonObjectList: TlkJSONlist;
  jsonObj : TlkJSONobject;
  muid : int64;
  vObject: TtrsGISMGTObject;
  datasource: TMgtDatasource;

begin
  showPopupMessage('�������� ���������...');

  vObject :=  GetObjectFromTicket(mgtDatasources[ 'Routes' ], aMuid);

  datasource := mgtDatasources[ 'RouteVariants' ];
  // �������� �������� �������� ��������
  jsonObjectList := core.getObjects(datasource, 'route_muid', IntToStr(aMuid), ['muid']);
  for i := 0 to jsonObjectList.count - 1 do
  begin
    jsonObj := jsonObjectList.asObject[i];
    muid := StrToInt64(jsonObj.asString['muid']);

    // ������� ������ �������
    AddObjectToTicket(datasource, muid, oDelete);
  end;
  FreeAndNil(jsonObjectList);

  // ������� ����� � �������
  deleteLnkObjects(vObject, mgtDatasources[ 'LnkParksRoutes' ], 'route_muid', aMuid);

  hidePopupMessage ();
end;

{**********************************************************************************************
* onRouteVariantDeleting
***********************************************************************************************}
procedure TMgtCore.onRouteVariantDeleting(aMuid: int64);
var
  i : integer;
  jsonObjectList: TlkJSONlist;
  jsonObj : TlkJSONobject;
  muid : int64;
  datasource: TMgtDatasource;

begin
  showPopupMessage('�������� ������, ����������...');
  updateRouteByVariantDates(aMuid);

  datasource := mgtDatasources[ 'RouteRounds' ];
  // �������� ����� �������� ��������
  jsonObjectList := core.getObjects(datasource, 'route_variant_muid', IntToStr(aMuid), ['muid']);
  for i := 0 to jsonObjectList.count - 1 do
  begin
    jsonObj := jsonObjectList.asObject[i];
    muid := StrToInt64(jsonObj.asString['muid']);

    // ������� ������ ����
    AddObjectToTicket(datasource, muid, oDelete);
  end;
  FreeAndNil(jsonObjectList);

  datasource := mgtDatasources[ 'RouteNullRounds' ];
  // �������� ������� ����� �������� ��������
  jsonObjectList := core.getObjects(datasource, 'route_variant_muid', IntToStr(aMuid), ['muid']);
  for i := 0 to jsonObjectList.count - 1 do
  begin
    jsonObj := jsonObjectList.asObject[i];
    muid := StrToInt64(jsonObj.asString['muid']);

    // ������� ������ ����
    AddObjectToTicket(datasource, muid, oDelete);
  end;
  FreeAndNil(jsonObjectList);
  hidePopupMessage();
end;

{**********************************************************************************************
* onRouteRoundDeleting
***********************************************************************************************}
procedure TMgtCore.onRouteRoundDeleting(aMuid: int64);
var
  i : integer;
  jsonObjectList: TlkJSONlist;
  jsonObj : TlkJSONobject;
  muid : int64;
  datasource: TMgtDatasource;

begin
  showPopupMessage('�������� ����������...');

  datasource := mgtDatasources[ 'RouteTrajectories' ];
  // �������� ���������� �������� �����
  jsonObjectList := core.getObjects(datasource, 'route_round_muid', IntToStr(aMuid), ['muid']);
  for i := 0 to jsonObjectList.count - 1 do
  begin
    jsonObj := jsonObjectList.asObject[i];
    muid := StrToInt64(jsonObj.asString['muid']);

    // ������� ������ ����������
    AddObjectToTicket(datasource, muid, oDelete);
  end;
  FreeAndNil(jsonObjectList);
  hidePopupMessage ();
end;

{**********************************************************************************************
* onRouteNullRoundDeleting
***********************************************************************************************}
procedure TMgtCore.onRouteNullRoundDeleting(aMuid: int64);
var
  i : integer;
  jsonObjectList: TlkJSONlist;
  jsonObj : TlkJSONobject;
  muid : int64;
  datasource: TMgtDatasource;

begin
  showPopupMessage('�������� ����������...');
  datasource := mgtDatasources[ 'RouteTrajectories' ];
  // �������� ���������� �������� �����
  jsonObjectList := core.getObjects(datasource, 'route_null_round_muid', IntToStr(aMuid), ['muid']);
  for i := 0 to jsonObjectList.count - 1 do
  begin
    jsonObj := jsonObjectList.asObject[i];
    muid := StrToInt64(jsonObj.asString['muid']);

    // ������� ������ ����������
    AddObjectToTicket(datasource, muid, oDelete);
  end;
  FreeAndNil(jsonObjectList);
  hidePopupMessage ();
end;

{**********************************************************************************************
* onRouteTrajectoryDeleting
***********************************************************************************************}
procedure TMgtCore.onRouteTrajectoryDeleting(aMuid: int64);
var
  vObject: TtrsGISMGTObject;
begin
  showPopupMessage('�������� ����������...');

  vObject := GetObjectFromTicket(mgtDatasources[ 'RouteTrajectories' ], aMuid);

  // ������� ����� � �����������
  deleteLnkObjects(vObject, mgtDatasources[ 'LnkRouteTrajectoriesStopPlaces' ], 'route_trajectory_muid', aMuid);
  // ������� ����� � �����������
  deleteLnkObjects(vObject, mgtDatasources[ 'LnkRouteTrajectoriesCheckPoints' ], 'route_trajectory_muid', aMuid);
  // ������� ����� � ������
  deleteLnkObjects(vObject, mgtDatasources[ 'LnkRouteTrajectoriesGraphSections' ], 'route_trajectory_muid', aMuid);
  // ������� ����� � ��������
  deleteLnkObjects(vObject, mgtDatasources[ 'LnkRouteTrajectoriesRegions' ], 'route_trajectory_muid', aMuid);
  // ������� ����� � �������
  deleteLnkObjects(vObject, mgtDatasources[ 'LnkRouteTrajectoriesStreets' ], 'route_trajectory_muid', aMuid);
   // ������� ����� � ���������������� �������
  deleteLnkObjects(vObject, mgtDatasources[ 'LnkRouteTrajectoriesCompensatoryPoints' ], 'route_trajectory_muid', aMuid);

  hidePopupMessage ();
end;

{*******************************************************************************
* onSubwayStationDeleting
*******************************************************************************}
procedure TMgtCore.onSubwayStationDeleting(aMuid: int64);
var
  vObject: TtrsGISMGTObject;
  datasource: TMgtDatasource;
  jsonObjectList: TlkJSONlist;
  jsonObj : TlkJSONobject;
  muid : int64;
  i : integer;
  
begin
  showPopupMessage('�������� �������...');
  vObject := GetObjectFromTicket(mgtDatasources[ 'SubwayStations' ], aMuid);

  datasource := mgtDatasources[ 'SubwayStationEntrances' ];
  // �������� �������� �������� ��������
  jsonObjectList := core.getObjects(datasource, 'subway_station_muid', IntToStr(aMuid), ['muid']);
  try
    for i := 0 to jsonObjectList.count - 1 do
    begin
      jsonObj := jsonObjectList.asObject[i];
      muid := StrToInt64(jsonObj.asString['muid']);

      // ������� ������ �������
      AddObjectToTicket(datasource, muid, oDelete);
    end;
  finally
    FreeAndNil(jsonObjectList);
  end;

  hidePopupMessage ();
end;

{*******************************************************************************
* onDisplayPanelDeleting
*******************************************************************************}
procedure TMgtCore.onDisplayPanelDeleting(aMuid: int64);
begin
  showPopupMessage('�������� ����������...');
  onAttachmentsDeleting(aMuid, mgtDatasources[ 'AttachmentsDisplayPanelPhotos' ]);
  hidePopupMessage();
end;

{*******************************************************************************
* onDisplayPanelServiceContractDeleting
*******************************************************************************}
procedure TMgtCore.onDisplayPanelServiceContractDeleting(aMuid: int64);
begin
  showPopupMessage('�������� ����������...');
  onAttachmentsDeleting(aMuid, mgtDatasources[ 'AttachmentsDisplayPanelServiceContracts' ]);
  hidePopupMessage();
end;

{*******************************************************************************
* onWifiEquipmentDeleting
*******************************************************************************}
procedure TMgtCore.onWifiEquipmentDeleting(aMuid: int64);
begin
  showPopupMessage('�������� ����������...');
  onAttachmentsDeleting(aMuid, mgtDatasources[ 'AttachmentsWifiEquipmentPhotos' ]);
  hidePopupMessage();
end;

{*******************************************************************************
* onVideoCameraDeleting
*******************************************************************************}
procedure TMgtCore.onVideoCameraDeleting(aMuid: int64);
begin
  showPopupMessage('�������� ����������...');
  onAttachmentsDeleting(aMuid, mgtDatasources[ 'AttachmentsVideoCamerasPhotos' ]);
  hidePopupMessage();
end;

{*******************************************************************************
* onWifiEquipmentServiceContractDeleting
*******************************************************************************}
procedure TMgtCore.onWifiEquipmentServiceContractDeleting(aMuid: int64);
begin
  showPopupMessage('�������� ����������...');
  onAttachmentsDeleting(aMuid, mgtDatasources[ 'AttachmentsWifiEquipmentServiceContracts' ]);
  hidePopupMessage();
end;

{*******************************************************************************
* onAttachmentsDeleting
*******************************************************************************}
procedure TMgtCore.onAttachmentsDeleting(aMuid: int64; aDatasource: TMgtDatasource);
var
  vAttachmentManager : TMgtAttachmentManager;

begin
  vAttachmentManager := TMgtAttachmentManager.Create(aMuid, aDatasource);
  try
    vAttachmentManager.Load();
    vAttachmentManager.RemoveAllFiles();
  finally
    FreeAndNil(vAttachmentManager);
  end;
end;

{**********************************************************************************************
* deletePark
***********************************************************************************************}
procedure TMgtCore.deletePark(aMuid: int64);
begin
  showPopupMessage('�������� ��� �����...');
  AddObjectToTicketInternal(mgtDatasources[ 'Parks' ], aMuid, oDelete);
  hidePopupMessage();
end;

{**********************************************************************************************
* deleteRoute
// ������� ������� (�������� � ����� �� ��������) � ���������� ����������
***********************************************************************************************}
procedure TMgtCore.deleteRoute(aMuid: int64);
begin
  showPopupMessage('�������� ���������...');
  AddObjectToTicketInternal(mgtDatasources[ 'Routes' ], aMuid, oDelete);
  hidePopupMessage();
end;

{**********************************************************************************************
* deleteRouteVariant
// ������� ������� (�������� � ����� �� ��������) � ���������� ����������
***********************************************************************************************}
procedure TMgtCore.deleteRouteVariant(aMuid: int64);
begin
  showPopupMessage('�������� ������, ����������...');
  // �������� ������, ����������
  AddObjectToTicketInternal(mgtDatasources[ 'RouteVariants' ], aMuid, oDelete);
  hidePopupMessage();
end;

{**********************************************************************************************
* deleteRouteRound
// ������� ���� (�������� � ����� �� ��������) � ���������� ����������
***********************************************************************************************}
procedure TMgtCore.deleteRouteRound(aMuid: int64);
begin
  showPopupMessage('�������� ����������...');
  AddObjectToTicketInternal(mgtDatasources[ 'RouteRounds' ], aMuid, oDelete);
  hidePopupMessage();
end;

{**********************************************************************************************
* deleteRouteNullRound
// ������� ������� ���� (�������� � ����� �� ��������) � ���������� ����������
***********************************************************************************************}
procedure TMgtCore.deleteRouteNullRound(aMuid: int64);
begin
  showPopupMessage('�������� ����������...');
  AddObjectToTicketInternal(mgtDatasources[ 'RouteNullRounds' ], aMuid, oDelete);
  hidePopupMessage();
end;

{**********************************************************************************************
* deleteRouteTrajectory
// ������� ���������� (�������� � ����� �� ��������) � ���������� ����������
***********************************************************************************************}
procedure TMgtCore.deleteRouteTrajectory(aMuid: int64);
begin
  showPopupMessage('�������� ����������...');
  AddObjectToTicketInternal(mgtDatasources[ 'RouteTrajectories' ], aMuid, oDelete);
  hidePopupMessage();
end;

{**********************************************************************************************
* deleteLnkObjects
// ������� ����� (�������� � ����� �� ��������) ��� ���������� ���������, ��� ���������� ������������� �������
// �������� ��� slave data ����������� �������
***********************************************************************************************}
procedure TMgtCore.deleteLnkObjects(aObject: TtrsGISMGTObject; aDataSource: TMgtDatasource; aParentField: string; aParentMuid: int64);
var
  i : integer;
  jsonObjectList: TlkJSONlist;
  jsonObj : TlkJSONobject;
  muid : int64;
begin
  jsonObjectList := core.getObjects(aDataSource, aParentField, IntToStr(aParentMuid), ['muid']);
  for i := 0 to jsonObjectList.count - 1 do
  begin
    jsonObj := jsonObjectList.asObject[i];
    muid := StrToInt64(jsonObj.asString['muid']);

    AddSlaveDataToTicket(aObject, aDataSource, muid, oDelete, nil);
  end;
  FreeAndNil(jsonObjectList);
end;

{**********************************************************************************************
* deleteStop
// ������� ������������ ����� (�������� � ����� �� ��������) � ���������� ����������
***********************************************************************************************}
procedure TMgtCore.deleteStop(aMuid: int64);
begin
  showPopupMessage('�������� ���� �������-�������...');
  AddObjectToTicketInternal(mgtDatasources[ 'Stops' ], aMuid, oDelete);
  hidePopupMessage();
end;

{**********************************************************************************************
* deleteStopPlace
// ������� ����� �������-������� (�������� � ����� �� ��������) � ���������� ����������
***********************************************************************************************}
procedure TMgtCore.deleteStopPlace(aMuid: int64);
begin
  showPopupMessage('�������� ���...');
  AddObjectToTicketInternal(mgtDatasources[ 'StopPlaces' ], aMuid, oDelete);
  hidePopupMessage();
end;

{**********************************************************************************************
* deleteStopPavilion
// ������� �������� �������� (�������� � ����� �� ��������) � ���������� ����������
***********************************************************************************************}
procedure TMgtCore.deleteStopPavilion(aMuid: int64);
begin
  showPopupMessage('�������� ���������...');
  AddObjectToTicketInternal(mgtDatasources[ 'StopPavilions' ], aMuid, oDelete);
  hidePopupMessage();
end;

{**********************************************************************************************
* onBeforeObjectDeleteFromTicket
***********************************************************************************************}
procedure TMgtCore.onBeforeObjectDeleteFromTicket(aDatasource: TMgtDatasource; aMuid: int64);
begin
  if aDatasource.Alias = 'StopPlaces' then
    onBeforeStopPlaceDeleteFromTicket(aMuid)
  else if aDatasource.Alias = 'RouteTrajectories' then
    onBeforeRouteTrajectoryDeleteFromTicket(aMuid);
end;

{**********************************************************************************************
* onAfterObjectDeleteFromTicket
// ������������� �������� ������� �� �������
***********************************************************************************************}
procedure TMgtCore.onAfterObjectDeleteFromTicket(aDatasource: TMgtDatasource; aMuid: int64);
begin

end;

{**********************************************************************************************
* onBeforeStopPlaceDeleteFromTicket
***********************************************************************************************}
procedure TMgtCore.onBeforeStopPlaceDeleteFromTicket(aMuid: int64);
var
  vStopPlaceObj, vInitStopObj: TtrsGISMGTObject;
  vStopMuid, vInitStopMuid: int64;
  vStop: TlkJSONlist;
begin
  vStopPlaceObj := GetObjectFromTicket(mgtDatasources[ 'StopPlaces' ], aMuid);
  vStopMuid := StrToInt64Def(getObjectValue(mgtDatasources[ 'StopPlaces' ], aMuid, 'stop_muid', '-1'), -1);
  vInitStopMuid := StrToInt64Def(vStopPlaceObj.MainData.GetInitialValue('stop_muid'), -1);
  // ����� ������ ������ ��� �� �������������� ��
  setFieldValue(vStopPlaceObj.MainData, 'stop_muid', vInitStopMuid);

  // ���� �������� ���� ���������� ��� ��������������, ��������� ���� �� ������� ��
  if vStopPlaceObj.Operation <> oDelete then
    DeleteStopIfNeeded(vStopMuid);

  // �� ������ �������������� ����� � �� -> ��� �����������
  if vInitStopMuid <= 0 then
    exit;

  vInitStopObj := GetObjectFromTicket(mgtDatasources[ 'Stops' ], vInitStopMuid);
  // �� ��� � ������� -> ������ ������ �� ����.
  if not Assigned(vInitStopObj) then
    Exit;

  // �������� ������ ����� ��� �� ����������
  vStop := getObjects(mgtDatasources[ 'Stops' ], 'muid', IntToStr(vStopMuid), ['muid']);
  if vStop.Count > 0 then
    if (vStopPlaceObj.Operation = oEdit) and (vInitStopMuid <> vStopMuid) then
      DeleteStopIfNeeded(vStopMuid);
  FreeAndNil(vStop);

  // ��������� ��� �������� ������� � ���������� ���������
  if (vInitStopObj.Operation = oDelete) or (not checkStopPlaceInsideStop(aMuid, vInitStopMuid)) then
    deleteStopFromTicket(vInitStopMuid);
end;

{**********************************************************************************************
* onBeforeRouteTrajectoryDeleteFromTicket
***********************************************************************************************}
procedure TMgtCore.onBeforeRouteTrajectoryDeleteFromTicket(aMuid: int64);
var
  vObj: TtrsGISMGTObject;
begin
  vObj := GetObjectFromTicket(mgtDatasources[ 'RouteTrajectories' ], aMuid);
  if Assigned(vObj) then
    if vObj.Operation = oAdd then
      FMapTrajectories.Delete(IntToStr(aMuid));
end;

{**********************************************************************************************
* deleteStopFromTicket
***********************************************************************************************}
procedure TMgtCore.deleteStopFromTicket(aMuid: int64);
var
  vStopPlaceList: TlkJSONlist;
  vStopPlaceMuid: int64;
  vStopPlaceObj, vStopObj: TtrsGISMGTObject;
  i: integer;
  datasource: TMgtDatasource;

begin
  datasource := mgtDatasources[ 'StopPlaces' ];
  // �������� ������ ���� ����������� � �� ���, ������� ���������
  vStopPlaceList := getObjects(datasource, 'stop_muid', IntToStr(aMuid), ['muid'], true);

  vStopObj := GetObjectFromTicket(mgtDatasources[ 'Stops' ], aMuid);
  if vStopObj.Operation <> oDelete then
    // ������� ��� �����
    for i := 0 to vStopPlaceList.Count - 1 do
    begin
      vStopPlaceMuid := StrToInt64(vStopPlaceList.asObject[i].asString['muid']);
      DeleteObjectLink(datasource, vStopPlaceMuid, mgtDatasources[ 'Stops' ], aMuid);
    end;

  // ����� ����� ����� ������� �� ������ ��������, �������� ����
  DeleteObjectFromTicket(mgtDatasources[ 'Stops' ], aMuid);

  // ��������� ����� � ���
  for i := 0 to vStopPlaceList.Count - 1 do
  begin
    vStopPlaceMuid := StrToInt64(vStopPlaceList.asObject[i].asString['muid']);
    vStopPlaceObj := GetObjectFromTicket(datasource, vStopPlaceMuid);
    // ���� ������� ��� � ������� ��� �� ���������, ������ �� ������
    if (not Assigned(vStopPlaceObj)) or (vStopPlaceObj.Operation = oDelete) then
      Continue;

    // ��� �� ������ ��, ������� ��� �� �������
    if not checkStopPlaceInsideStop(vStopPlaceMuid, aMuid) then
      DeleteObjectFromTicket(datasource, vStopPlaceMuid);
  end;

  FreeAndNil(vStopPlaceList);
end;

{**********************************************************************************************
* checkStopPlaceInsideStop
***********************************************************************************************}
function TMgtCore.checkStopPlaceInsideStop(aStopPlaceMuid, aStopMuid: int64): boolean;
var
  vStopMOS, vStopPlaceMOS: TMapObjectStructure;
begin
  Result := False;

  vStopMOS := TMapObjectStructure.Create();
  vStopPlaceMOS := TMapObjectStructure.Create();

  try
    if not mapCore.GetMapObject(mgtDatasources[ 'Stops' ], aStopMuid, vStopMOS) then
      exit;

    if not mapCore.GetMapObject(mgtDatasources[ 'StopPlaces' ], aStopPlaceMuid, vStopPlaceMOS) then
      exit;

    Result := vStopMOS.CheckPointInContour(-1, vStopPlaceMOS.FirstVertex[0].X, vStopPlaceMOS.FirstVertex[0].Y, 1) >= 0;
  finally
    FreeAndNil(vStopMOS);
    FreeAndNil(vStopPlaceMOS);
  end;
end;

{**********************************************************************************************
* CreateStopZone
***********************************************************************************************}
procedure TMgtCore.CreateStopZone(aStopPlaceMuid: int64);
var
  jsonObjectList : TlkJSONlist;
  vMuid: int64;
  vStopPlaceMos, vZoneMos: TMapObjectStructure;
  vZoneObject: TtrsObject;
  vGeom: string;
begin
  jsonObjectList := core.getObjects( mgtDatasources[ 'StopZones' ], 'stop_place_muid', IntToStr(aStopPlaceMuid), ['muid']);

  // ���� ��� ����, ������ �� ������
  if jsonObjectList.Count > 0 then
  begin
    jsonObjectList.Free();
    exit;
  end;

  vStopPlaceMos := TMapObjectStructure.Create();
  if not mapCore.GetMapObject( mgtDatasources[ 'StopPlaces' ], aStopPlaceMuid, vStopPlaceMos) then
  begin
    FreeAndNil(vStopPlaceMos);
    exit;
  end;

  vZoneMos := TMapObjectStructure.Create();
  vZoneMos.lCode := mgtDatasources[ 'StopZones' ].layerCode;
  vZoneMos.oType := POLYGON_TYPE;
  vZoneMos.oStyleID := mgtDatasources['StopZones'].MapStyleID;

  vMuid := GenerateMuid();
  vZoneMos.oMUID := vMuid;

  vZoneObject := AddObjectToTicket( mgtDatasources['StopZones'], vMuid, oAdd);

  mapCore.MakeHexagonBuffer(vZoneMos, vStopPlaceMos.FirstVertex[0], 100000);
  vGeom := mapCore.GetGeometryAsBase64String(vZoneMos, vZoneMos.lCode);

  setFieldValue(vZoneObject.MainData, MOS_TAG, vGeom);
  setFieldValue(vZoneObject.MainData, 'stop_place_muid', aStopPlaceMuid);
  AddObjectLinks( mgtDatasources['StopZones'], vMuid);
  mapCore.TicketsManager.AddBatchEditObjectByMOS(vZoneMos);

  FreeAndNil(vZoneMos);
  FreeAndNil(vStopPlaceMos);
end;

{**********************************************************************************************
* CreateStop
***********************************************************************************************}
function TMgtCore.CreateStop(aStopPlaceMuid: int64): int64;
var
  vStopPlaceMos, vStopMos: TMapObjectStructure;
  vStopObject: TtrsObject;
  vStrDirList: TMapIntegers;
  vStopPlaces: TMapInt64;
  vGeom, vRes, vName: string;
  vMuid: int64;
begin
  Result := -1;

  vStopPlaceMos := TMapObjectStructure.Create();
  // �������� ������� ����� �������-�������
  if not mapCore.GetMapObject( mgtDatasources[ 'StopPlaces' ], aStopPlaceMuid, vStopPlaceMos) then
  begin
    FreeAndNil(vStopPlaceMos);
    exit;
  end;

  vStopMos := TMapObjectStructure.Create();
  Result := GenerateMuid();
  vStopObject := AddObjectToTicket(mgtDatasources[ 'Stops' ], Result, oAdd);

  // ��������� �������
  vStopPlaces := TMapInt64.Create();
  vStopPlaces.addItem(IntToStr(aStopPlaceMuid) , -1);
  if not mapCore.CreateStopGraphics(vStopMos, vStopPlaces) then
    mapCore.MakeSquareBuffer(vStopMos, vStopPlaceMos.FirstVertex[0], 5000);

  vStopMos.lCode := mgtDatasources[ 'Stops' ].layerCode;
  vStopMos.oType := POLYGON_TYPE;
  vStopMos.oStyleID := mgtDatasources[ 'Stops' ].MapStyleID;
  vStopMos.oMUID := Result;
  vGeom := mapCore.GetGeometryAsBase64String(vStopMos, vStopMos.lCode);

  // ����� � ������ �������
  setFieldValue(vStopObject.MainData, MOS_TAG, vGeom);

  // ����� ������������
  // ��������� ��� ��� �� �������� ������ ���, ��� ������� �� ����������� �����
  vName := getStopPlaceName(aStopPlaceMuid);
  setFieldValue(vStopObject.MainData, 'name', vName);
  // �� �������� ��� ����������� ��������
  setFieldValue(vStopObject.MainData, 'signpost_caption', SignpostCaption(vName, true));

  vName := getStopPlaceName(aStopPlaceMuid, true);
  setFieldValue(vStopObject.MainData, 'name_for_terminal_point', vName);

  setFieldValue(vStopObject.MainData, 'signpost_narrow_name', SignpostNarrowName(vName, true));
  setFieldValue(vStopObject.MainData, 'signpost_wide_name', SignpostWideName(vName, true));

  // ����� � �� � ��
  vMuid := mapCore.GetRegionContourUid(vStopPlaceMos.FirstVertex[0], nil);
  vRes := '';
  if (vMuid > 0) and (core.getObjectValueFromDB(mgtDatasources[ 'EgkoRegions' ], vMuid, 'region_muid', vRes)) then
    setFieldValue(vStopObject.MainData, 'region_muid', vRes);

  vMuid := StrToInt64Def(vRes, -1);
  if (vMuid > 0) and (core.getObjectValueFromDB(mgtDatasources[ 'OmkRegions' ], vMuid, 'district_muid', vRes)) then
    setFieldValue(vStopObject.MainData, 'district_muid', vRes);

  vMuid := mapCore.GetTDContourUid(vStopPlaceMos.FirstVertex[0], nil);
  vRes := '';
  if (vMuid > 0) and (core.getObjectValueFromDB(mgtDatasources[ 'TDO' ], vMuid, 'muid', vRes)) then
    setFieldValue(vStopObject.MainData, 'tdo_muid', vRes);

  vStrDirList := TMapIntegers.Create(mcAdd);
  formStrDirListBySPCoords(vStopPlaceMos.FirstVertex[0], vStrDirList);
  if vStrDirList.Count > 0 then
  begin
    setFieldValue(vStopObject.MainData, 'street_muid', vStrDirList.keys[0]);
    setFieldValue(vStopObject.MainData, 'direction_muid', vStrDirList.items[0]);
  end;

  mapCore.TicketsManager.AddBatchEditObjectByMOS(vStopMos);

  FreeAndNil(vStopPlaces);
  FreeAndNil(vStrDirList);
  FreeAndNil(vStopMos);
  FreeAndNil(vStopPlaceMos);
end;

{**********************************************************************************************
* CreateStopGraphics
***********************************************************************************************}
function TMgtCore.CreateStopGraphics(aStopMuid: int64; vStopGraphics: TMapObjectStructure): boolean;
var
  vStopPlaces: TMapInt64;
  vStopPlacesList: TlkJSONlist;
  i: integer;
  vStopPlaceMuid: int64;
  vStopPlaceMOS: TMapObjectStructure;
begin
  Result := false;

  if not Assigned(vStopGraphics) then
    exit;

  vStopPlaceMOS := TMapObjectStructure.Create();
  vStopPlaces := TMapInt64.Create();
  vStopPlacesList := getObjects( mgtDatasources[ 'StopPlaces' ], 'stop_muid', IntToStr(aStopMuid), ['muid']);
  for i := 0 to vStopPlacesList.Count - 1 do
    vStopPlaces.addItem(vStopPlacesList.asObject[i].asString['muid'], -1);

  Result := mapCore.CreateStopGraphics(vStopGraphics, vStopPlaces);
  vStopGraphics.oMUID := aStopMuid;
  vStopGraphics.lCode := mgtDatasources[ 'Stops' ].layerCode;
  vStopGraphics.oType := POLYGON_TYPE;
  vStopGraphics.oStyleID := mgtDatasources['Stops'].MapStyleID;

  // ��������� ��� ��� ��� ������ ��
  if Result then
    for i := 0 to vStopPlacesList.Count - 1 do
    begin
      vStopPlaceMuid := StrToInt64(vStopPlacesList.asObject[i].asString['muid']);
      Result := mapCore.GetMapObject(mgtDatasources[ 'StopPlaces' ], vStopPlaceMuid, vStopPlaceMOS);
      if not Result then
        break;

      Result := vStopGraphics.CheckPointInContour(-1, vStopPlaceMOS.FirstVertex[0].X, vStopPlaceMOS.FirstVertex[0].Y, 1) >= 0;

      if not Result then
        break;
    end;

  FreeAndNil(vStopPlaceMOS);
  FreeAndNil(vStopPlaces);
  FreeAndNil(vStopPlacesList);
end;


{**********************************************************************************************
* DeleteStopIfNeeded
***********************************************************************************************}
procedure TMgtCore.DeleteStopIfNeeded(aStopMuid: int64);
var
  jsonObjectList, vStop : TlkJSONlist;
begin
  if aStopMuid <= 0 then
    exit;

  // ���� ��� ����������� � �� ����� �������-�������
  jsonObjectList := core.getObjects(mgtDatasources[ 'StopPlaces' ], 'stop_muid', IntToStr(aStopMuid), ['muid']);
  try
    if jsonObjectList.Count = 0 then
    begin
      vStop := getObjects(mgtDatasources[ 'Stops' ], 'muid', IntToStr(aStopMuid), ['muid']);
      if vStop.Count > 0 then // ���� ������ �� ��� ���, �� ������� ��� ������
        deleteStop(aStopMuid);
      FreeAndNil(vStop);
    end;

  finally
    FreeAndNil(jsonObjectList);
  end;
end;

{**********************************************************************************************
* RebuildTrajectoriesByStopPlace
***********************************************************************************************}
procedure TMgtCore.RebuildTrajectoriesByStopPlace(aStopPlaceMuid: int64; aRebuildSet : EMgtRouteTransportKindsSet);
var
  vListTraj: TlkJSONlist;
  vTraj: TlkJSONobject;
  vTrajMuid, vLastMuid: int64;
  vTrajectory: TMgtRouteTrajectory;
  i, j, ErrCnt, WrnCnt: integer;
  oldLength, newLength, lengthDiff: double;
  vRes: boolean;
  vTicketObject: TtrsGISMGTObject;
  vStopPlaces: TMapInt64;
  ErrWrnMessage: string;
  datasource: TMgtDatasource;
begin
  vListTraj := getObjects(mgtDatasources[ 'LnkRouteTrajectoriesStopPlaces' ], 'stop_place_muid', IntToStr(aStopPlaceMuid),
                                                                                     ['muid', 'route_trajectory_muid']);
  //������ ���������� �����
  if not Assigned(vListTraj) then
    raise EMgtException.Create('������ ��� ��������� ������ ���������� ����������!');

  vStopPlaces := TMapInt64.Create();
  vStopPlaces.Add(IntToStr(aStopPlaceMuid));

  ErrCnt := 0;
  WrnCnt := 0;
  try
    try
      vLastMuid := -1;
	  datasource := mgtDatasources[ 'RouteTrajectories' ];
      for i := 0 to vListTraj.Count - 1 do
      begin
        core.showPopupMessage('���������� ���������� ' + IntToStr(i + 1) + ' �� ' + IntToStr(vListTraj.Count));
        vTraj := vListTraj.asObject[i];
        vTrajMuid := StrToInt64(vTraj.asString['route_trajectory_muid']);

        // ���� ���� ������ ��� ���������� ������� ����������������� ��� ��������� ���������� - �� ��������������� ������ ��.
        if (aRebuildSet <> []) and (not (GetTrajectoryTransportKind(vTrajMuid) in aRebuildSet)) then
          continue;

        // ��� ���������� ��� ������������, ��������
        if vLastMuid = vTrajMuid then
          continue;

        vLastMuid := vTrajMuid;

        // ��������� ���������� � ������
        vTicketObject := AddObjectToTicket(datasource, vTrajMuid, oEdit);

        // ��������� � ���������� ������ ����������
        vTrajectory := AddTrajectory(vTrajMuid);

        // ��������������� ����������
        vRes := vTrajectory.RebuildTrajectoryByStopPlaces(vStopPlaces);

        // ��������� ��������� � ������
        vTrajectory.SaveToTicket();

        // ��������� ��������
        cardsManager.refreshTrajectoryForm(vTrajectory.Muid);

        if not vRes then
        begin
          Inc(ErrCnt);
          vTrajectory.Correct := false;
        end;
        // ����������� ����� � 3�� ����� ����� �������
        // ���� ����� ���������� ����� ��� �� 10 ������, ������ ������������ ������
        oldLength := UniStrToFloatDef(vTicketObject.MainData.GetInitialValue('length'), 0);
        newLength := UniStrToFloatDef(vTicketObject.MainData.GetActualValue('length'), 0);
        lengthDiff := Abs(Round(oldLength * 1000) - Round(newLength * 1000));
        if lengthDiff > TRAJ_MIN_VALUE_DIFF then
        begin
          vTrajectory.setStatus(mtosWarning);
          Inc (WrnCnt);
        end;
      end;
    finally
      FreeAndNil(vListTraj);
    end;

    // ������ ���� ����������, ������� ����� ��� ���������, �� � ������ ���� ������ ��� ��� (�� ������ apply �� �������� ����������)
    try
      for i := 0 to FMapTrajectories.Count - 1 do
      begin
        vTrajectory := TMgtRouteTrajectory(FMapTrajectories.items[i]);
        // ���������� ����������������� � ��������, �� � ��� ����������
        if vTrajectory.Saved then
          Continue;

        // � ��������� ������ ��������� ����� �� ��������������� ��� ����������
        for j := 0 to vTrajectory.ControlPoints.Count - 1 do
        begin
          if vTrajectory.ControlPoints.ItemStopPlaceMuid[j] = aStopPlaceMuid then
          begin
            // ��� ������
            core.showPopupMessage('���������� ������������ ����������');
            // ��������� ����
            AddStopPlaceTrajectoryObjectLink(aStopPlaceMuid, vTrajectory.Muid);

            // ��������������� ����������
            vRes := vTrajectory.RebuildTrajectoryByStopPlaces(vStopPlaces);

            // ��������� ��������� � ������
            vTicketObject := vTrajectory.SaveToTicket();
            // ��������� ��������
            cardsManager.refreshTrajectoryForm(vTrajectory.Muid);

            if not vRes then
            begin
              Inc(ErrCnt);
              vTrajectory.Correct := false;
            end;
            // ����������� ����� � 3�� ����� ����� �������
            // ���� ����� ���������� ����� ��� �� 20 ������, ��������� ��������
            oldLength := UniStrToFloatDef(vTicketObject.MainData.GetInitialValue('length'), 0);
            newLength := UniStrToFloatDef(vTicketObject.MainData.GetActualValue('length'), 0);
            lengthDiff := Abs(Round(oldLength * 1000) - Round(newLength * 1000));
            if lengthDiff > TRAJ_MIN_VALUE_DIFF then
            begin
              vTrajectory.setStatus(mtosWarning);
              Inc (WrnCnt);
            end;
            break;
          end;
        end;
      end;
      AddStopPlaceTrajectoryObjectLinks(aStopPlaceMuid);
    finally
      FreeAndNil(vStopPlaces);
    end;
  finally
    core.hidePopupMessage();

    ErrWrnMessage := '';
    if WrnCnt <> 0 then
      ErrWrnMessage := ErrWrnMessage + '����� ����� ��� ���������� ���������� ������ ����������' + sLineBreak;

    if ErrCnt <> 0 then
      ErrWrnMessage := ErrWrnMessage + '�� ������� ������������� ���� ��� ��������� ����������' + sLineBreak;

    if Length(ErrWrnMessage) <> 0 then
      showDialog(dtAlert, dbsOK, ErrWrnMessage + '����������� � ������ ���������.');

  end;          
end;

{**********************************************************************************************
* RebuildTrajectoriesByStop
***********************************************************************************************}
procedure TMgtCore.RebuildTrajectoriesByStop(aStopMuid: int64);
var
  vListTraj, vListSP: TlkJSONlist;
  vTrajectory: TMgtRouteTrajectory;
  vTrajMuid: int64;
  i, j: integer;
  oldLength, newLength, lengthDiff: double;
  vRes: boolean;
  vTicketObject: TtrsGISMGTObject;
  vStopPlaces, vTrajectories: TMapInt64;
  datasource: TMgtDatasource;

begin
  vStopPlaces := TMapInt64.Create();
  vTrajectories := TMapInt64.Create(mcIgnore);
  vListSP := getObjects(mgtDatasources[ 'StopPlaces' ], 'stop_muid', IntToStr(aStopMuid), ['muid']);
  // �������� ������ ���� ��� � ��
  for i := 0 to vListSP.Count - 1 do
    vStopPlaces.Add(vListSP.asObject[i].asString['muid']);
  FreeAndNil(vListSP);
  datasource := mgtDatasources[ 'LnkRouteTrajectoriesStopPlaces' ];
  // �������� ������ ���������� ���������� ���������� ����� ���� ��
  for i := 0 to vStopPlaces.Count - 1 do
  begin
    vListTraj := getObjects(datasource, 'stop_place_muid', vStopPlaces.keys[i],
                                                                                ['muid', 'route_trajectory_muid']);
    for j := 0 to vListTraj.Count - 1 do
      vTrajectories.Add(vListTraj.asObject[j].asString['route_trajectory_muid']);
    FreeAndNil(vListTraj);
  end;

  try
    datasource := mgtDatasources[ 'RouteTrajectories' ];
    for i := 0 to vTrajectories.Count - 1 do
    begin
      core.showPopupMessage('���������� ���������� ' + IntToStr(i + 1) + ' �� ' + IntToStr(vTrajectories.Count));
      vTrajMuid := StrToInt64(vTrajectories.keys[i]);

      // ��������� ���������� � ������
      vTicketObject := AddObjectToTicket(datasource, vTrajMuid, oEdit);

      // ��������� � ���������� ������ ����������
      vTrajectory := AddTrajectory(vTrajMuid);

      // ��������������� ����������
      vRes := vTrajectory.RebuildTrajectoryByStopPlaces(vStopPlaces);

      // ��������� ��������� � ������
      vTrajectory.SaveToTicket();
      // ��������� ��������
      cardsManager.refreshTrajectoryForm(vTrajectory.Muid);

      if not vRes then
      begin
        showDialog(dtAlert, dbsOK, '�� ������� ��������� ���������� � ���������������: ' + IntToStr(vTrajectory.Muid));
        vTrajectory.Correct := false;
      end;
      // ����������� ����� � 3�� ����� ����� �������
      // ���� ����� ���������� ����� ��� �� 10 ������, ������ ������������ ������
      oldLength := UniStrToFloatDef(vTicketObject.MainData.GetInitialValue('length'), 0);
      newLength := UniStrToFloatDef(vTicketObject.MainData.GetActualValue('length'), 0);
      lengthDiff := Abs(Round(oldLength * 1000) - Round(newLength * 1000));
      if lengthDiff > 10 then
        vTrajectory.setStatus(mtosWarning);
    end;
  finally
    FreeAndNil(vTrajectories);
  end;

  // ������ ���� ����������, ������� ����� ��� ���������, �� � ������ ���� ������ ��� ���
  try
    for i := 0 to FMapTrajectories.Count - 1 do
    begin
      vTrajectory := TMgtRouteTrajectory(FMapTrajectories.items[i]);
      // ���������� ������������������ � ��������, �� � ��� ����������
      if vTrajectory.Saved then
        Continue;

      // � ��������� ������ ��������� ����� �� ��������������� ��� ����������
      for j := 0 to vTrajectory.ControlPoints.Count - 1 do
      begin
        if vStopPlaces.IndexOf(IntToStr(vTrajectory.ControlPoints.ItemStopPlaceMuid[j])) >= 0 then
        begin
          // ��� ������
          core.showPopupMessage('���������� ������������ ����������');
          // ��������� ����
          AddStopPlaceTrajectoryObjectLink(vTrajectory.ControlPoints.ItemStopPlaceMuid[j], vTrajectory.Muid);

          // ��������������� ����������
          vRes := vTrajectory.RebuildTrajectoryByStopPlaces(vStopPlaces);

          // ��������� ��������� � ������
          vTicketObject := vTrajectory.SaveToTicket();
          // ��������� ��������
          cardsManager.refreshTrajectoryForm(vTrajectory.Muid);

          if not vRes then
          begin
            showDialog(dtAlert, dbsOK, '�� ������� ��������� ���������� � ���������������: ' + IntToStr(vTrajectory.Muid));
            vTrajectory.Correct := false;
          end;
          // ����������� ����� � 3�� ����� ����� �������
          // ���� ����� ���������� ����� ��� �� 20 ������, ��������� ��������
          oldLength := UniStrToFloatDef(vTicketObject.MainData.GetInitialValue('length'), 0);
          newLength := UniStrToFloatDef(vTicketObject.MainData.GetActualValue('length'), 0);
          lengthDiff := Abs(Round(oldLength * 1000) - Round(newLength * 1000));
          if lengthDiff > 10 then
            vTrajectory.setStatus(mtosWarning);
          break;
        end;
      end;
    end;
    for i := 0 to vStopPlaces.Count - 1 do
      AddStopPlaceTrajectoryObjectLinks(StrToInt64(vStopPlaces.keys[i]));
  finally
    FreeAndNil(vStopPlaces);
    core.hidePopupMessage();
  end;
end;

{**********************************************************************************************
* RebuildTrajectoriesByGraphSection
***********************************************************************************************}
procedure TMgtCore.RebuildTrajectoriesByGraphSection(aGraphSectionMuid: int64; RebuildTransportTypeSet : EMgtRouteTransportKindsSet);
var
  vListTraj: TlkJSONlist;
  vTraj: TlkJSONobject;
  vTrajMuid, vLastMuid: int64;
  vTrajectory: TMgtRouteTrajectory;
  i, j: integer;
  FailedRebuildRouteList : TStringList;

begin
  vListTraj := getObjects(mgtDatasources[ 'LnkRouteTrajectoriesGraphSections' ], 'graph_section_muid', IntToStr(aGraphSectionMuid),
                                                                                     ['muid', 'route_trajectory_muid']);
  //������ ���������� �����
  if not Assigned(vListTraj) then
    raise EMgtException.Create('������ ��� ��������� ������ ���������� ����������!');

  FailedRebuildRouteList := TStringList.Create ();
  try
    vLastMuid := -1;
    for i := 0 to vListTraj.Count - 1 do
    begin
      {$IFNDEF GRAPH_POST_COMMIT}
      core.showPopupMessage('���������� ���������� ' + IntToStr(i + 1) + ' �� ' + IntToStr(vListTraj.Count));
      {$ENDIF}

      vTraj := vListTraj.asObject[i];
      vTrajMuid := StrToInt64(vTraj.asString['route_trajectory_muid']);


      // ��� ���������� ��� ������������, ��������
      if vLastMuid = vTrajMuid then
        Continue;

      vLastMuid := vTrajMuid;

      if not (GetTrajectoryTransportKind(vTrajMuid) in RebuildTransportTypeSet) then
        Continue;

       ////// ��������� ����� ����������� ������������ ���������� ///////
       {$IFDEF GRAPH_POST_COMMIT}
       FPostTrajUniqueMuids.addItem(IntToStr (vTrajMuid), aGraphSectionMuid);
       Continue;
       {$ENDIF}
      ////// ��������� ����� ����������� ������������ ���������� ///////

      // ��������� ���������� � ������
      AddObjectToTicket(mgtDatasources[ 'RouteTrajectories' ], vTrajMuid, oEdit);
      // ��������� ����
      AddGraphSectionTrajectoryObjectLink(aGraphSectionMuid, vTrajMuid);
  
      // ��������� � ���������� ������ ����������
      vTrajectory := AddTrajectory(vTrajMuid);

      // ��������������� ����������
      if not vTrajectory.RebuildTrajectoryBySection(aGraphSectionMuid) then
        FailedRebuildRouteList.Add(getObjectFullName(mgtDatasources[ 'RouteTrajectories' ], vTrajMuid));

      // ��������� ��������� � ������
        vTrajectory.SaveToTicket();
      // ��������� ��������
      cardsManager.refreshTrajectoryForm(vTrajectory.Muid);
    end;
  finally
    if (FailedRebuildRouteList.Count <> 0) then
      showDialog(dtAlert, dbsOK, '�� ������� ��������� ���������� � ���������������: ' + FailedRebuildRouteList.DelimitedText);

    FreeAndNil(vListTraj);
    FreeAndnIl (FailedRebuildRouteList);
  end;

  // ������ ���� ����������, ������� ����� ��� ����, �� � ������ ���� ������ ��� ���
  try
    for i := 0 to FMapTrajectories.Count - 1 do
    begin
      vTrajectory := TMgtRouteTrajectory(FMapTrajectories.items[i]);
      // ���������� ������������������ � ��������, �� � ��� ����������
      if vTrajectory.Saved then
        Continue;

      // � ��������� ������ ��������� ����� �� ��������������� ��� ����������
      for j := 0 to vTrajectory.SectionList.Count - 1 do
      begin
        if vTrajectory.SectionList.items[j] = aGraphSectionMuid then
        begin
           ////// ��������� ����� ����������� ������������ ���������� ///////
           {$IFDEF GRAPH_POST_COMMIT}
           FPostTrajUniqueMuids.addItem(inttoStr (vTrajectory.muid), aGraphSectionMuid);
            Continue;
          {$ENDIF}
          ////// ��������� ����� ����������� ������������ ���������� ///////

          // ��� ������
          core.showPopupMessage('���������� ������������ ����������');
          // ��������� ����
          AddGraphSectionTrajectoryObjectLink(aGraphSectionMuid, vTrajectory.Muid);

          vTrajectory.RebuildTrajectoryBySection(aGraphSectionMuid);
          vTrajectory.SaveToTicket(); // ����� �� ��������� � ����� ���������??
          // ��������� ��������
          cardsManager.refreshTrajectoryForm(vTrajectory.Muid);
          break;
        end;
      end;
    end;
  finally
    {$IFNDEF GRAPH_POST_COMMIT}
    core.hidePopupMessage();
    {$ENDIF}
  end;
end;
{**********************************************************************************************
* RebuildTrajectoriesByGraphNode
***********************************************************************************************}
procedure TMgtCore.RebuildTrajectoriesByGraphNode(aGraphNodeMuid: int64);
var
  vAdjasentSections1,vAdjasentSections2 : TlkJSONlist;
  vTrajUniqueMuids: TMapInt64;
  vSectionMuid: string;
  vTrajMuid, vSectionMuidInt64: int64;
  vTrajectory: TMgtRouteTrajectory;
  i,j,iCard: integer;
  datasource: TMgtDatasource;
begin
  datasource := mgtDatasources[ 'GraphSections' ];

  // �������� ���� ������� ���������� ��� ������������� �� ���������� ������������� ����
  vAdjasentSections1 := getObjects(datasource,dbaMapplFieldNames[mfStartNodeMUID],IntToStr(aGraphNodeMuid),['muid']);
  vAdjasentSections2 := getObjects(datasource,dbaMapplFieldNames[mfEndNodeMUID],IntToStr(aGraphNodeMuid),['muid']);

  // ������ ��������������� ���������� ������
  vTrajUniqueMuids := TMapInt64.Create(mcIgnore,True);
  try
    // ���� ��� ������ ����� - �� ��������, ����� ������� ���
    if not ( Assigned(vAdjasentSections1) and Assigned(vAdjasentSections2) ) then
      Exit;

    // ������������ 1 ������
    if Assigned(vAdjasentSections1) then
    begin

      for i := 0 to vAdjasentSections1.Count -1 do
      begin
        vSectionMuid:= TlkJSONobject(vAdjasentSections1[i]).asString['muid'];

        GetTrajectoriesBySectionMuid(vSectionMuid,vTrajUniqueMuids,True);
      end;
    end;

    // ������������ 2 ������
    if Assigned(vAdjasentSections2) then
    begin

      for i := 0 to vAdjasentSections2.Count -1 do
      begin
        vSectionMuid:= TlkJSONobject(vAdjasentSections2[i]).asString['muid'];

        GetTrajectoriesBySectionMuid(vSectionMuid,vTrajUniqueMuids,True);
      end;
    end;

    //
    for i := 0 to vTrajUniqueMuids.Count -1 do
    begin
      {$IFNDEF GRAPH_POST_COMMIT}
      core.showPopupMessage('���������� ���������� ' + IntToStr(i + 1) + ' �� ' + IntToStr(vTrajUniqueMuids.Count));
      {$ENDIF}

      vTrajMuid := StrToInt64Def(vTrajUniqueMuids.keys[i],-1);
      vSectionMuidInt64 := vTrajUniqueMuids.items[i];

      ////// ��������� ����� ����������� ������������ ���������� ///////
      {$IFDEF GRAPH_POST_COMMIT}
      FPostTrajUniqueMuids.addItem(vTrajUniqueMuids.keys[i], vSectionMuidInt64);
      Continue;
      {$ENDIF}
      ////// ��������� ����� ����������� ������������ ���������� ///////

      if (vTrajMuid > 0) and (vSectionMuidInt64 > 0) then
      begin
        // ��������� ������      
        AddObjectToTicket(mgtDatasources[ 'RouteTrajectories' ], vTrajMuid, oEdit);
        // ��������� ����
        AddGraphSectionTrajectoryObjectLink(vSectionMuidInt64, vTrajMuid);
        // ��������� � ���������� ������ ����������
        vTrajectory := AddTrajectory(vTrajMuid);

        // ��������������� ����������
        if not vTrajectory.RebuildTrajectoryBySectionList() then
          showDialog(dtAlert, dbsOK, '�� ������� ��������� ���������� � ���������������: ' + IntToStr(vSectionMuidInt64));
        // ��������� ��������� � ������
        vTrajectory.SaveToTicket();
        // ��������� ��������
        cardsManager.refreshTrajectoryForm(vTrajectory.Muid);

        // ������ ���� ����������, ������� ����� ��� ����, �� � ������ ���� ������ ��� ���
        for iCard := 0 to FMapTrajectories.Count - 1 do
        begin
          vTrajectory := TMgtRouteTrajectory(FMapTrajectories.items[i]);
          // ���������� ������������������ � ��������, �� � ��� ����������
          if vTrajectory.Saved then
            Continue;

          // � ��������� ������ ��������� ����� �� ��������������� ��� ����������
          for j := 0 to vTrajectory.SectionList.Count - 1 do
          begin
            if vTrajectory.SectionList.items[j] = vSectionMuidInt64 then
            begin
              // ��� ������
              core.showPopupMessage('���������� ������������ ����������');
              // ��������� ����
              AddGraphSectionTrajectoryObjectLink(vSectionMuidInt64,vTrajMuid);

              // ��������������� ����������
              if not vTrajectory.RebuildTrajectoryBySectionList() then
                showDialog(dtAlert, dbsOK, '�� ������� ��������� ���������� � ���������������: ' + IntToStr(vTrajectory.Muid));

              vTrajectory.SaveToTicket(); // ����� �� ��������� � ����� ���������??
              // ��������� ��������
              cardsManager.refreshTrajectoryForm(vTrajectory.Muid);
              break;
            end;
          end;
        end;
      end;
    end;

  finally
    FreeAndNil(vAdjasentSections1);
    FreeAndNil(vAdjasentSections2);

    FreeAndNil(vTrajUniqueMuids);

    {$IFNDEF GRAPH_POST_COMMIT}
    core.hidePopupMessage();
    {$ENDIF}
  end;

end;
{**********************************************************************************************
* �heckIncorrctTrajectories
***********************************************************************************************}
function TMgtCore.CheckIncorrctTrajectories(): boolean;
var
  i: integer;
  card: TMgtCard;
begin
  Result := false;
  card := mgtCards[ 'RouteTrajectories' ];
  for i := 0 to FMapTrajectories.Count - 1 do
    if not TMgtRouteTrajectory(FMapTrajectories.items[i]).Correct then
    begin
      Result := true;
      cardsManager.showForm(card, TMgtRouteTrajectory(FMapTrajectories.items[i]).Muid);
      break;
    end;
end;

{**********************************************************************************************
* RebuildAllTrajectories -- ������� ��������� ������������� �������� - ��� ������ �� ��������
***********************************************************************************************}
procedure TMgtCore.RebuildAllTrajectories();
var
  dbRes: TDBResult;
  sql: string;
  vMuid: int64;
  vTrajectory: TMgtRouteTrajectory;
  i: integer;
  vFile: TextFile;
begin
  sql := 'SELECT muid FROM gis_mgt.route_trajectories WHERE sign_deleted = 0 ' +
         ' AND SUBSTRING(wkt_geom, 14, 1) <> ''.''' +
         ' AND okey > 1 ';
//          +          ' LIMIT 1 ';

  if FConn.QueryOpen(sql, dbRes, true) <> 0 then
    raise EMgtException.Create('�� ������� ������� ������ ����������������� ����������');

  i := 1;
  AssignFile(vFile, 'Incorrect trajectories.txt');
  Append(vFile);
  while dbRes.Fetch do
  begin
    vMuid := dbRes.asInt64(0);
    if vMuid < 1 then
      Continue;

    showPopupMessage(IntToStr(i) + ' �� ' + IntToStr(dbRes.numRows));
    try
      vTrajectory := TMgtRouteTrajectory.Create(vMuid, FTicket);
      if vTrajectory.RebuildTrajectory() then
      begin
        vTrajectory.SaveToTicket();
        core.CommitTicket();
      end
      else
        WriteLn(vFile, IntToStr(vMuid));
    except
      on e: Exception do
      begin
        WriteLn(vFile, IntToStr(vMuid) + ' - ' + e.message);
        FreeAndNil(vTrajectory);
        Inc(i);
        Continue;
      end;
    end;
    FreeAndNil(vTrajectory);
    Inc(i);

  end;
  hidePopupMessage();
  CloseFile(vFile);
  FreeAndNil(dbRes);
end;

{**********************************************************************************************
* RebuildNullTrajectories
***********************************************************************************************}
procedure TMgtCore.RebuildNullTrajectories();
var
  dbRes: TDBResult;
  sql: string;
  vMuid: int64;
  vTrajectory: TMgtRouteTrajectory;
  i: integer;
  vTicketObject: TtrsGISMGTObject;
  oldLength, newLength, lengthDiff: double;
begin
  FsqlParams.Clear();
  FsqlParams.itemsByKey['okey_from_value'] := UniFloatToStr(FMain.BSEOKEYFROM.Value);
  FsqlParams.itemsByKey['okey_to_value'] := UniFloatToStr(FMain.BSEOKEYTO.Value);
  FsqlParams.itemsByKey['limit_value'] := UniFloatToStr(FMain.BSELimit.Value);

  sql := getCardsSql('NullRoundTrajectories', FsqlParams);

  if FConn.QueryOpen(sql, dbRes, true) <> 0 then
    raise EMgtException.Create('�� ������� ������� ������ ����������������� ����������');

  if dbRes.numRows = 0 then
    showDialog(dtInfo, dbsOK, '�� �������� ���������� �� ������� �� ����� �������������� ���������� �������� �����!');

  i := 1;
  while dbRes.Fetch() do
  begin
    vTrajectory := nil;
    vMuid := dbRes.asInt64(0);
    if vMuid < 1 then
      Continue;

    showPopupMessage(IntToStr(i) + ' �� ' + IntToStr(dbRes.numRows));
    try
      AddObjectToTicket(mgtDatasources[ 'RouteTrajectories' ], vMuid, oEdit);
      vTrajectory := TMgtRouteTrajectory(FMapTrajectories.itemsByKey[IntToStr(vMuid)]);
      if not Assigned(vTrajectory) then
        Continue;

      //��������� ������ ����������� �����

      if dbRes.isNotNull(1) then
      begin
        vTrajectory.ControlPoints[0].Kind := rcpkStopPlace;
        vTrajectory.ControlPoints[0].Muid := GenerateMUID();
        vTrajectory.ControlPoints[0].StopMode := 1;
        vTrajectory.ControlPoints[0].StopType := 1;

        vTrajectory.ControlPoints[0].StopPlaceMuid := dbRes.asInt64(1);
      end;
      if dbRes.isNotNull(2) then
      begin
        vTrajectory.ControlPoints[vTrajectory.ControlPoints.Count - 1].Kind := rcpkStopPlace;
        vTrajectory.ControlPoints[vTrajectory.ControlPoints.Count - 1].Muid := GenerateMUID();
        vTrajectory.ControlPoints[vTrajectory.ControlPoints.Count - 1].StopMode := 1;
        vTrajectory.ControlPoints[vTrajectory.ControlPoints.Count - 1].StopType := 1;

        vTrajectory.ControlPoints[vTrajectory.ControlPoints.Count - 1].StopPlaceMuid := dbRes.asInt64(2);
      end;

      //��������� ������ ����������� �����

      if vTrajectory.RebuildTrajectory() then
      begin
        vTicketObject := vTrajectory.SaveToTicket();

        // ����������� ����� � 3�� ����� ����� �������
        // ���� ����� ���������� ����� ��� �� 100 ������, ������ ������������ ������
        oldLength := UniStrToFloatDef(vTicketObject.MainData.GetInitialValue('length'), 0);
        newLength := UniStrToFloatDef(vTicketObject.MainData.GetActualValue('length'), 0);
        lengthDiff := Abs(Round(oldLength * 1000) - Round(newLength * 1000));
        if lengthDiff > 400 then
          vTrajectory.setStatus(mtosWarning);
      end
      else
        vTrajectory.setStatus(mtosError);

    except
      on e: Exception do
      begin
        if Assigned(vTrajectory) then
          vTrajectory.setStatus(mtosError);
//        showDialog(dtError, dbsOK, '�� ������� ������������� ����������! MUID: ' + IntToStr(vMuid));
//        FreeAndNil(vTrajectory);
        Inc(i);
        Continue;
      end;
    end;
//    FreeAndNil(vTrajectory);
    Inc(i);

  end;
  hidePopupMessage();
  FreeAndNil(dbRes);
end;

{**********************************************************************************************
* RecalculateTrajectoriesHash - ����������� ���� ����������� ��� (��������� �������)
***********************************************************************************************}
procedure TMgtCore.RecalculateTrajectoriesHash();
var
  i: integer;
  vSql, vHash: string;
  dbRes, dbRes2: TDBResult;
begin
  vSql := 'SELECT rt.muid FROM gis_mgt.route_trajectories rt ' +
          'WHERE rt.sign_deleted = 0 AND rt.route_round_muid IS NOT NULL';

  if FConn.QueryOpen(vSql, dbRes, false) <> 0 then
    raise EMgtException.Create('�� ������� ������� ������ ����������!');

  i := 1;
  try
    while dbRes.Fetch() do
    begin
      core.showPopupMessage('���������� ���� ��� ���������� ' + IntToStr(i) + '...');
      vSql := 'SELECT lnk.stop_place_muid FROM gis_mgt.lnk_route_trajectories__stop_places lnk ' +
              ' WHERE lnk.route_trajectory_muid = ' + dbRes.asString(0) +
              ' AND lnk.sign_deleted = 0 ' +
              ' AND lnk.stop_mode_muid <> 5 ' +
              ' ORDER BY lnk.index ';

      if FConn.QueryOpen(vSql, dbRes2, false) <> 0 then
      begin
        showDialog(dtError, dbsOK, '�� ������� ������� ������ ��������� ���������� ' + dbRes.asString(0) + ' !');
        Continue;
      end;
      
      vHash := '';
      while dbRes2.Fetch() do
      begin
        if vHash = '' then
          vHash := dbRes2.asString(0)
        else
          vHash := vHash + '_' + dbRes2.asString(0);
      end;
      if vHash <> '' then
      begin
        vHash := IntToStr(GenerateMUID(vHash));
        vSql := 'UPDATE gis_mgt.route_trajectories rt SET rt.hash = ' + vHash + ' WHERE rt.muid = ' + dbRes.asString(0);
        if FConn.QueryExec(vSql) <> 0 then
          showDialog(dtError, dbsOK, '�� ������� �������� ��� ���������� ' + dbRes.asString(0) + ' !');
      end;
      FreeAndNil(dbRes2);
      Inc(i);
    end;
  finally
    FreeAndNil(dbRes);
    FreeAndNil(dbRes2);
    core.hidePopupMessage();
  end;
end;


{**********************************************************************************************
* CreateAllStopGraphics -- ������� ��������� ������������� �������� - ��� ������ �� ��������
***********************************************************************************************}
procedure TMgtCore.CreateAllStopGraphics();
var
  vStopGraphics: TMapObjectStructure;
  sql, vPackedGeom: string;
  dbRes: TDBResult;
  vMuid: int64;
  vObjData: TtrsMapplObjectData;
begin
  vStopGraphics := TMapObjectStructure.Create();
  sql := 'SELECT s.okey, s.muid FROM gis_mgt.stops s inner join gis_mgt.stops_to_del sd ON s.muid = sd.muid';

  try
    if FConn.QueryOpen(sql, dbRes, false) <> 0 then
      raise EMgtException.Create('������ ��� ���������� sql-�������: ' + sql);

    while dbRes.Fetch do
    begin
      core.showPopupMessage('���������� ������� ��������� � OKEY ' + dbRes.asString(0));
      vMuid := dbRes.asInt64(1);

      if core.CreateStopGraphics(vMuid, vStopGraphics) then
      begin                                                             
        vPackedGeom := mapCore.GetGeometryAsBase64String(vStopGraphics, mgtDatasources[ 'Stops' ].layerCode );
        vObjData := TtrsMapplObjectData.Create(FConn, 'gis_mgt', 2937670521655343077, mgtDatasources[ 'Stops' ].TableName, vMuid, oEdit, '');
        vObjData.SetFieldValue(MOS_TAG, vPackedGeom);
        vObjData.Commit(FAuth.User.MUID);
        FreeAndNil(vObjData);
      end;
    end;
  finally
    core.hidePopupMessage();
    FreeAndNil(vStopGraphics);
    FreeAndNil(dbRes);
  end;
end;

{**********************************************************************************************
* ReattachAllStopPlacesToGraph
***********************************************************************************************}
procedure TMgtCore.ReattachAllStopPlacesToGraph();
var
  vStopPlaceGraphics: TMapObjectStructure;
  sql: string;
  dbRes: TDBResult;
  vSPMuid, vMuid, vTramMuid: int64;
  vObjData: TtrsMapplObjectData;
  vPoint: TDoublePoint;
  vOffset, vTramOffset: double;
begin
  vStopPlaceGraphics := TMapObjectStructure.Create();
  sql := 'SELECT sp.okey, sp.muid FROM gis_mgt.stop_places sp WHERE sp.sign_deleted = 0 and sp.OKEY > 1';
  try
    if FConn.QueryOpen(sql, dbRes, false) <> 0 then
      raise EMgtException.Create('������ ��� ���������� SQL-�������: ' + sql);

    while dbRes.Fetch do
    begin
      core.showPopupMessage('�������� � ����� ��� � OKEY ' + dbRes.asString(0));
      vSPMuid := dbRes.asInt64(1);

      if not mapCore.GetMapObject(mgtDatasources[ 'StopPlaces' ], vSPMuid, vStopPlaceGraphics) then
        continue;

      vPoint := vStopPlaceGraphics.FirstVertex[0];

      if mapCore.GetNearestGraphSection(vPoint, tkBus, vMuid, vOffset) >= 0 then
        vOffset := mapCore.Mappl.ConvertFromMapUnits(vOffset,muM)
      else if mapCore.GetNearestGraphSection(vPoint, tkTrolley, vMuid, vOffset) >= 0 then
        vOffset := mapCore.Mappl.ConvertFromMapUnits(vOffset,muM);

      if mapCore.GetNearestGraphSection(vPoint, tkTram, vTramMuid, vTramOffset) >= 0 then
        vTramOffset := mapCore.Mappl.ConvertFromMapUnits(vTramOffset,muM);

      vObjData := TtrsMapplObjectData.Create(FConn, 'gis_mgt', 2937670521655343073, mgtDatasources[ 'StopPlaces' ].TableName, vSPMuid, oEdit, '');

      if vOffset >= 0 then
        vObjData.setFieldValue('graph_section_offset', UniFloatToStrF(vOffset, ffFixed, 9, 3))
      else
        vObjData.setFieldValue('graph_section_offset', '');
      if vTramOffset >= 0 then
        vObjData.setFieldValue('graph_tram_section_offset', UniFloatToStrF(vTramOffset, ffFixed, 9, 3))
      else
        vObjData.setFieldValue('graph_tram_section_offset', '');

      if vMuid >= 0 then
        vObjData.setFieldValue('graph_section_muid', IntToStr(vMuid));
      if vTramMuid >= 0 then
        vObjData.setFieldValue('graph_tram_section_muid', IntToStr(vTramMuid));

      if vObjData.Fields.Count > 0 then
        vObjData.Commit(FAuth.User.MUID);
      FreeAndNil(vObjData);
    end;
  finally
    FreeAndNil(vStopPlaceGraphics);
    core.hidePopupMessage();
    FreeAndNil(dbRes);
  end;
end;

{**********************************************************************************************
* RebuildTrajectories
***********************************************************************************************}
procedure TMgtCore.RebuildTrajectories(aMuidList: TMapInt64);
var
  vMuid: int64;
  vTrajectory: TMgtRouteTrajectory;
  i, vFailedCount: integer;
  vTicketObject: TtrsObject;
  oldLength, newLength, lengthDiff: double;
  vRes: boolean;
  datasource: TMgtDatasource;

begin
  if not Assigned(aMuidList) then
    raise EMgtException.Create('������ ��������������� ���������� �� �����!');

  vFailedCount := 0;

  try
    datasource := mgtDatasources[ 'RouteTrajectories' ];
    for i := 0 to aMuidList.Count - 1 do
    begin
      showPopupMessage('���������� ����������...' + IntToStr(i + 1) + ' �� ' + IntToStr(aMuidList.Count));
      vMuid := aMuidList.items[i];
      try
        vTicketObject := AddObjectToTicket(datasource, vMuid, oEdit);
        vTrajectory := AddTrajectory(vMuid);

        //vRes := vTrajectory.RebuildTrajectoryBySectionList();
        //if not vRes then
          vRes := vTrajectory.RebuildTrajectory();

        vTrajectory.SaveToTicket();
        // ��������� ��������
        cardsManager.refreshTrajectoryForm(vTrajectory.Muid);

        if not vRes then
        begin
          Inc(vFailedCount);
          vTrajectory.Correct := false;
        end;

        // ����������� ����� � 3�� ����� ����� �������
        // ���� ����� ���������� ����� ��� �� 20 ������, ��������� ��������
        oldLength := UniStrToFloatDef(vTicketObject.MainData.GetInitialValue('length'), 0);
        newLength := UniStrToFloatDef(vTicketObject.MainData.GetActualValue('length'), 0);
        lengthDiff := Abs(Round(oldLength * 1000) - Round(newLength * 1000));
        if lengthDiff > TRAJ_MIN_VALUE_DIFF then
          vTrajectory.setStatus(mtosWarning);
      except
        showDialog(dtAlert, dbsOK, '�� ������� �������� � ������ ���������� � ��������������� ' + IntToStr(vMuid));
        Continue;
      end;
    end;
  finally
    hidePopupMessage();
    if vFailedCount > 0 then
       showDialog(dtAlert, dbsOK, '�� ������� ����������� ����������: ' + IntToStr(vFailedCount) + ' �� ' + IntToStr(aMuidList.Count));
  end;
end;

{**********************************************************************************************
* AddStopPavilionToPosterApplication
***********************************************************************************************}
procedure TMgtCore.AddStopPavilionToPosterApplication(aStopPavilionMuid: int64);
var
  vObj: TtrsGISMGTObject;
  i, vObjCount: integer;
  vData, vFields: TMapStrings;
  vRefbookMuid: int64;
  jsonObjectList : TlkJSONlist;
  jsonObj : TlkJSONobject;
  datasource: TMgtDatasource;
begin
  try
    core.showPopupMessage('���������� ���������');

    if not (oAdd in mgtCards[ 'PosterApplications' ].Operations) then
      exit;

    if aStopPavilionMuid <= 0 then
      exit;

    vRefbookMUID := GetRefbookMUID(mgtDatasources[ 'PosterApplications' ]);
    vObjCount := core.Ticket.GetObjectsCountByRefbook(vRefbookMuid);

    if vObjCount > 1 then
    begin
      showDialog(dtInfo, dbsOk, '� ������ ������ ����� ����� ������ ��������� � ������ ��������������!' +
                                #13#10 + '��� ���������� ��������� � ������ �������� �� ����� �����.'  );
      exit;
    end;

    jsonObjectList := core.getObjects(mgtDatasources[ 'LnkPosterApplicationPavilions' ], 'stop_pavilion_muid', IntToStr(aStopPavilionMuid),
                                    ['muid', 'poster_application_muid']);

    vFields := TMapStrings.Create();
    vFields.addItem('name', '');
    vFields.addItem('application_status_muid', '');

    datasource := mgtDatasources[ 'PosterApplications' ];
    for i := 0 to jsonObjectList.Count - 1 do
    begin
      jsonObj := jsonObjectList.asObject[i];
      if not getObjectValues(datasource, StrToInt64(jsonObj.asString['poster_application_muid']), vFields) then
        continue;

      if StrToInt(vFields.itemsByKey['application_status_muid']) < 4 then
      begin
        showDialog(dtInfo, dbsOk, '������ �������� ��� ������������ � �������� ������: ' + vFields.itemsByKey['name'] + '!');
        FreeAndNil(vFields);
        FreeAndNil(jsonObjectList);
        exit;
      end;
    end;
    FreeAndNil(vFields);
    FreeAndNil(jsonObjectList);

    vObj := core.GetFirstTicketObjectByDatasource(datasource);
    if not Assigned(vObj) then
    begin
      vObj := AddObjectToTicket(datasource, GenerateMuid(), oAdd);
      setFieldValue(vObj.MainData, 'application_status_muid', '1');
      setFieldValue(vObj.MainData, 'name', '����� ������');
    end;
  //  for i := 0 to vObj.SlaveData.Count - 1 do
  //  begin
  //    vObjData := vObj.SlaveData.items[i];
  //    if vObjData.GetActualValue('stop_pavilion_muid') = IntToStr(aStopPavilionMuid) then
  //    begin
  //      showDialog(dtInfo, dbsOk, '������ �������� ��� ������������ � ������!');
  //      exit;
  //    end;
  //  end;
    vData := TMapStrings.Create(mcReplace);
    try
      vData.addItem('poster_application_muid', IntToStr(vObj.ObjMUID));
      vData.addItem('stop_pavilion_muid', IntToStr(aStopPavilionMuid));
      vData.addItem('stop_pavilion_installation_muid',
                    core.getObjectValue(mgtDatasources[ 'StopPavilions' ], aStopPavilionMuid, 'current_stop_pavilion_installation_muid'));
      vData.addItem('status', '0');
      core.AddSlaveDataToTicket(vObj, mgtDatasources[ 'LnkPosterApplicationPavilions' ], GenerateMuid(), oAdd, vData);
      cardsManager.reloadFormByTicket(datasource, vObj.ObjMUID);
    finally
      FreeAndNil(vData);
    end;
    cardsManager.showForm(mgtCards['PosterApplications'], vObj.ObjMUID);
  finally
    core.hidePopupMessage ();
  end;
end;

{**********************************************************************************************
* AddRouteTrajectoryToMapFilterContainer
***********************************************************************************************}
procedure TMgtCore.AddRTToMapFilterContainer (aRouteTrajectoryMuid: string);
begin
  MapViewFilterTrajContainer.addItem(aRouteTrajectoryMuid, strtoInt64(aRouteTrajectoryMuid));
end;

{**********************************************************************************************
* DeleteRouteTrajectoryFromMapFilterContainer
***********************************************************************************************}
procedure TMgtCore.DeleteRTFromMapFilterContainer (aRouteTrajectoryMuid: string);
begin
  MapViewFilterTrajContainer.Delete(aRouteTrajectoryMuid);
end;

{**********************************************************************************************
* ProcessRTInMapFilterContainer
***********************************************************************************************}
procedure TMgtCore.ProcessRTInMapFilterContainer (aRouteTrajectoryMuid: int64);
var
  vMuid : string;
begin
  if (aRouteTrajectoryMuid <= 0) then
  Exit;

  vMuid := IntToStr(aRouteTrajectoryMuid);

  if MapViewFilterTrajContainer.itemsByKey[vMuid] = 0 then
    AddRTToMapFilterContainer(vMuid)
  else
    DeleteRTFromMapFilterContainer(vMuid);
end;

{**********************************************************************************************
* ClearMapFilterContainer
***********************************************************************************************}
procedure TMgtCore.ClearMapFilterContainer ();
begin
  MapViewFilterTrajContainer.Clear ();
end;

{**********************************************************************************************
* ClearMapTrajectories
***********************************************************************************************}
procedure TMgtCore.ClearMapTrajectories();
var
  i: integer;
  card: TMgtCard;
begin
  i := 0;
  card := mgtCards[ 'RouteTrajectories' ];
  while i < FMapTrajectories.Count do
  begin
    if cardsManager.getOpenedForm(card, TMgtRouteTrajectory(FMapTrajectories.items[i]).Muid) = nil then
      FMapTrajectories.Delete(i)
    else
      Inc(i);
  end;
end;

{**********************************************************************************************
* getDBMainRouteMuidByTrajectoryMuid
***********************************************************************************************}
function TMgtCore.getDBMainRouteMuidByTrajectoryMuid(aTrajectoryMuid: int64): int64;
var
  sql: string;
  dbRes: TDBResult;
begin
  Result := -1;

  FsqlParams.Clear();
  FsqlParams.addItem('muid', IntToStr(aTrajectoryMuid));

  sql := getCoreSQL('MainRouteMuidByTrajectoryMuid', FsqlParams);

  if (conn.QueryOpen(sql, dbRes, true) < 0) then
  begin
    showDialog(dtError, dbsOK, '������ ��� ���������� SQL-�������.', sql);
    exit;
  end;

  if (dbRes.Fetch()) then
    Result := dbRes.asInt64(0);

  dbRes.Free();
end;

{**********************************************************************************************
* GetTrajectoryRoundMuid
***********************************************************************************************}
function TMgtCore.GetTrajectoryRoundMuid(ATrajectoryMuid: Int64; var VRoundMuid: int64): EMgtRouteTrajectoryRoundType;
var
  values: TMapStrings;
begin
  Result:= rtrUndefined;
  VRoundMuid:= -1;

  values:= TMapStrings.Create(True);
  try
    values.addItem(ROUTE_TRAJECTORY_ROUND_UID_FIELD_NAME,'');
    values.addItem(ROUTE_TRAJECTORY_NULL_ROUND_UID_FIELD_NAME,'');

    if getObjectValues(mgtDatasources[ 'RouteTrajectories' ],ATrajectoryMuid,values) then
    begin
      // ������������� �����
      VRoundMuid:=
        StrToInt64Def(values.itemsByKey[ROUTE_TRAJECTORY_ROUND_UID_FIELD_NAME],-1);

      if (VRoundMuid > 0) then
      begin
        Result:= rtrRound;
        Exit;
      end
      else
      begin
        VRoundMuid:= StrToInt64Def(values.itemsByKey[ROUTE_TRAJECTORY_NULL_ROUND_UID_FIELD_NAME],-1);
        if (VRoundMuid > 0) then
          Result:= rtrNullRound;
      end;
    end;
  finally
    FreeandNil(values)
  end

end;

{**********************************************************************************************
* GetTrajectoryVariantMuid
***********************************************************************************************}
function TMgtCore.GetTrajectoryVariantMuid(ATrajectoryMuid: Int64; var VVariantMuid: int64; out Datasource: TMgtDatasource): Integer;
var
  roundMuid: Int64;
  variantTypeFieldName: string;
  values: TMapStrings;
begin
  VVariantMuid:= -1;
  Result:= 0;

  // �������� ������������� �����, ������������ � ��� ��� ����� ������ ��������...
  case GetTrajectoryRoundMuid(ATrajectoryMuid,roundMuid) of
    rtrRound:
    begin
      Datasource:= mgtDatasources[ 'RouteRounds' ];
      variantTypeFieldName:= ROUTE_ROUND_TYPE_UID_FIELD_NAME;
    end;
    rtrNullRound:
    begin
      Datasource:= mgtDatasources[ 'RouteNullRounds' ];
      variantTypeFieldName:= ROUTE_NULL_ROUND_TYPE_UID_FIELD_NAME;
    end;
  else
    begin
      Datasource:= nil;
      Exit;
    end;
  end;

  values:= TMapStrings.Create(True);
  try
    values.addItem(ROUTE_ROUND_VARIANT_UID_FIELD_NAME,'');
    values.addItem(variantTypeFieldName,'');

    // �������� ��������, ���������� ���������...
    if getObjectValues(Datasource,roundMuid,values) then
    begin
      VVariantMuid:= StrToInt64Def(values.itemsByKey[ROUTE_ROUND_VARIANT_UID_FIELD_NAME],-1);
      Result:= StrToIntDef(values.itemsByKey[variantTypeFieldName],0)
    end
    else
      Datasource:= nil;
  finally
    FreeAndNil(values);
  end;
end;

{**********************************************************************************************
* GetTrajectoryRouteMuid
***********************************************************************************************}
function TMgtCore.GetTrajectoryRouteMuid(ATrajectoryMuid: Int64; var VRouteMuid: int64): Boolean;
var
  variantMuid: Int64;
  ds: TMgtDatasource;
begin
  Result:= False;
  VRouteMuid:= -1;

  if (GetTrajectoryVariantMuid(ATrajectoryMuid,variantMuid,ds) = 0) then
    Exit;

  VRouteMuid:= StrToInt64Def(
    core.getObjectValue(mgtDatasources[ 'RouteVariants' ],variantMuid,ROUTE_VARIANT_ROUTE_UID_FIELD_NAME),-1);

  Result:= (VRouteMuid > 0);
end;


{**********************************************************************************************
* GetTrajectoriesByRoundMuid
// �������� ����� ������ � �������� ���������� (���� ��� -1)
***********************************************************************************************}
procedure TMgtCore.GetTrajectoriesByRoundMuid(aRoundMuid: int64; aRoundType: EMgtRouteTrajectoryRoundType;
  var VForwardTrajectoryMuid: int64; var VBackwardTrajectoryMuid: int64);
var
  jsonTrajectories : TlkJSONlist;
  jsonObject : TlkJSONobject;
  i : integer;
  trajectoryType: EMgtRouteTrajectoryType;
  muid: int64;
  roundFieldName: string;
begin
  VForwardTrajectoryMuid := -1;
  VBackwardTrajectoryMuid := -1;

  case aRoundType of
    rtrUndefined: exit;
    rtrRound: roundFieldName := ROUTE_TRAJECTORY_ROUND_UID_FIELD_NAME;
    rtrNullRound: roundFieldName := ROUTE_TRAJECTORY_NULL_ROUND_UID_FIELD_NAME;
  end;

  // �������� ������ � �������� ���������� ����� ��� �������� �����
  jsonTrajectories := core.getObjects(mgtDatasources[ 'RouteTrajectories' ], roundFieldName, IntToStr(aRoundMuid),
    ['muid', 'trajectory_type_muid']);

  if jsonTrajectories.Count > 2 then
    raise Exception.Create('��� ������� ����� ���������� ������ ���� ����������');

  for i := 0 to jsonTrajectories.count - 1 do
  begin
    jsonObject := jsonTrajectories.asObject[i];
    trajectoryType := EMgtRouteTrajectoryType( StrToIntDef( jsonObject.asString['trajectory_type_muid'], 0 ) );
    muid := StrToInt64( jsonObject.asString['muid'] );

    if (trajectoryType = rttForward) then
      VForwardTrajectoryMuid := muid
    else if (trajectoryType = rttBackward) then
      VBackwardTrajectoryMuid := muid;
    //else
      //raise EMgtException.Create('��������� ��� ����������');
  end;

  FreeAndNil(jsonTrajectories);
end;

{**********************************************************************************************
* GetStopPlaces
// �������� ����� ���������� �� ����� ������
***********************************************************************************************}
function TMgtCore.GetTrajectoriesBySectionMuid(aSectionMuid: string; vTrajMuids: TMapInt64; flAppendToList : Boolean): Integer;
var
  i: Integer;
  vTrajsBySection: TlkJSONlist;
  vTrajMuid: Int64;
begin
  Result := -1;
  if (vTrajMuids = nil) then
    Exit;

  // ���� �� ������� ���� ���������� - ������ �������� ������
  if not flAppendToList then
    vTrajMuids.Clear;

  Result:= -1;

  vTrajsBySection := getObjects(mgtDatasources[ 'LnkRouteTrajectoriesGraphSections' ],'graph_section_muid',
                                           aSectionMuid,['muid', 'route_trajectory_muid']);

  if Assigned(vTrajsBySection) then
  begin
    try
      for i := 0 to vTrajsBySection.Count -1 do
      begin
        vTrajMuid := StrToInt64Def((vTrajsBySection[i] as TlkJSONobject).asString['route_trajectory_muid'],-1);
        if vTrajMuid < 0 then
          Continue;

        vTrajMuids.itemsByKey[IntToStr(vTrajMuid)]:= StrToInt64(aSectionMuid);
      end;
    finally
      FreeAndNil(vTrajsBySection);
    end;
    
    Result := vTrajMuids.Count;
  end;
end;

{**********************************************************************************************
* GetGraphSectionsByGraphNode
***********************************************************************************************}
function TMgtCore.GetGraphSectionsByGraphNode(aNodeMuid: int64; aSectionMuids: TMapInt64): integer;
var
  vAdjasentSections: TlkJSONlist;
  i: Integer;
  vSectionMuid: int64;
  datasource: TMgtDatasource;
begin
  Result := -1;
  if (aSectionMuids = nil) then
    Exit;

  aSectionMuids.Clear();

  datasource := mgtDatasources[ 'GraphSections' ];
  // �������� ��������� ����
  vAdjasentSections := getObjects(datasource,dbaMapplFieldNames[mfStartNodeMUID],IntToStr(aNodeMuid),['muid']);
  for i := 0 to vAdjasentSections.Count -1 do
  begin
    vSectionMuid := StrToInt64(TlkJSONobject(vAdjasentSections[i]).asString['muid']);

    aSectionMuids.addItem(datasource.TableName, vSectionMuid);
  end;
  FreeAndNil(vAdjasentSections);

  // �������� �������� ����
  vAdjasentSections := getObjects(datasource,dbaMapplFieldNames[mfEndNodeMUID],IntToStr(aNodeMuid),['muid']);
  for i := 0 to vAdjasentSections.Count -1 do
  begin
    vSectionMuid := StrToInt64(TlkJSONobject(vAdjasentSections[i]).asString['muid']);

    aSectionMuids.addItem(datasource.TableName, vSectionMuid);
  end;
  FreeAndNil(vAdjasentSections);

  Result := aSectionMuids.Count;
end;

{**********************************************************************************************
* GetStopPlaces
// �������� ����� ���� �������-������� ����������
***********************************************************************************************}
function TMgtCore.GetStopPlaces(aTrajectoryMuid: int64): TMapInt64;
var
  jsonStopPlaces : TlkJSONlist;
  jsonObject : TlkJSONobject;
  i : integer;
  muid: int64;
begin
  Result := TMapInt64.Create();

  // ��������� �� ������ ���������
  jsonStopPlaces := core.getObjects(mgtDatasources[ 'LnkRouteTrajectoriesStopPlaces' ], '', IntToStr(aTrajectoryMuid),
    ['muid', 'stop_place_muid', 'index'], false, 'index', jsmInteger);

  for i := 0 to jsonStopPlaces.count - 1 do
  begin
    jsonObject := jsonStopPlaces.asObject[i];
    muid := StrToInt64( jsonObject.asString['muid'] );

    Result.addItem(jsonObject.asString['muid'], muid);
  end;
end;

{**********************************************************************************************
* GetRouteCurrentVariant
// �������� ���� ������������ �������� ��������
***********************************************************************************************}
function TMgtCore.GetRouteCurrentVariant(ARouteMuid: Int64): int64;
begin
  Result := StrToInt64Def(getObjectValue(mgtDatasources[ 'Routes' ], ARouteMuid, 'current_route_variant_muid'), -1);
end;

{**********************************************************************************************
* GetRouteMainRoundByVariant
// �������� ���� ��������� ����� ('00') �� ����� ��������
***********************************************************************************************}
function TMgtCore.GetRouteMainRoundByVariant(AVariantMuid: Int64): int64;
var
  i: integer;
  jsonRounds: TlkJSONlist;
begin
  Result := -1;
  if AVariantMuid <=0 then
    exit;

  // �������� ���� ��������� �����
  jsonRounds := core.getObjects(mgtDatasources[ 'RouteRounds' ], 'route_variant_muid', IntToStr(AVariantMuid), ['muid', 'code']);
  for i := 0 to jsonRounds.Count - 1 do
  begin
    if jsonRounds.asObject[i].asString['code'] = '00' then
    begin
      Result := StrToInt64Def(jsonRounds.asObject[i].asString['muid'], -1);

      break;
    end;
  end;
  jsonRounds.Free();
end;

{**********************************************************************************************
* GetRouteMainRound
// �������� ���� ��������� ����� ('00') ������������ �������� �� ����� ��������
***********************************************************************************************}
function TMgtCore.GetRouteMainRound(ARouteMuid: Int64): int64;
var
  variantMuid: int64;
begin
  Result := -1;
  variantMuid := GetRouteCurrentVariant(ARouteMuid);

  if variantMuid = -1 then
    exit; // � ������� �������� �� ����� �������� �������

  Result := GetRouteMainRoundByVariant(variantMuid);
end;

{**********************************************************************************************
* GetRouteNullRounds
***********************************************************************************************}
function TMgtCore.GetRouteRounds(ARouteMuid: Int64; aRoundType: EMgtRouteTrajectoryRoundType; vRoundMuids: TMapInt64): integer;
var
  vVariantMuid, vRoundMuid: int64;
  i: integer;
  vObj: TlkJSONobject;
  jsonRounds: TlkJSONlist;
  vDatasource: TMgtDatasource;
begin
  Result := -1;
  if not Assigned(vRoundMuids) then
    exit;

  vVariantMuid := GetRouteCurrentVariant(ARouteMuid);
  if vVariantMuid = -1 then
    exit; // � ������� �������� �� ����� �������� �������

  if aRoundType = rtrRound then
    vDatasource := mgtDatasources[ 'RouteRounds' ]
  else if aRoundType = rtrNullRound then
    vDatasource := mgtDatasources[ 'RouteNullRounds' ]
  else
    exit;

  // �������� ���� ��������� �����
  vRoundMuids.Clear();
  jsonRounds := core.getObjects(vDatasource, 'route_variant_muid', IntToStr(vVariantMuid), ['muid', 'code']);
  for i := 0 to jsonRounds.Count - 1 do
  begin
    vObj := jsonRounds.asObject[i];
    vRoundMuid := StrToInt64(vObj.asString['muid']);
    vRoundMuids.addItem(vObj.asString['code'], vRoundMuid);
  end;
  Result := jsonRounds.Count;
  jsonRounds.Free();
end;

{**********************************************************************************************
* GetRouteTrajectoryMuid
***********************************************************************************************}
function TMgtCore.GetRouteTrajectoryMuid(ARouteMuid: Int64; ADirection: EMgtRouteTrajectoryType): int64;
var
  i: integer;
  roundMuid: int64;
  jsonTrajectories: TlkJSONlist;
begin
  Result := -1;
  roundMuid := GetRouteMainRound(ARouteMuid);
  if roundMuid = -1 then
    exit; // � ������� �������� �� ����� �������� ������� ��� ��� ��������� �����

  jsonTrajectories := getObjects(mgtDatasources[ 'RouteTrajectories' ], 'route_round_muid',
                                 IntToStr(roundMuid), ['muid', 'trajectory_type_muid']);

  for i := 0 to jsonTrajectories.Count - 1 do
  begin
    if jsonTrajectories.asObject[i].asString['trajectory_type_muid'] = IntToStr(Integer(ADirection)) then
    begin
      Result := StrToInt64Def(jsonTrajectories.asObject[i].asString['muid'], -1);

      break;
    end;
  end;
  jsonTrajectories.Free();
end;

{**********************************************************************************************
* GetTrajectoryTransportKind
***********************************************************************************************}
function TMgtCore.GetTrajectoryTransportKind(ATrajectoryMuid: Int64): EMgtRouteTransportKind;
var
  routeMuid : int64;
begin
  Result := tkUndefined;

  if not GetTrajectoryRouteMuid(ATrajectoryMuid, routeMuid) then
    Exit;

  Result :=
    EMgtRouteTransportKind(
      StrToInt64Def(core.getObjectValue(mgtDatasources[ 'Routes' ], routeMuid, ROUTE_TRANSPORT_KIND_UID_FIELD_NAME), 0));
end;

{**********************************************************************************************
* GetObjectFromTicket
***********************************************************************************************}
function TMgtCore.GetObjectFromTicket(aDataSource: TMgtDatasource; aMuid: int64): TtrsGISMGTObject;
begin
  Result := GetObjectFromTicket(aDataSource.TableName, aMuid);
end;

{**********************************************************************************************
* GetObjectFromTicket
***********************************************************************************************}
function TMgtCore.GetObjectFromTicket(aTableName: string; aMuid: int64): TtrsGISMGTObject;
var
  vRefbookMuid: int64;
begin
  Result := nil;

  vRefbookMuid := GetRefbookMUID(aTableName);
  if vRefbookMuid <= 0 then
    exit;

  Result := TtrsGISMGTObject(FTicket.GetObjectByRefbookAndMUID(vRefbookMuid, aMuid));
end;

{**********************************************************************************************
* GetRefbookMUID
***********************************************************************************************}
function TMgtCore.GetRefbookMUID(aDataSource: TMgtDatasource): int64;
begin
  Result := GetRefbookMUID(aDataSource.TableName);
end;

{**********************************************************************************************
* GetRefbookMUID
***********************************************************************************************}
function TMgtCore.GetRefbookMUID(aTableName: string): int64;
begin
  Result := FMapRefbooks.itemsByKey[aTableName];
  if Result <= 0 then
  begin
    Result := FClientEngine.getRefbookMUID(aTableName);
    FMapRefbooks.itemsByKey[aTableName] := Result;
  end;
end;

{**********************************************************************************************
* SplitSection
***********************************************************************************************}
procedure TMgtCore.SplitSection(aEditedSection, aNewSection: int64);
var
  i, j: Integer;
  vBase64Mos: string;
  vEditedSectionLength: double;
  vFieldList: TMapStrings;
  vIsRegular, vIsTram: boolean;
  vTrajList: TlkJSONlist;
  vTraj: TlkJSONobject;
  vTrajMuid, vLastMuid, vSrcNodeMuid, vDestNodeMuid: int64;
  vTrajectory: TMgtRouteTrajectory;
  vRes: boolean;
begin
  if (aEditedSection <= 0) or (aNewSection <= 0) then
    raise EMgtException.Create('�� �������� �������������� ��� �����!');

  vFieldList := TMapStrings.Create();
  vFieldList.Add(MOS_TAG);
  vFieldList.Add('has_bus');
  vFieldList.Add('has_trolley');
  vFieldList.Add('has_tram');
  vFieldList.Add('startNodeMUID');
  vFieldList.Add('endNodeMUID');

  getObjectValues(mgtDatasources[ 'GraphSections' ], aEditedSection, vFieldList);
  // �������� ������� �������
  vBase64Mos := vFieldList.itemsByKey[MOS_TAG];
  // �������� ����� ����� ����
  mapCore.GetGeometryLength(vBase64Mos, vEditedSectionLength, muM);

  vIsRegular := (vFieldList.itemsByKey['has_bus'] = '1') or
                (vFieldList.itemsByKey['has_trolley'] = '1');

  vIsTram    := (vFieldList.itemsByKey['has_tram'] = '1');

  // ��������������� ���������
  if vIsRegular then
    ReboundStopPlacesToGraph(aEditedSection, aNewSection, vEditedSectionLength);
  if vIsTram then
    ReboundStopPlacesToTramGraph(aEditedSection, aNewSection, vEditedSectionLength);

  vSrcNodeMuid  := StrToInt64Def(vFieldList.itemsByKey['startNodeMUID'], -1);
  vDestNodeMuid := StrToInt64Def(vFieldList.itemsByKey['endNodeMUID'], -1);
  // �������� ��������� ����
  if (vSrcNodeMuid > 0) and (vDestNodeMuid > 0) then
    CopyNodeData(vSrcNodeMuid, vDestNodeMuid);

  // �������� ��������� ������
  if (aEditedSection > 0) and (aNewSection > 0) then
    CopySectionData (aEditedSection, aNewSection);

  // �������� ���������� ����������� � ���� ����
  // ���������� � ���������� �� ����������, ��������� ���������� ����� ������ �� ���� ��������� ���
  vTrajList := getObjects(mgtDatasources[ 'LnkRouteTrajectoriesGraphSections' ], 'graph_section_muid', IntToStr(aEditedSection),
                ['muid', 'route_trajectory_muid']);

  try
    vLastMuid := -1;
    for i := 0 to vTrajList.Count - 1 do
    begin
      core.showPopupMessage('���������� ���������� ' + IntToStr(i + 1) + ' �� ' + IntToStr(vTrajList.Count));
      vTraj := vTrajList.asObject[i];
      vTrajMuid := StrToInt64(vTraj.asString['route_trajectory_muid']);
      // ��� ���������� ��� ������������, ��������
      if vLastMuid = vTrajMuid then
        Continue;

      vLastMuid := vTrajMuid;

      // ��������� ���������� � ������
      AddObjectToTicket(mgtDatasources[ 'RouteTrajectories' ], vTrajMuid, oEdit);
      // ��������� ����
      AddGraphSectionTrajectoryObjectLink(aEditedSection, vTrajMuid);

      // ��������� � ���������� ������ ����������
      vTrajectory := AddTrajectory(vTrajMuid);

      // ��������� ���� ����������
      vRes := vTrajectory.SplitSection(aEditedSection, aNewSection, vEditedSectionLength);

      // ��������� ��������� � ������
      vTrajectory.SaveToTicket();
      // ��������� ��������
      cardsManager.refreshTrajectoryForm(vTrajectory.Muid);

      if not vRes then
      begin
        showDialog(dtAlert, dbsOK, '�� ������� ��������� ���������� � ���������������: ' + IntToStr(vTrajectory.Muid));
        vTrajectory.Correct := false;
      end;
    end;
  finally
    FreeAndNil(vTrajList);
  end;

  // ������ ���� ����������, ������� ����� ��� ����, �� � ������ ���� ������ ��� ���
  try
    for i := 0 to FMapTrajectories.Count - 1 do
    begin
      vTrajectory := TMgtRouteTrajectory(FMapTrajectories.items[i]);
      // ���������� ������������������ � ��������, �� � ��� ����������
      if vTrajectory.Saved then
        Continue;

      // � ��������� ������ ��������� ����� �� ��������������� ��� ����������
      for j := 0 to vTrajectory.SectionList.Count - 1 do
      begin
        if vTrajectory.SectionList.items[i] = aEditedSection then
        begin
          // ��� ������
          core.showPopupMessage('���������� ������������ ����������');
          // ��������� ����
          AddGraphSectionTrajectoryObjectLink(aEditedSection, vTrajectory.Muid);

          // ��������������� ����������
          vRes := vTrajectory.SplitSection(aEditedSection, aNewSection, vEditedSectionLength);

          // ��������� ��������� � ������
          vTrajectory.SaveToTicket();
          // ��������� ��������
          cardsManager.refreshTrajectoryForm(vTrajectory.Muid);

          if not vRes then
          begin
            showDialog(dtAlert, dbsOK, '�� ������� ��������� ���������� � ���������������: ' + IntToStr(vTrajectory.Muid));
            vTrajectory.Correct := false;
          end;

          break;
        end;
      end;
    end;
  finally
    core.hidePopupMessage();
  end;
end;

{**********************************************************************************************
* CopyNodeData
***********************************************************************************************}
procedure TMgtCore.CopyNodeData(aSrcNodeMuid, aDestNodeMuid: int64);
var
  vNodeObj: TtrsGISMGTObject;
  vFieldList: TMapStrings;
begin
  vNodeObj := GetObjectFromTicket(mgtDatasources[ 'GraphNodes' ], aDestNodeMuid);
  if not Assigned(vNodeObj) then
    exit;

  vFieldList := TMapStrings.Create();
  vFieldList.Add('street_muid');
  vFieldList.Add('direction_muid');
  try
    getObjectValues(mgtDatasources[ 'GraphNodes' ], aSrcNodeMuid, vFieldList);
    setFieldValue(vNodeObj.MainData, 'street_muid', vFieldList.itemsByKey['street_muid']);
    setFieldValue(vNodeObj.MainData, 'direction_muid', vFieldList.itemsByKey['direction_muid']);
  finally
    FreeAndNil(vFieldList);
  end;
end;

{**********************************************************************************************
* CopySectionData
***********************************************************************************************}
procedure TMgtCore.CopySectionData(aSrcSectionMuid, aDestSectionMuid: int64);
var
  vSectionObj: TtrsGISMGTObject;
  vFieldList: TMapStrings;
begin
  vSectionObj := GetObjectFromTicket(mgtDatasources[ 'GraphSections' ], aDestSectionMuid);
  if not Assigned(vSectionObj) then
    exit;

  vFieldList := TMapStrings.Create();
  vFieldList.Add('has_bus');
  vFieldList.Add('has_trolley');
  vFieldList.Add('has_tram');
  vFieldList.Add('has_bus_lane');

  try
    getObjectValues(mgtDatasources[ 'GraphSections' ], aSrcSectionMuid, vFieldList);
    setFieldValue(vSectionObj.MainData, 'has_bus', vFieldList.itemsByKey['has_bus']);
    setFieldValue(vSectionObj.MainData, 'has_trolley', vFieldList.itemsByKey['has_trolley']);
    setFieldValue(vSectionObj.MainData, 'has_tram', vFieldList.itemsByKey['has_tram']);
    setFieldValue(vSectionObj.MainData, 'has_bus_lane', vFieldList.itemsByKey['has_bus_lane']);
  finally
    FreeAndNil(vFieldList);
  end;
end;

{**********************************************************************************************
* UniteSection
***********************************************************************************************}
procedure TMgtCore.UniteSection(aEditedSection, aRemovedSection: int64);
begin

end;


//������������� ����� � ������� ����
{**********************************************************************************************
* RecalcOffsetStopPlacesToGraph
***********************************************************************************************}
function TMgtCore.RecalcOffsetStopPlacesToGraph(aSectionMuid : int64) : boolean;
var
  i: integer;
  vOffset: double;
  vStopPlaces: TlkJSONlist;
  vStopPlace: TlkJSONobject;
  vMuid: int64;
  dsStopPlaces, dsNodes, dsSections: TMgtDatasource;
  vTicketObj: TtrsGISMGTObject;
  startNodeDB: boolean;
  endNodeDB: boolean;
  vTempMuid: string;
begin
  Result := true;
  
  dsNodes := mgtDatasources[ 'GraphNodes' ];
  dsSections := mgtDatasources[ 'GraphSections' ];
  
  // ���� ��������� ��� �������� ���� ������ ���� � ������ ���� � � ���� ��� (���������)
  // �� ������ ���� ������ � ������������ ���������� � splitSection

  getObjectValueFromTicket(dsSections, aSectionMuid, 'StartNodeMuid', vTempMuid);
  startNodeDB := getObjectValueFromDB(dsNodes, StrToInt64Def(vTempMuid, -1), 'muid', vTempMuid);

  getObjectValueFromTicket(dsSections, aSectionMuid, 'EndNodeMuid', vTempMuid);
  endNodeDB := getObjectValueFromDB(dsNodes, StrToInt64Def(vTempMuid, -1), 'muid', vTempMuid);

  // ���� ���� �� ������ ���� ���� � �� - ������ ���� ����������� - �������
  if (not startNodeDB) or (not endNodeDB) then
    Exit;

  // �������� ��� ����������� � ���� ���� ����� �������-�������
  dsStopPlaces := mgtDatasources[ 'StopPlaces' ];  
  vStopPlaces := getObjects(dsStopPlaces, 'graph_section_muid', IntToStr(aSectionMuid), ['muid']);

  for i := 0 to vStopPlaces.Count - 1 do
  begin
    vStopPlace := vStopPlaces.asObject[i];
    vMuid := StrToInt64(vStopPlace.asString['muid']);
    vOffset := mapcore.GetStopPlaceGraphSectionOffset(vMuid, aSectionMuid);

    //���� ������������ �������� ��� ��������� ������
     if vOffset < 0 then
     begin
       Result := false;
       exit;
     end;

    // ��������� ��������� � ������ � �������������
    vTicketObj := AddObjectToTicket(dsStopPlaces, vMuid, oEdit);
    AddObjectLink(dsStopPlaces, vMuid, mgtDatasources[ 'GraphSections' ], aSectionMuid, [tltCascade, tltCommit, tltDenyDelete]);
    setFieldValue(vTicketObj.MainData, 'graph_section_offset', UniFormatFloat('0.###', vOffset));

  end;
  FreeAndNil(vStopPlaces);
end;

//������������� ����� � ���������� ����
{**********************************************************************************************
* RecalcOffsetStopPlacesToTramGraph
***********************************************************************************************}
function TMgtCore.RecalcOffsetStopPlacesToTramGraph(aSectionMuid : int64) : boolean;
var
  i: Integer;
  vOffset: double;
  vStopPlaces: TlkJSONlist;
  vStopPlace: TlkJSONobject;
  vMuid: int64;
  vTicketObj: TtrsGISMGTObject;
  dsStopPlaces, dsNodes, dsSections: TMgtDatasource;
  startNodeDB: boolean;
  endNodeDB: boolean;
  vTempMuid: string;
begin
  Result := true;
   
  dsNodes := mgtDatasources[ 'GraphNodes' ];
  dsSections := mgtDatasources[ 'GraphSections' ];   

  // ���� ��������� ��� �������� ���� ������ ���� � ������ ���� � � ���� ��� (���������)
  // �� ������ ���� ������ � ������������ ���������� � splitSection

  getObjectValueFromTicket(dsSections, aSectionMuid, 'StartNodeMuid', vTempMuid);
  startNodeDB := getObjectValueFromDB(dsNodes, StrToInt64Def(vTempMuid, -1), 'muid', vTempMuid);

  getObjectValueFromTicket(dsSections, aSectionMuid, 'EndNodeMuid', vTempMuid);
  endNodeDB := getObjectValueFromDB(dsNodes, StrToInt64Def(vTempMuid, -1), 'muid', vTempMuid);

  // ���� ���� �� ������ ���� ���� � �� - ������ ���� ����������� - �������
  if (not startNodeDB) or (not endNodeDB) then
    Exit;

  // �������� ��� ����������� � ���� ���� ����� �������-�������
  dsStopPlaces := mgtDatasources[ 'StopPlaces' ];  
  vStopPlaces := getObjects(dsStopPlaces, 'graph_tram_section_muid', IntToStr(aSectionMuid), ['muid']);

  for i := 0 to vStopPlaces.Count - 1 do
  begin
    vStopPlace := vStopPlaces.asObject[i];
    vMuid := StrToInt64(vStopPlace.asString['muid']);
    vOffset := mapcore.GetStopPlaceGraphSectionOffset(vMuid, aSectionMuid);

    //���� ������������ �������� ��� ��������� ������
     if vOffset < 0 then
     begin
       Result := false;
       exit;
     end;

    // ��������� ��������� � ������ � �������������
    vTicketObj := AddObjectToTicket(dsStopPlaces, vMuid, oEdit);
    AddObjectLink(dsStopPlaces, vMuid, mgtDatasources[ 'GraphSections' ], aSectionMuid, [tltCascade, tltCommit, tltDenyDelete]);
    setFieldValue(vTicketObj.MainData, 'graph_tram_section_offset', UniFormatFloat('0.###', vOffset));

  end;
  FreeAndNil(vStopPlaces);
end;

{**********************************************************************************************
* ReboundStopPlacesToGraph
***********************************************************************************************}
procedure TMgtCore.ReboundStopPlacesToGraph(aSectionMuid, aNewSectionMuid: Int64; aSectionLength: double);
var
  i: Integer;
  vOffset: double;
  vStopPlaces: TlkJSONlist;
  vStopPlace: TlkJSONobject;
  vMuid: int64;
  vTicketObj: TtrsGISMGTObject;
  datasource: TMgtDatasource;
begin
  datasource := mgtDatasources[ 'StopPlaces' ];
  // �������� ��� ����������� � ���� ���� ����� �������-�������
  vStopPlaces := getObjects(datasource, 'graph_section_muid', IntToStr(aSectionMuid),
                ['muid', 'graph_section_offset']);
  for i := 0 to vStopPlaces.Count - 1 do
  begin
    vStopPlace := vStopPlaces.asObject[i];
    vOffset := UniStrToFloatDef(vStopPlace.asString['graph_section_offset'], -1);
    // ������ �� ����������, ������ �� ������
    if vOffset < 0 then
      Continue;

    // ��������� ���� �� ������������� ����� �������-������� �� ����� ����
    if vOffset > aSectionLength then
    begin
      // ��������� ��������� � ������ � �������������
      vMuid := StrToInt64(vStopPlace.asString['muid']);
      vTicketObj := AddObjectToTicket(datasource, vMuid, oEdit);
      AddObjectLink(datasource, vMuid, mgtDatasources[ 'GraphSections' ], aNewSectionMuid, [tltCascade, tltCommit, tltDenyDelete]);
      setFieldValue(vTicketObj.MainData, 'graph_section_muid', aNewSectionMuid);
      setFieldValue(vTicketObj.MainData, 'graph_section_offset',UniFloatToStrF(vOffset - aSectionLength, ffFixed, 9, 3));
    end;
  end;
  FreeAndNil(vStopPlaces);
end;

{**********************************************************************************************
* ReboundStopPlacesToTramGraph
***********************************************************************************************}
procedure TMgtCore.ReboundStopPlacesToTramGraph(aSectionMuid, aNewSectionMuid: Int64; aSectionLength: double);
var
  i: Integer;
  vOffset: double;
  vStopPlaces: TlkJSONlist;
  vStopPlace: TlkJSONobject;
  vMuid: int64;
  vTicketObj: TtrsGISMGTObject;
  datasource: TMgtDatasource;
begin
  datasource := mgtDatasources[ 'StopPlaces' ];
  // �������� ��� ����������� � ���� ���� ����� �������-�������
  vStopPlaces := getObjects(datasource, 'graph_tram_section_muid', IntToStr(aSectionMuid),
                ['muid', 'graph_tram_section_offset']);
  for i := 0 to vStopPlaces.Count - 1 do
  begin
    vStopPlace := vStopPlaces.asObject[i];
    vOffset := UniStrToFloat(vStopPlace.asString['graph_tram_section_offset']);
    // ������ �� ����������, ������ �� ������
    if vOffset < 0 then
      Continue;

    // ��������� ���� �� ������������� ����� �������-������� �� ����� ����
    if vOffset > aSectionLength then
    begin
      // ��������� ��������� � ������ � �������������
      vMuid := StrToInt64(vStopPlace.asString['muid']);
      vTicketObj := AddObjectToTicket(datasource, vMuid, oEdit);
      AddObjectLink(datasource, vMuid, mgtDatasources[ 'GraphSections' ], aNewSectionMuid, [tltCascade, tltCommit, tltDenyDelete]);
      setFieldValue(vTicketObj.MainData, 'graph_tram_section_muid', aNewSectionMuid);
      setFieldValue(vTicketObj.MainData, 'graph_tram_section_offset', UniFloatToStrF(vOffset - aSectionLength, ffFixed, 9, 3));
    end;
  end;
  FreeAndNil(vStopPlaces);
end;

{**********************************************************************************************
* addNodeToList
***********************************************************************************************}
procedure TMgtCore.addNodeToList(aNodeMuid: int64; vStrDirList: TMapIntegers);
var
  i: integer;
  vStreetMuid1, vStreetMuid2: string;
  vDirection1, vDirection2: integer;
  datasource: TMgtDatasource;
begin
  if not Assigned(vStrDirList) then
    exit;

  datasource := mgtDatasources[ 'GraphNodes' ];
  vStreetMuid1 := core.getObjectValue(datasource, aNodeMuid, 'street_muid');
  vDirection1 := StrToIntDef(core.getObjectValue(datasource, aNodeMuid, 'direction_muid'), -1);
  for i := 0 to vStrDirList.Count - 1 do
  begin
    vStreetMuid2 := vStrDirList.keys[i];
    vDirection2 := vStrDirList.items[i];
    if (vStreetMuid1 = vStreetMuid2) and (vDirection1 = vDirection2) then
      exit;
  end;
  vStrDirList.addItem(vStreetMuid1, vDirection1);
end;

{**********************************************************************************************
* addSectionToList
***********************************************************************************************}
procedure TMgtCore.addSectionToList(aSectionMuid: int64; vStrDirList: TMapIntegers);
var
  vNodeMuid: int64;
  datasource: TMgtDatasource;
begin
  if not Assigned(vStrDirList) then
    exit;

  datasource := mgtDatasources[ 'GraphSections' ];
  vNodeMuid := StrToInt64Def(core.getObjectValue(datasource, aSectionMuid, 'startNodeMuid'), -1);
  if vNodeMuid > 0 then
    addNodeToList(vNodeMuid, vStrDirList);

  vNodeMuid := StrToInt64Def(core.getObjectValue(datasource, aSectionMuid, 'endNodeMuid'), -1);
  if vNodeMuid > 0 then
    addNodeToList(vNodeMuid, vStrDirList);
end;

{**********************************************************************************************
* formStrDirListBySPCoords
***********************************************************************************************}
procedure TMgtCore.formStrDirListBySPCoords(aPoint: TDoublePoint; vStrDirList: TMapIntegers);
var
  vSectionMuid: int64;
  vOffset: double;
begin
  if not Assigned(vStrDirList) then
    exit;

  vStrDirList.Clear();
  //�������� � ����� �������������� ���� � ������� ��� ���������
  if mapCore.GetNearestGraphSection(aPoint, tkBus, vSectionMuid, vOffset) >= 0 then
    addSectionToList(vSectionMuid, vStrDirList)
  else if mapCore.GetNearestGraphSection(aPoint, tkTrolley, vSectionMuid, vOffset) >= 0 then
    addSectionToList(vSectionMuid, vStrDirList);
  if mapCore.GetNearestGraphSection(aPoint, tkTram, vSectionMuid, vOffset) >= 0 then
    addSectionToList(vSectionMuid, vStrDirList);
end;

{**********************************************************************************************
* updateStoredFiltration
***********************************************************************************************}
procedure TMgtCore.updateStoredFiltration(flRefreshMap: boolean);
begin
  if StoredFiltration.flIsUpdating then
    exit;
    
  StoredFiltration.flIsUpdating := true;
  try
    mgtDatasets['Routes'].compGridView.DataController.Filter.BeginUpdate();
    AddGridRouteStoredFiltration();
    mgtDatasets['Routes'].compGridView.DataController.Filter.EndUpdate();

    mgtDatasets['Stops'].compGridView.DataController.Filter.BeginUpdate();
    AddGridStopStoredFiltration(mgtDatasets['Stops']);
    mgtDatasets['Stops'].compGridView.DataController.Filter.EndUpdate();

    mgtDatasets[ 'StopPlaces' ].compGridView.DataController.Filter.BeginUpdate();
    AddGridStopStoredFiltration(mgtDatasets[ 'StopPlaces' ]);
    mgtDatasets[ 'StopPlaces' ].compGridView.DataController.Filter.EndUpdate();

    mgtDatasets[ 'RouteTrajectories' ].compGridView.DataController.Filter.BeginUpdate();
    AddGridRouteTrajectoryStoredFiltration(mgtDatasets[ 'RouteTrajectories' ]);
    mgtDatasets[ 'RouteTrajectories' ].compGridView.DataController.Filter.EndUpdate();

    mgtDatasets[ 'RouteNullTrajectories' ].compGridView.DataController.Filter.BeginUpdate();
    AddGridRouteNullTrajectoryStoredFiltration();
    mgtDatasets[ 'RouteNullTrajectories' ].compGridView.DataController.Filter.EndUpdate();

    mgtDatasets[ 'StopPlaceRouteTrajectories' ].compGridView.DataController.Filter.BeginUpdate();
    AddGridRouteTrajectoryStoredFiltration(mgtDatasets[ 'StopPlaceRouteTrajectories' ]);
    mgtDatasets[ 'StopPlaceRouteTrajectories' ].compGridView.DataController.Filter.EndUpdate();

    mgtDatasets[ 'StreetRoutes' ].compGridView.DataController.Filter.BeginUpdate();
    AddGridRouteTrajectoryStoredFiltration(mgtDatasets[ 'StreetRoutes' ]);
    mgtDatasets[ 'StreetRoutes' ].compGridView.DataController.Filter.EndUpdate();

    mgtDatasets[ 'TerminalPointRounds' ].compGridView.DataController.Filter.BeginUpdate();
    AddGridRouteTrajectoryStoredFiltration(mgtDatasets[ 'TerminalPointRounds' ]);
    mgtDatasets[ 'TerminalPointRounds' ].compGridView.DataController.Filter.EndUpdate();

    mgtDatasets[ 'SPTasks' ].compGridView.DataController.Filter.BeginUpdate();
    AddTOStoredFiltration(mgtDatasets[ 'SPTasks' ]);
    mgtDatasets[ 'SPTasks' ].compGridView.DataController.Filter.EndUpdate();

    mgtDatasets[ 'SPStopplaces' ].compGridView.DataController.Filter.BeginUpdate();
    AddTOStoredFiltration(mgtDatasets[ 'SPStopplaces' ]);
    mgtDatasets[ 'SPStopplaces' ].compGridView.DataController.Filter.EndUpdate();

    if flRefreshMap then
    begin
      AddMapRouteStoredFiltration();
      AddMapStopPlacesStoredFiltration();

      mapCore.Mappl.RefreshMap();
      mapCore.Mappl.TrySetFocus();
    end;
  finally
    StoredFiltration.flIsUpdating := false;
  end;
end;

{**********************************************************************************************
* AddMapRouteStoredFiltration
***********************************************************************************************}
procedure TMgtCore.AddMapRouteStoredFiltration();
var
  vFormula: string;
  vLayerCode: integer;
begin
  // ���� ����������
  vLayerCode := mgtDatasources[ 'RouteTrajectories' ].layerCode;

  // ��������
  vFormula := '';
  if StoredFiltration.VariantActive then
    vFormula := '((1 OR 2)';
  if StoredFiltration.VariantPlanned then
  begin
    if StoredFiltration.VariantActive then
      vFormula := vFormula + ' OR ((3 OR 4) AND (5 OR 6))'
    else
      vFormula := '(((3 OR 4) AND (5 OR 6))';
  end;
  if StoredFiltration.VariantArchive then
  begin
    if StoredFiltration.VariantActive or StoredFiltration.VariantPlanned then
      vFormula := vFormula + ' OR (7 OR 8)'
    else
      vFormula := '((7 OR 8)';
  end;
  if vFormula <> '' then
    vFormula := vFormula + ')'
  else
    vFormula := '(1 AND 2)';
    
  // ���� ����������
  if StoredFiltration.TransportKindBus then
    vFormula := vFormula + ' AND ((9 OR 10)';
  if StoredFiltration.TransportKindTrolley then
  begin
    if StoredFiltration.TransportKindBus then
      vFormula := vFormula + ' OR (11 OR 12)'
    else
      vFormula := vFormula + ' AND ((11 OR 12)'
  end;
  if StoredFiltration.TransportKindTram then
  begin
    if StoredFiltration.TransportKindBus or StoredFiltration.TransportKindTrolley then
      vFormula := vFormula + ' OR (13 OR 14)'
    else
      vFormula := vFormula + ' AND ((13 OR 14)'
  end;
  if StoredFiltration.TransportKindBus or StoredFiltration.TransportKindTrolley or StoredFiltration.TransportKindTram then
    vFormula := vFormula + ')'
  else
    vFormula := vFormula + ' AND (9 AND 10)';

  // ��������
  if StoredFiltration.RouteIsMGT then
    vFormula := vFormula + ' AND ((15 OR 16) AND (32 OR 33)';
  if StoredFiltration.RouteIsCommercial then
  begin
    if StoredFiltration.RouteIsMGT then
      vFormula := vFormula + ' OR (17 OR 18)'
    else
      vFormula := vFormula + ' AND ((17 OR 18)'
  end;
  if StoredFiltration.RouteIsCompensatory then
  begin
    if StoredFiltration.RouteIsMGT or StoredFiltration.RouteIsCommercial then
      vFormula := vFormula + ' OR NOT (32 AND 33)'
    else
      vFormula := vFormula + ' AND (NOT (32 AND 33)'
  end;
  if StoredFiltration.RouteIsMGT or StoredFiltration.RouteIsCommercial or StoredFiltration.RouteIsCompensatory then
    vFormula := vFormula + ')'
  else
    vFormula := vFormula + ' AND (15 AND 16)';

  // �����
  if StoredFiltration.RoundMain then
    vFormula := vFormula + ' AND (19';
  if StoredFiltration.RoundAdditional then
  begin
    if StoredFiltration.RoundMain then
      vFormula := vFormula + ' OR 20'
    else
      vFormula := vFormula + ' AND (20';
  end;
  if StoredFiltration.RoundNull then
  begin
    if StoredFiltration.RoundMain or StoredFiltration.RoundAdditional then
      vFormula := vFormula + ' OR 21'
    else
      vFormula := vFormula + ' AND (21'
  end;
  if StoredFiltration.RoundMain or StoredFiltration.RoundAdditional or StoredFiltration.RoundNull then
    vFormula := vFormula + ')'
  else
    vFormula := vFormula + ' AND (19 AND 20)';

  // ������� ���������
  if StoredFiltration.RouteStateForApproval then
    vFormula := vFormula + ' AND ((22 OR 23)';
  if StoredFiltration.RouteStateOpened then
  begin
    if StoredFiltration.RouteStateForApproval then
      vFormula := vFormula + ' OR (24 OR 25)'
    else
      vFormula := vFormula + ' AND ((24 OR 25)';
  end;
  if StoredFiltration.RouteStateTempOpened then
  begin
    if StoredFiltration.RouteStateForApproval or StoredFiltration.RouteStateOpened then
      vFormula := vFormula + ' OR (26 OR 27)'
    else
      vFormula := vFormula + ' AND ((26 OR 27)';
  end;
  if StoredFiltration.RouteStateTempClosed then
  begin
    if StoredFiltration.RouteStateForApproval or StoredFiltration.RouteStateOpened or
       StoredFiltration.RouteStateTempOpened then
      vFormula := vFormula + ' OR (28 OR 29)'
    else
      vFormula := vFormula + ' AND ((28 OR 29)';
  end;
  if StoredFiltration.RouteStateClosed then
  begin
    if StoredFiltration.RouteStateForApproval or StoredFiltration.RouteStateOpened or
       StoredFiltration.RouteStateTempOpened or StoredFiltration.RouteStateTempClosed then
      vFormula := vFormula + ' OR (30 OR 31)'
    else
      vFormula := vFormula + ' AND ((30 OR 31)';
  end;

  if StoredFiltration.RouteStateForApproval or StoredFiltration.RouteStateOpened or
     StoredFiltration.RouteStateTempOpened or StoredFiltration.RouteStateTempClosed or StoredFiltration.RouteStateClosed then
    vFormula := vFormula + ')'
  else
    vFormula := vFormula + ' AND (22 AND 23)';

  // ������ �������
  mapCore.Mappl.Layer[vLayerCode].FilterContainer.FilterMode := fmRules;
  mapCore.Mappl.Layer[vLayerCode].FilterContainer.FilterRule.Formula := vFormula;
  mapCore.Mappl.flLayerMapFilterStr[vLayerCode] := true;
end;

{**********************************************************************************************
* AddMapStopPlacesStoredFiltration
***********************************************************************************************}
procedure TMgtCore.AddMapStopPlacesStoredFiltration();
var
  vFormula: string;
  vLayerCode: integer;
begin
  // ���� ���� �������\�������
  vLayerCode := mgtDatasources[ 'StopPlaces' ].layerCode;

  vFormula := '';
  if StoredFiltration.StopPlaceActive then
  begin
    if StoredFiltration.TransportKindBus then
      vFormula := '(1';
    if StoredFiltration.TransportKindTrolley then
    begin
      if StoredFiltration.TransportKindBus then
        vFormula := vFormula + ' OR 2'
      else
        vFormula := '(2';
    end;
    if StoredFiltration.TransportKindTram then
    begin
      if StoredFiltration.TransportKindBus or StoredFiltration.TransportKindTrolley then
        vFormula := vFormula + ' OR 3'
      else
        vFormula := '(3';
    end;
    if StoredFiltration.TransportKindBus or
       StoredFiltration.TransportKindTrolley or
       StoredFiltration.TransportKindTram then
      vFormula := vFormula + ')'
    else
      vFormula := '(1 AND NOT 1)';
  end;

  if not StoredFiltration.StopPlaceActive then
    vFormula := '(1 AND NOT 1)';

  if StoredFiltration.RouteIsMGT then
    vFormula := vFormula + ' AND (4 ';
  if StoredFiltration.RouteIsCommercial then
  begin
    if StoredFiltration.RouteIsMGT then
      vFormula := vFormula + ' OR 5'
    else
      vFormula := vFormula + ' AND (5'
  end;
  if StoredFiltration.RouteIsCompensatory then
  begin
    if StoredFiltration.RouteIsMGT or StoredFiltration.RouteIsCommercial then
      vFormula := vFormula + ' OR (6 OR 7)'
    else
      vFormula := vFormula + ' AND ((6 OR 7)'
  end;
  if StoredFiltration.RouteIsMGT or
     StoredFiltration.RouteIsCommercial or
     StoredFiltration.RouteIsCompensatory then
    vFormula := vFormula + ')'
  else 
    vFormula := vFormula + ' AND (4 AND NOT 4)';

  if StoredFiltration.StopPlaceInactive then
    vFormula := vFormula + ' OR NOT (1 OR 2 OR 3)';

  // ������ �������
  mapCore.Mappl.Layer[vLayerCode].FilterContainer.FilterMode := fmRules;
  mapCore.Mappl.Layer[vLayerCode].FilterContainer.FilterRule.Formula := vFormula;
  mapCore.Mappl.flLayerMapFilterStr[vLayerCode] := true;
end;

{**********************************************************************************************
* getFilterItemList
***********************************************************************************************}
function TMgtCore.getFilterItemList(aRoot: TcxFilterCriteriaItemList; aDisplayValues: TMapStrings): TcxFilterCriteriaItemList;
var
  i: integer;
  vColumn: TcxGridExtendedDBColumn;
begin
  Result := nil;

  if not Assigned(aDisplayValues) then
    exit;

  for i := 0 to aRoot.Count - 1 do
  begin
    if aRoot.Items[i].IsItemList then
      Result := getFilterItemList(TcxFilterCriteriaItemList(aRoot.Items[i]), aDisplayValues);

    if Assigned(Result) then
      break;

    if not aRoot.Items[i].IsItemList then
    begin
      if TcxFilterCriteriaItem(aRoot.Items[i]).ItemLink is TcxGridExtendedDBColumn then
      begin
        vColumn := TcxGridExtendedDBColumn(TcxFilterCriteriaItem(aRoot.Items[i]).ItemLink);
        if aDisplayValues.IndexOf(vColumn.DataBinding.FieldName) >= 0 then
        begin
          Result := aRoot.Items[i].Parent;
          exit;
        end;
      end;
    end;
  end;
  while Assigned(Result) and (Result.Parent <> aRoot) do
    Result := Result.Parent;
end;

{**********************************************************************************************
* AddGridRouteStoredFiltration
***********************************************************************************************}
procedure TMgtCore.AddGridRouteStoredFiltration();
var
  filter: TcxDBDataFilterCriteria;
  filterItemList: TcxFilterCriteriaItemList;
  vFieldList: TMapStrings;
  vColumn: TcxGridExtendedDBColumn;
  gridView: TcxGridExtendedDBTableView;
begin
  gridView := mgtDatasets[ 'Routes' ].compGridView;
  filter := gridView.DataController.Filter;
  vFieldList := TMapStrings.Create();
  try
    // ��� ����������
    vFieldList.addItem('filter_route_transport_kind', '');
    filterItemList := getFilterItemList(filter.Root, vFieldList);

    if Assigned(filterItemList) then
      filterItemList.Clear()
    else
      filterItemList := filter.Root.AddItemList(fboOr);

    vColumn := gridView.GetColumnByFieldName('filter_route_transport_kind');
    if StoredFiltration.TransportKindBus then
      filterItemList.addItem(vColumn, foEqual, '�', '�');
    if StoredFiltration.TransportKindTrolley then
      filterItemList.addItem(vColumn, foEqual, '��', '��');
    if StoredFiltration.TransportKindTram then
      filterItemList.addItem(vColumn, foEqual, '��', '��');

    if filterItemList.IsEmpty then
      filterItemList.addItem(vColumn, foEqual, Null, '�����');

    if (StoredFiltration.TransportKindBus and
        StoredFiltration.TransportKindTrolley and
        StoredFiltration.TransportKindTram) then
      FreeAndNil(filterItemList);

    // ����������
    vFieldList.Clear();
    vFieldList.addItem('agency_muid', '');
    filterItemList := getFilterItemList(filter.Root, vFieldList);

    if Assigned(filterItemList) then
      filterItemList.Clear()
    else
      filterItemList := filter.Root.AddItemList(fboOr);

    vColumn := gridView.GetColumnByFieldName('agency_muid');
    if StoredFiltration.RouteIsMGT then
      filterItemList.addItem(vColumn, foEqual, '1', '1');
    if StoredFiltration.RouteIsCommercial then
      filterItemList.addItem(vColumn, foNotEqual, '1', '1');

    if filterItemList.IsEmpty then
      filterItemList.addItem(vColumn, foEqual, Null, '�����');

    if (StoredFiltration.RouteIsMGT and
        StoredFiltration.RouteIsCommercial) then
      FreeAndNil(filterItemList);

    // �������
    vFieldList.Clear();
    vFieldList.addItem('route_state2', '');
    filterItemList := getFilterItemList(filter.Root, vFieldList);
    if Assigned(filterItemList) then
      filterItemList.Clear()
    else
      filterItemList := filter.Root.AddItemList(fboOr);

    vColumn := gridView.GetColumnByFieldName('route_state2');
    if StoredFiltration.RouteStateForApproval then
      filterItemList.addItem(vColumn, foEqual, '�� �����������', '�� �����������');
    if StoredFiltration.RouteStateOpened then
      filterItemList.addItem(vColumn, foEqual, '������', '������');
    if StoredFiltration.RouteStateTempOpened then
      filterItemList.addItem(vColumn, foEqual, '�������� ������', '�������� ������');
    if StoredFiltration.RouteStateTempClosed then
      filterItemList.addItem(vColumn, foEqual, '�������� ������', '�������� ������');
    if StoredFiltration.RouteStateClosed then
      filterItemList.addItem(vColumn, foEqual, '������', '������');

    if filterItemList.IsEmpty then
      filterItemList.addItem(vColumn, foEqual, Null, '�����');

    if (StoredFiltration.RouteStateForApproval and
        StoredFiltration.RouteStateOpened and
        StoredFiltration.RouteStateTempOpened and
        StoredFiltration.RouteStateTempClosed and
        StoredFiltration.RouteStateClosed) then
      FreeAndNil(filterItemList);

    filter.Active := true;
  finally
    FreeAndNil(vFieldList);
  end;
end;

{**********************************************************************************************
* AddGridStopStoredFiltration
***********************************************************************************************}
procedure TMgtCore.AddGridStopStoredFiltration(aDataSet: TMgtDataset);
var
  filter: TcxDBDataFilterCriteria;
  filterItemList, filterItemListTmp: TcxFilterCriteriaItemList;
  vFieldList: TMapStrings;
  vColumn: TcxGridExtendedDBColumn;
  gridView : TcxGridExtendedDBTableView;

begin
  if not ((aDataSet.Alias = 'Stops') or (aDataSet.Alias = 'StopPlaces')) then
    exit;

  gridView := aDataSet.compGridView;
  filter := gridView.DataController.Filter;
  vFieldList := TMapStrings.Create();
  try
    vFieldList.addItem('has_bus', '');
    vFieldList.addItem('has_trolley', '');
    vFieldList.addItem('has_tram', '');
    vFieldList.addItem('has_mgt_routes', '');
    vFieldList.addItem('has_commercial_routes', '');
    filterItemList := getFilterItemList(filter.Root, vFieldList);

    if Assigned(filterItemList) then
      filterItemList.Clear()
    else
      filterItemList := filter.Root.AddItemList(fboOr);

    // �������������
    if StoredFiltration.StopPlaceInactive then
    begin
      filterItemListTmp := filterItemList.AddItemList(fboAnd);

      vColumn := gridView.GetColumnByFieldName('has_bus');
      filterItemListTmp.addItem(vColumn, foEqual, '0', '���');
      vColumn := gridView.GetColumnByFieldName('has_trolley');
      filterItemListTmp.addItem(vColumn, foEqual, '0', '���');
      vColumn := gridView.GetColumnByFieldName('has_tram');
      filterItemListTmp.addItem(vColumn, foEqual, '0', '���');
    end;

    // �����������
    if StoredFiltration.StopPlaceActive then
    begin
      with filterItemList.AddItemList(fboAnd) do
      begin
        // ���� ����������
        filterItemListTmp := AddItemList(fboOr);

        vColumn := gridView.GetColumnByFieldName('has_bus');
        if StoredFiltration.TransportKindBus  then
          filterItemListTmp.addItem(vColumn, foEqual, '1', '����');
        vColumn := gridView.GetColumnByFieldName('has_trolley');
        if StoredFiltration.TransportKindTrolley then
          filterItemListTmp.addItem(vColumn, foEqual, '1', '����');
        vColumn := gridView.GetColumnByFieldName('has_tram');
        if StoredFiltration.TransportKindTram then
          filterItemListTmp.addItem(vColumn, foEqual, '1', '����');

        if filterItemListTmp.IsEmpty then
          filterItemListTmp.addItem(vColumn, foEqual, Null, '�����');

        if (StoredFiltration.TransportKindBus and
          StoredFiltration.TransportKindTrolley and
          StoredFiltration.TransportKindTram) and
          (StoredFiltration.StopPlaceActive = StoredFiltration.StopPlaceInactive) then
          FreeAndNil(filterItemListTmp);

        // �����������
        filterItemListTmp := AddItemList(fboOr);
        vColumn := gridView.GetColumnByFieldName('has_mgt_routes');
        if StoredFiltration.RouteIsMGT then
          filterItemListTmp.addItem(vColumn, foEqual, '1', '1');
        vColumn := gridView.GetColumnByFieldName('has_commercial_routes');
        if StoredFiltration.RouteIsCommercial then
          filterItemListTmp.addItem(vColumn, foEqual, '1', '1');

        if filterItemListTmp.IsEmpty then
          filterItemListTmp.addItem(vColumn, foEqual, Null, '�����');

        if (StoredFiltration.RouteIsMGT and
          StoredFiltration.RouteIsCommercial) then
          FreeAndNil(filterItemListTmp);
      end;
    end;

    if (StoredFiltration.TransportKindBus and
        StoredFiltration.TransportKindTrolley and
        StoredFiltration.TransportKindTram and
        StoredFiltration.StopPlaceActive and
        StoredFiltration.StopPlaceInactive and
        StoredFiltration.RouteIsMGT and
        StoredFiltration.RouteIsCommercial) then
      FreeAndNil(filterItemList)
    else if not (StoredFiltration.StopPlaceActive or StoredFiltration.StopPlaceInactive) then
    begin
      filterItemList.Clear();
      vColumn := gridView.GetColumnByFieldName('has_bus');
      filterItemList.addItem(vColumn, foEqual, Null, '�����');
    end;

    filter.Active := true;
  finally
    FreeAndNil(vFieldList);
  end;
end;

{**********************************************************************************************
* AddGridRouteTrajectoryStoredFiltration
***********************************************************************************************}
procedure TMgtCore.AddGridRouteTrajectoryStoredFiltration(aDataSet: TMgtDataset);
var
  filter: TcxDBDataFilterCriteria;
  filterItemList: TcxFilterCriteriaItemList;
  vFieldList: TMapStrings;
  vColumn: TcxGridExtendedDBColumn;
  gridView : TcxGridExtendedDBTableView;

begin
  gridView := aDataSet.compGridView;
  filter := gridView.DataController.Filter;
  vFieldList := TMapStrings.Create();
  try
    // ��� ����������
    vFieldList.addItem('filter_route_transport_kind', '');
    filterItemList := getFilterItemList(filter.Root, vFieldList);

    if Assigned(filterItemList) then
      filterItemList.Clear()
    else
      filterItemList := filter.Root.AddItemList(fboOr);

    vColumn := gridView.GetColumnByFieldName('filter_route_transport_kind');
    if StoredFiltration.TransportKindBus then
      filterItemList.addItem(vColumn, foEqual, '�', '�');
    if StoredFiltration.TransportKindTrolley then
      filterItemList.addItem(vColumn, foEqual, '��', '��');
    if StoredFiltration.TransportKindTram then
      filterItemList.addItem(vColumn, foEqual, '��', '��');

    if filterItemList.IsEmpty then
      filterItemList.addItem(vColumn, foEqual, Null, '�����');

    if (StoredFiltration.TransportKindBus and
        StoredFiltration.TransportKindTrolley and
        StoredFiltration.TransportKindTram) then
      FreeAndNil(filterItemList);

    // ����������
    vFieldList.Clear();
    vFieldList.addItem('agency_muid', '');
    filterItemList := getFilterItemList(filter.Root, vFieldList);

    if Assigned(filterItemList) then
      filterItemList.Clear()
    else
      filterItemList := filter.Root.AddItemList(fboOr);

    vColumn := gridView.GetColumnByFieldName('agency_muid');
    if StoredFiltration.RouteIsMGT then
      filterItemList.addItem(vColumn, foEqual, '1', '1');
    if StoredFiltration.RouteIsCommercial then
      filterItemList.addItem(vColumn, foNotEqual, '1', '1');

    if filterItemList.IsEmpty then
      filterItemList.addItem(vColumn, foEqual, Null, '�����');

    if (StoredFiltration.RouteIsMGT and
        StoredFiltration.RouteIsCommercial) then
      FreeAndNil(filterItemList);

    // �����
    vFieldList.Clear();
    vFieldList.addItem('is_main_round', '');
    filterItemList := getFilterItemList(filter.Root, vFieldList);
    if Assigned(filterItemList) then
      filterItemList.Clear()
    else
      filterItemList := filter.Root.AddItemList(fboOr);

    vColumn := gridView.GetColumnByFieldName('is_main_round');
    if StoredFiltration.RoundMain then
      filterItemList.addItem(vColumn, foEqual, '1', '��');
    if StoredFiltration.RoundAdditional then
      filterItemList.addItem(vColumn, foEqual, '0', '���');

    if filterItemList.IsEmpty then
      filterItemList.addItem(vColumn, foEqual, Null, '�����');

    if (StoredFiltration.RoundMain and
        StoredFiltration.RoundAdditional) then
      FreeAndNil(filterItemList);

    // �������
    vFieldList.Clear();
    vFieldList.addItem('route_state2', '');
    filterItemList := getFilterItemList(filter.Root, vFieldList);
    if Assigned(filterItemList) then
      filterItemList.Clear()
    else
      filterItemList := filter.Root.AddItemList(fboOr);

    vColumn := gridView.GetColumnByFieldName('route_state2');
    if StoredFiltration.RouteStateForApproval then
      filterItemList.addItem(vColumn, foEqual, '�� �����������', '�� �����������');
    if StoredFiltration.RouteStateOpened then
      filterItemList.addItem(vColumn, foEqual, '������', '������');
    if StoredFiltration.RouteStateTempOpened then
      filterItemList.addItem(vColumn, foEqual, '�������� ������', '�������� ������');
    if StoredFiltration.RouteStateTempClosed then
      filterItemList.addItem(vColumn, foEqual, '�������� ������', '�������� ������');
    if StoredFiltration.RouteStateClosed then
      filterItemList.addItem(vColumn, foEqual, '������', '������');

    if filterItemList.IsEmpty then
      filterItemList.addItem(vColumn, foEqual, null, '�����');

    if (StoredFiltration.RouteStateForApproval and
        StoredFiltration.RouteStateOpened and
        StoredFiltration.RouteStateTempOpened and
        StoredFiltration.RouteStateTempClosed and
        StoredFiltration.RouteStateClosed) then
      FreeAndNil(filterItemList);

    // ��������
    vFieldList.Clear();
    vFieldList.addItem('variant_state', '');
    filterItemList := getFilterItemList(filter.Root, vFieldList);
    if Assigned(filterItemList) then
      filterItemList.Clear()
    else
      filterItemList := filter.Root.AddItemList(fboOr);

    vColumn := gridView.GetColumnByFieldName('variant_state');
    if StoredFiltration.VariantActive then
      filterItemList.addItem(vColumn, foEqual, '�����������', '�����������');
    if StoredFiltration.VariantPlanned then
      filterItemList.addItem(vColumn, foEqual, '�����������', '�����������');
    if StoredFiltration.VariantArchive then
      filterItemList.addItem(vColumn, foEqual, '��������', '��������');

    if filterItemList.IsEmpty then
      filterItemList.addItem(vColumn, foEqual, null, '�����');

    if (StoredFiltration.VariantActive and
        StoredFiltration.VariantPlanned and
        StoredFiltration.VariantArchive) then
      FreeAndNil(filterItemList);

    filter.Active := true;
  finally
    FreeAndNil(vFieldList);
  end;
end;

{**********************************************************************************************
* AddGridRouteNullTrajectoryStoredFiltration
***********************************************************************************************}
procedure TMgtCore.AddGridRouteNullTrajectoryStoredFiltration();
var
  filter: TcxDBDataFilterCriteria;
  filterItemList: TcxFilterCriteriaItemList;
  vFieldList: TMapStrings;
  vColumn: TcxGridExtendedDBColumn;
  gridView : TcxGridExtendedDBTableView;

begin
  gridView := mgtDatasets[ 'RouteNullTrajectories' ].compGridView;
  filter := gridView.DataController.Filter;
  vFieldList := TMapStrings.Create();
  try
    // ��� ����������
    vFieldList.addItem('filter_route_transport_kind', '');
    filterItemList := getFilterItemList(filter.Root, vFieldList);

    if Assigned(filterItemList) then
      filterItemList.Clear()
    else
      filterItemList := filter.Root.AddItemList(fboOr);

    vColumn := gridView.GetColumnByFieldName('filter_route_transport_kind');
    if StoredFiltration.RoundNull then
    begin
      if StoredFiltration.TransportKindBus then
        filterItemList.addItem(vColumn, foEqual, '�', '�');
      if StoredFiltration.TransportKindTrolley then
        filterItemList.addItem(vColumn, foEqual, '��', '��');
      if StoredFiltration.TransportKindTram then
        filterItemList.addItem(vColumn, foEqual, '��', '��');
    end;

    if filterItemList.IsEmpty then
      filterItemList.addItem(vColumn, foEqual, null, '�����');

    if (StoredFiltration.TransportKindBus and
        StoredFiltration.TransportKindTrolley and
        StoredFiltration.TransportKindTram and
        StoredFiltration.RoundNull) then
      FreeAndNil(filterItemList);

    // ����������
    vFieldList.Clear();
    vFieldList.addItem('agency_muid', '');
    filterItemList := getFilterItemList(filter.Root, vFieldList);

    if Assigned(filterItemList) then
      filterItemList.Clear()
    else
      filterItemList := filter.Root.AddItemList(fboOr);

    vColumn := gridView.GetColumnByFieldName('agency_muid');
    if StoredFiltration.RouteIsMGT then
      filterItemList.addItem(vColumn, foEqual, '1', '1');
    if StoredFiltration.RouteIsCommercial then
      filterItemList.addItem(vColumn, foNotEqual, '1', '1');

    if filterItemList.IsEmpty then
      filterItemList.addItem(vColumn, foEqual, null, '�����');

    if (StoredFiltration.RouteIsMGT and
        StoredFiltration.RouteIsCommercial) then
      FreeAndNil(filterItemList);

    // �������
    vFieldList.Clear();
    vFieldList.addItem('route_state2', '');
    filterItemList := getFilterItemList(filter.Root, vFieldList);
    if Assigned(filterItemList) then
      filterItemList.Clear()
    else
      filterItemList := filter.Root.AddItemList(fboOr);

    vColumn := gridView.GetColumnByFieldName('route_state2');
    if StoredFiltration.RouteStateForApproval then
      filterItemList.addItem(vColumn, foEqual, '�� �����������', '�� �����������');
    if StoredFiltration.RouteStateOpened then
      filterItemList.addItem(vColumn, foEqual, '������', '������');
    if StoredFiltration.RouteStateTempOpened then
      filterItemList.addItem(vColumn, foEqual, '�������� ������', '�������� ������');
    if StoredFiltration.RouteStateTempClosed then
      filterItemList.addItem(vColumn, foEqual, '�������� ������', '�������� ������');
    if StoredFiltration.RouteStateClosed then
      filterItemList.addItem(vColumn, foEqual, '������', '������');

    if filterItemList.IsEmpty then
      filterItemList.addItem(vColumn, foEqual, null, '�����');

    if (StoredFiltration.RouteStateForApproval and
        StoredFiltration.RouteStateOpened and
        StoredFiltration.RouteStateTempOpened and
        StoredFiltration.RouteStateTempClosed and
        StoredFiltration.RouteStateClosed) then
      FreeAndNil(filterItemList);

    // ��������
    vFieldList.Clear();
    vFieldList.addItem('variant_state', '');
    filterItemList := getFilterItemList(filter.Root, vFieldList);
    if Assigned(filterItemList) then
      filterItemList.Clear()
    else
      filterItemList := filter.Root.AddItemList(fboOr);

    vColumn := gridView.GetColumnByFieldName('variant_state');
    if StoredFiltration.VariantActive then
      filterItemList.addItem(vColumn, foEqual, '�����������', '�����������');
    if StoredFiltration.VariantPlanned then
      filterItemList.addItem(vColumn, foEqual, '�����������', '�����������');
    if StoredFiltration.VariantArchive then
      filterItemList.addItem(vColumn, foEqual, '��������', '��������');

    if filterItemList.IsEmpty then
      filterItemList.addItem(vColumn, foEqual, null, '�����');

    if (StoredFiltration.VariantActive and
        StoredFiltration.VariantPlanned and
        StoredFiltration.VariantArchive) then
      FreeAndNil(filterItemList);

    filter.Active := true;
  finally
    FreeAndNil(vFieldList);
  end;
end;

{**********************************************************************************************
* AddTOStoredFiltration
***********************************************************************************************}
procedure TMgtCore.AddTOStoredFiltration(aDataSet: TMgtDataset);
begin
  AddTOStoredFiltration( aDataSet.compGridView, aDataSet.Alias = 'SPTasks');
end;

{**********************************************************************************************
* AddTOStoredFiltration
***********************************************************************************************}
procedure TMgtCore.AddTOStoredFiltration(aGridView: TcxGridExtendedDBTableView; aFlStringFilter: boolean);
var
  i: ETDO;
  filter: TcxDBDataFilterCriteria;
  filterItemList: TcxFilterCriteriaItemList;
  vFieldList: TMapStrings;
  vColumn, vColumnZel: TcxGridExtendedDBColumn;
  vAllChecked: boolean;
begin
  if not Assigned(aGridView) then
    exit;

  filter := aGridView.DataController.Filter;
  vFieldList := TMapStrings.Create();
  try
    // ����� ��
    vFieldList.addItem('TDO_filter', '');
    filterItemList := getFilterItemList(filter.Root, vFieldList);

    if Assigned(filterItemList) then
      filterItemList.Clear()
    else
      filterItemList := filter.Root.AddItemList(fboOr);

    vAllChecked := true;
    vColumn := aGridView.GetColumnByFieldName('TDO_filter');
    vColumnZel := aGridView.GetColumnByFieldName('schedules_zel_approved');
    for i := Low(ETDO) to High(ETDO) do
      if StoredFiltration.CheckedTO[i] then
      begin
        if aFlStringFilter then
        begin
          filterItemList.addItem(vColumn, foLike, '%' + mgtTDOFilterCode[i] + '%', mgtTDOShortName[i]);

          if (i = tdoZelAK) and Assigned(vColumnZel) then
            filterItemList.addItem(vColumnZel, foEqual, '0', '�� �����������');
        end
        else
          filterItemList.addItem(vColumn, foEqual, Integer(i), mgtTDOShortName[i]);
      end
      else
        vAllChecked := false;
    
    if vAllChecked or filterItemList.IsEmpty() then
      FreeAndNil(filterItemList);

    filter.Active := true;
  finally
    FreeAndNil(vFieldList);
  end;
end;

{*******************************************************************************
* createJsonsAllTrafaretsInTasks
*******************************************************************************}
procedure TMgtCore.createJsonsAllTrafaretsInTasks();
var
  sql : string;
  dbRes, dbResBySignpost : TDBResult;
  n, i, l : integer;
  taskMuid : int64;
  task : TSPTask;
  jsonList : TStringList;
  signpostInTask : TspSignpostInTask;
  json : string;
begin
  sql := 'SELECT t.muid FROM gis_mgt.tasks t WHERE t.sign_deleted = 0 AND t.is_declined = 0';
  n := 1;
  if FConn.QueryOpen(sql, dbRes, true) <> 0 then
    raise EMgtException.Create('�� ������� ������� ������ �������');

  try
    while dbRes.Fetch() do
    begin
      showPopupMessage( '�������� Json. �����...' + IntToStr(n) + ' �� ' + IntToStr( dbRes.numRows() ) );
      Inc(n);

      taskMuid := dbRes.asInt64(0);
      task := TSPTask.Create( taskMuid, oEdit, nil );
      jsonList := TStringList.Create();

      try
        try
          task.load();

          for i := 0 to task.Signposts.Count - 1 do
          begin
            signpostInTask := TspSignpostInTask(task.Signposts.items[i]);

            CreateTrafaretModels( task, signpostInTask, jsonList );

            sql := 'SELECT asldc.muid, asldc.json FROM gis_mgt_attachments.attachments_signposts_layouts_data_cdr asldc WHERE asldc.task_signpost_muid = ' + IntToStr( signpostInTask.lnkMuid );

            if FConn.QueryOpen(sql, dbResBySignpost, true) <> 0 then
                raise EMgtException.Create('�� ������� ������� ������ ����������');

            try
              l := 0;
              while dbResBySignpost.Fetch() do
              begin

                if dbResBySignpost.isNotNull(1) then
                  continue;

                if jsonList.Count > l then
                  json := jsonList[l]
                else
                  json := 'null';  

                sql := 'UPDATE gis_mgt_attachments.attachments_signposts_layouts_data_cdr'
                       + ' asldc SET asldc.json = ' +  UTF8Decode( FConn.quoteValue( json ) )
                       + ' WHERE asldc.muid = ' + dbResBySignpost.asString(0);

                if FConn.QueryExec(sql, false) <> 0 then
                  continue;

                Inc(l);
              end;
            finally

              FreeAndNil( dbResBySignpost );
            end;
          end;
        finally
          FreeAndNil( task );
          FreeAndNil( jsonList );
        end;
      except
      end;
    end;
  finally
    hidePopupMessage();
    FreeAndNil( dbRes );
  end;
end;

{*******************************************************************************
* GetDaysCaption
* �������� ��������� ������������� ���� ������
* aDays - ������ ������������ ���� ������ ����: '1,2,3,4,5,6,7' (�� Mysql)
*******************************************************************************}
function TMgtCore.GetDaysCaption(aDaysStr: string ): string;
const
  captionsFull  : Array [0..7] of String = ('', '�����������', '�������', '�����', '�������', '�������', '�������', '�����������');
  captionsShort : Array [0..7] of String = ('', '��', '��', '��', '��', '��', '��', '��');
var
  list : TStringList;
  i : integer;

begin
  Result := '������������';

  if aDaysStr = '' then
    exit;

  if aDaysStr = '1,2,3,4,5,6,7' then
    Result := '������'
  else if aDaysStr = '1,2,3,4,5' then
    Result := '�����'
  else if aDaysStr = '6,7' then
    Result := '��������'
  else
  begin
    list := TstringList.Create;
    list.CommaText := aDaysStr;
    try
      if list.Count = 1 then
      begin
        Result := captionsFull[ StrToInt( List[0] ) ];
        Exit;
      end;

      Result := captionsShort[ StrToInt( list[0] ) ];
      for i := 1 to list.Count -1 do
      begin
        // ��������� ����
        if ( i = list.Count -1 ) then
        begin
          if ( StrToInt( list[i] ) = StrToInt( list[i-1] ) + 1 ) then
            Result := Result + '-' + captionsShort[ StrToInt( list[i] ) ]
          else
            Result := Result + ', ' + captionsShort[ StrToInt( list[i] ) ];

          Exit;
        end;

        // ���������� ���� ������ �� 1 � ��������� ������ �� 1
        if ( StrToInt( list[i] ) = StrToInt( list[i-1] ) + 1 )
          and ( StrToInt( list[i] ) = StrToInt( list[i+1] ) - 1 ) then
        begin
          Continue;
        end
         // ���������� ���� ������ �� 1, � ��������� �� ����� +1 � ���������
        else if ( StrToInt( list[i] ) = StrToInt( list[i-1] ) + 1 )
              and ( StrToInt( list[i] ) <> StrToInt( list[i+1] ) - 1 ) then
        begin
          if i < 2 then
            Result := Result + '-' + captionsShort[ StrToInt( list[i] ) ]
          // else
          // if ( StrToInt( list[i] ) = StrToInt( list[i-1] ) + i ) then
          //  Result := Result + ', ' + captionsShort[ StrToInt( list[i] ) ]
          else
            Result := Result + '-' + captionsShort[ StrToInt( list[i] ) ];
        end
         // ���������� �� ����� -1 � ���������, � ��������� ������ �� 1
        else if ( StrToInt( list[i] ) <> StrToInt( list[i-1] ) + 1 )
              and ( StrToInt( list[i] ) = StrToInt( list[i+1] ) - 1 ) then
        begin
          Result := Result + ', '  + captionsShort[ StrToInt( list[i] ) ];
        end
         // ���������� �� ����� -1 � ��������� � ��������� �� ����� +1 � ���������
        else if ( StrToInt( list[i] ) <> StrToInt( list[i-1] ) + 1 )
              and ( StrToInt( list[i] ) <> StrToInt( list[i+1] ) - 1 ) then
        begin
          Result := Result + ', ' + captionsShort[ StrToInt( list[i] ) ];
        end;
      end;
    finally
      FreeAndNil( list );
    end;
  end;
end;

{**********************************************************************************************
* varIsValid
***********************************************************************************************}
function TMgtCore.varIsValid(aVal : Variant; aAllowBlank: boolean): boolean;
begin
  Result := not ( VarIsNull(aVal) or VarIsEmpty(aVal) or ((VarToStr(aVal) = '') and not aAllowBlank));
end;

{**********************************************************************************************
* blendColors
***********************************************************************************************}
function TMgtCore.blendColors(Color1, Color2: TColor; proportion: Byte): TColor;
var
  c1, c2: Longint;
  r, g, b, v1, v2: byte;
begin
  proportion:= Round(2.55 * proportion);
  c1 := ColorToRGB(Color1);
  c2 := ColorToRGB(Color2);
  v1:= GetRValue(c1);
  v2:= GetRValue(c2);
  if v1 >= v2 then
    r:= proportion * (v1 - v2) shr 8 + v2
  else
    r:= v2 - (abs(proportion * (v1 - v2)) shr 8);

  v1:= GetGValue(c1);
  v2:= GetGValue(c2);
  if v1 >= v2 then
    g:= proportion * (v1 - v2) shr 8 + v2
  else
    g:= v2 - (abs(proportion * (v1 - v2)) shr 8);

  v1:= GetBValue(c1);
  v2:= GetBValue(c2);
  if v1 >= v2 then
    b:= proportion * (v1 - v2) shr 8 + v2
  else
    b:= v2 - (abs(proportion * (v1 - v2)) shr 8);
  Result := (b shl 16) + (g shl 8) + r;
end;

{**********************************************************************************************
* getUserFIO
***********************************************************************************************}
function TMgtCore.getUserFIO(aUserMuid: int64; aFullName: boolean): string;
var
  vUser: TUser;
begin
  Result := '';

  vUser := TUser(AdminEngine.Admin.users.GetObject(aUserMuid));
  if vUser = nil then
    exit;

  if aFullName then
    Result := vUser.GetFullName()
  else
    Result := vUser.GetFIO();
end;

{*******************************************************************************
* getInstallerFIO
*******************************************************************************}
function TMgtCore.getInstallerFIO( aInstallerMuid : int64; aFullName : boolean ) : string;
var
  sql : string;
  dbRes : TDBResult;
begin
  Result := '';

  sqlParams.Clear();
  sqlParams.addItem( 'muid', IntToStr( aInstallerMuid ) );

  sql := getCoreSQL( 'GetInstallerFIO', sqlParams );

  if Conn.QueryOpen(sql, dbRes, false) <> 0 then
    raise EMgtException.Create('������ ��� ��������� �������� �� ��.' + #13#10 + sql);

  try
    while dbRes.Fetch() do
    begin
      Result := dbRes.asString( 0 ) + ' ';

      if aFullName then
      begin
        Result := Result + dbRes.asString(1) + ' ' + dbRes.asString(2);
        break;
      end;

      Result := Result + dbRes.asString(1)[1] + '. ';
      Result := Result + dbRes.asString(2)[1] + '.';
    end;
  finally
    FreeAndNil( dbRes );
  end;
end;

{**********************************************************************************************
* getBlobString
***********************************************************************************************}
function TMgtCore.getBlobString(vStream: TMemoryStream; vFieldName: string): string;
begin
  Result := '';
  if (vStream = nil) then
    exit;

  SetLength(Result, vStream.size);
  vStream.Position := 0;
  vStream.Read(Pointer(Result)^, vStream.size);
  FConn.EscapeBlob(Result, PChar(Result), vStream.Size, vFieldName);
end;

{**********************************************************************************************
* loadLayoutByPath
***********************************************************************************************}
procedure TMgtCore.loadLayoutByPath(aCDRPath, aPreviewPath: string; var vCDR: TMemoryStream; var vPreview: TdxPNGImage);
begin
  if aCDRPath = '' then
    exit;
    
  if aPreviewPath = '' then
  begin
    if Assigned(generator) then
    begin
      generator.openDocument(aCDRPath);
      generator.exportAsPNG(core.getTempDirectory('\!generator') + '\temp.png');
      aPreviewPath := core.getTempDirectory('\!generator') + '\temp.png';
    end
    else
      aPreviewPath := ExtractFileDir(ParamStr(0)) + BLANK_PREVIEW;
  end;

  vCDR.LoadFromFile(aCDRPath);
  vPreview.LoadFromFile(aPreviewPath);
end;

{**********************************************************************************************
* getTempDirectory
***********************************************************************************************}
function TMgtCore.getTempDirectory(aSubdirectory: string): string;
var
  vAppInfo: TAppInfo;
begin
  Result := '';
  vAppInfo := TAppInfo.GetAppInfo();
  if not GetTempDir(Result) then
    Result := vAppInfo.exeDirName;

  Result := ExcludeTrailingPathDelimiter(Result) + aSubdirectory;
  ForceDirectories(Result);
end;

{**********************************************************************************************
* openDirectory
***********************************************************************************************}
procedure TMgtCore.openDirectory(aDirectory: string);
begin
  ShellExecute(0, nil, 'explorer.exe', PChar(aDirectory), nil, SW_SHOWNORMAL);
end;

{**********************************************************************************************
* getTasksMessage
// �������� ���-�� ������� �� ������������ ����������, ��������� ������� ������������
***********************************************************************************************}
function TMgtCore.getUserTaskCount(): integer;
var
  sql : string;
  dbRes: TDbResult;
  tdo : ETDO;
  // ������ �������� �������, ������� �������� ��� ��������� ������ ������������ (� ������������ � �������)
  statuses : TMapStrings;
  // ������ ���, ��������� ������� �������� ������� ������������
  tdos : TMapStrings;
  canApproveRoutesCondition, canApproveSchedulesCondition, canApproveSignpostsCondition : string;
  canSendToDLSCondition: string;
  tdoCondition, statusCondition, operatorCondition : string;

begin
  Result := 0;

  if (conn = nil) or (not conn.flConnected) then
    exit;

  // �� ������ ������������ ��������� ������� ��� sql-�������
  statuses := TMapStrings.Create(mcReplace);
  tdos := TMapStrings.Create(mcReplace);

  operatorCondition := '1';
  canApproveRoutesCondition := '0';
  canApproveSchedulesCondition := '0';
  canApproveSignpostsCondition := '0';
  canSendToDLSCondition := '0';

//  ����� ��������� ��������
//  ������ = 2 (�� ������������) � �� ���������� ��������
  if FUserPermissions.Signposts.flApproveRoutesMGT or
     FUserPermissions.Signposts.flApproveRoutesComm then
  begin
    statuses.addItem('2', '');
    canApproveRoutesCondition := '(routes_approved = 0)';
  end;

//  ����� ��������� ����������
//  ������ = 2 (�� ������������) � �� ���������� ����������
//  TODO: ����� ������ ����� ������� ������ ��� ����������, ����� �������������� �� �������� ����������,
//        � ���������� - �������������� 
  if FUserPermissions.Signposts.flApproveSchedulesMSC or
     FUserPermissions.Signposts.flApproveSchedulesZelAK or
     FUserPermissions.Signposts.flApproveSchedulesComm then
  begin
    statuses.addItem('2', '');
    canApproveSchedulesCondition := '(schedules_approved = 0)';
  end;

//  ����� ��������� ��������� ������, ����������� ���������� ������
//  ������ = 2 (�� ������������) � � ������ ���� ��������� ������� ���, ������� �� ����������
//  ��� ������ = 9 (��������) � � ������ ���� ��������� ������� ���, ������� �� ������������
  for tdo := Low(ETDO) to High(ETDO) do
  begin
    if tdo = tdoUndefined then
      continue;

    statuses.addItem('2', '');
    statuses.addItem('9', '');

//�������� ��������, ����� ������������ ������ �� ������, ������� ���������
//� ������� ����������� ��� � ���� �� ���� �� ��������� �� ���������� ��� ���� �� �� ����� ��� ������������� ���������� ������
    canApproveSignpostsCondition := '(signposts_approved = 0)';

    if FUserPermissions.Signposts.flApproveSignpostsByTDO[tdo] then
      tdos.addItem(IntToStr( Integer(tdo) ), '');
  end;

//  ����� ��������� � ���
//  ������ = 2 (�� ������������) � ��� ����������
  if FUserPermissions.Signposts.flSendTasksToDLS then
  begin
    statuses.addItem('2', '');
    canSendToDLSCondition := '( (routes_approved = 1) AND (schedules_approved = 1) AND (signposts_approved = 1) )'
  end;

//  ����� ��������� ���������
//  ������ = 3 (c����������)
  if FUserPermissions.Signposts.flAssignDlsOperator then
  begin
    statuses.addItem('3', '');
  end;

//  ����� ��������� �������
//  ������ = 4 (���������� �������) � operator_muid = ������� ������������
  if FUserPermissions.Signposts.flLayoutPreparing then
  begin
    statuses.addItem('4', '');
    operatorCondition := '(t.operator_muid = ' + IntToStr(FAuth.User.MUID) + ')';
  end;

//  ����� ��������� �� ������ - ... - �������� ����� �����������
//  ������ = 5, 6, 7, 8 (����� � ������ - ... - ����� ��� �������) � operator_muid = ������� ������������
  if FUserPermissions.Signposts.flInstalling then
  begin
    statuses.addItem('5', '');
    statuses.addItem('6', '');
    statuses.addItem('7', '');
    statuses.addItem('8', '');

    operatorCondition := '(t.operator_muid = ' + IntToStr(FAuth.User.MUID) + ')';
  end;

  statusCondition := '0';
  if statuses.Count > 0 then
    statusCondition := '(ts.muid IN (' + statuses.CommaText + '))';

  tdoCondition := '0';
  if tdos.Count > 0 then
    tdoCondition := '(tdo.number IN (' + tdos.CommaText + '))';

  // ��������� ��������� sql-�������
  FsqlParams.Clear();
  FsqlParams.addItem('statusCondition', statusCondition);
  FsqlParams.addItem('tdoCondition', tdoCondition);
  FsqlParams.addItem('operatorCondition', operatorCondition);
  FsqlParams.addItem('canApproveRoutesCondition', canApproveRoutesCondition);
  FsqlParams.addItem('canApproveSchedulesCondition', canApproveSchedulesCondition);
  FsqlParams.addItem('canApproveSignpostsCondition', canApproveSignpostsCondition);
  FsqlParams.addItem('canSendToDLSCondition', canSendToDLSCondition);

  sql := getCoreSQL('getUserTaskCount', FsqlParams);

  if (conn.QueryOpen(sql, dbRes, true) < 0) then
    exit;

  Result := dbRes.numRows;

  dbRes.Free();

  statuses.Free();
  tdos.Free();
end;

{**********************************************************************************************
* loadRouteSchedules
// ��������� ���������� �� �������� (��� ������ ������ �������� ��)
***********************************************************************************************}
function TMgtCore.loadRouteSchedules(aRouteErmID: integer; aFullReload: boolean): boolean;
var
  url, dir, fileName: string;
  contentText, timeSecs, msg, msg2 : string;
  errorCount, res : integer;
  oStream: TFileStream;
  flSuccess: boolean;

begin
  Result := false;

  if aRouteErmID <= 0 then
    exit;

  try
    dir := killTrailingSlash(ExtractFileDir(ParamStr(0))) + '\' + EXPPARAMS_FILES_DIR;
    ForceDirectories(dir);

    fileName := dir + '\' + 'route_erm_id_' + IntToStr(aRouteErmID) + '__'
        + FormatDateTime('yyyy_mm_dd__hh_nn_ss', Now()) + '.html';
    ostream := TFileStream.Create(fileName, fmCreate);

    url := 'http://' + expParamsServiceHost + ':' + IntToStr(expParamsServicePort)
                      + '/load?route_erm_id=' + IntToStr(aRouteErmID)
                      + '&reload=' + IntToStr( Integer(aFullReload) );

    showPopupMessage('�������� ����������...');

    try
      httpClient.Get(URL, ostream);
      httpClient.Disconnect();
    finally
      hidePopupMessage();
    end;

    if (httpClient.ResponseCode = 200) then
    begin
      msg := httpClient.Response.RawHeaders.Values['_success'];
      flSuccess := Boolean(StrToIntDef(msg, 0));

      msg := httpClient.Response.RawHeaders.Values['_errorCount'];
      errorCount := StrToIntDef(msg, 0);

      timeSecs := httpClient.Response.RawHeaders.Values['_time'];

      msg := '�������� ���������� ���������.'
            + #13#10 + '������ � �����������: ' + IntToStr(errorCount) + '.';

      msg2 := '����� ��������: ' + timeSecs + '.';

      if flSuccess and (errorCount > 0) then
      begin
        // ������ ���� � ��������, ��������� (���������) ���
        FreeAndNil(oStream);

        msg := msg + #13#10 + '������� ���� � ��������?';
        res := showDialog(dtInfo, dbsYesNo, msg, msg2);

        if (res = IDYES) then
        begin
          ShellExecute(0, 'open', PChar(fileName), nil, nil, SW_SHOWNORMAL) ;
        end;
      end
      else
      begin
        // html-����� � �������� � ������ ���, ���� ���������, ��������� ���, ������� ����
        SetLength(contentText, oStream.Size);
        oStream.Position := 0;
        oStream.Read(Pointer(contentText)^, oStream.Size);

        FreeAndNil(oStream);
        DeleteFile(fileName);

        if flSuccess then
        begin
          showDialog(dtInfo, dbsOK, msg, msg2 + #13#10 + contentText);
        end
        else
        begin
          showDialog(dtError, dbsOK, '������ ��� �������� ����������', msg2 + #13#10 + contentText);
        end;
      end;

      Result := flSuccess and (errorCount = 0);
    end
    else
    begin
      SetLength(contentText, oStream.Size);
      oStream.Position := 0;
      oStream.Read(Pointer(contentText)^, oStream.Size);

      msg2 := '�������� ���: ' + IntToStr(httpClient.ResponseCode)
          + #13#10 + httpClient.ResponseText;

      showDialog(dtError, dbsOK, '������ ��� �������� ����������', url + #13#10 + msg2 + #13#10 + contentText);

      FreeAndNil(oStream);
      DeleteFile(fileName);
    end;

  except
    on e: Exception do
    begin
      msg2 := '�������� ���: ' + IntToStr(httpClient.ResponseCode)
          + #13#10 + httpClient.ResponseText;

      if e.Message <> '' then
        msg2 := msg2 + '. ���������: ' + e.message;

      showDialog(dtError, dbsOK, '������ ��� �������� ����������', url + #13#10 + msg2);
      FreeAndNil(ostream);
      DeleteFile(fileName);
    end;
  end;
end;

{**********************************************************************************************
* setNullRoundParkAsStopPlace
***********************************************************************************************}
procedure TMgtCore.setNullRoundParkAsStopPlace();
var
  sql: string;
  dbRes: TDBResult;
  MOS: TMapObjectStructure;
  stopPlaces: TStrings;
  MosList: TLayerObjectIdentificationList;
  x, y: double;
  i: integer;
begin
  stopPlaces := TStringList.Create();
  MosList := TLayerObjectIdentificationList.Create();

  sql := 'SELECT sp.muid FROM gis_mgt.stop_places sp ' +
         'WHERE sp.sign_deleted = 0 AND sp.is_technical = 1 AND park_zone_muid IS NOT NULL';

  if (conn.QueryOpen(sql, dbRes, true) <> 0) then
    raise EMgtException.Create('������ ��� ��������� �������� �� ��.' + #13#10 + sql);

  while dbRes.Fetch() do
    stopPlaces.add(dbRes.asString(0));

  FreeAndNil(dbRes);

  MOS := TMapObjectStructure.Create();

  sql := 'SELECT rnr.muid, rt.muid, IF(rnr.park_1_muid IS NOT NULL, 1, 2) FROM gis_mgt.route_null_rounds rnr INNER JOIN gis_mgt.route_trajectories rt ' +
         'ON rt.route_null_round_muid = rnr.muid ' +
         'WHERE rnr.sign_deleted = 0 AND rt.sign_deleted = 0 AND ' +
         '((rnr.park_1_muid IS NOT NULL AND rnr.stop_place_A_muid IS NULL) OR (rnr.park_2_muid IS NOT NULL AND rnr.stop_place_B_muid IS NULL))';

  if (conn.QueryOpen(sql, dbRes, true) <> 0) then
    raise EMgtException.Create('������ ��� ��������� �������� �� ��.' + #13#10 + sql);

  while dbRes.Fetch() do
  begin
    mapCore.Mappl.ReadMapObject(MOS, mgtDatasources[ 'RouteTrajectories' ].layerCode, dbRes.asInt64(1), idtMUID);

    if (dbRes.asInteger(2) = 1) then
    begin
      x := mos.Vertex[0, 0].X;
      y := mos.Vertex[0, 0].Y;
    end
    else
    begin
      x := mos.Vertex[mos.SubObjectsCount - 1, mos.VertexesCount[mos.SubObjectsCount - 1] - 1].X;
      y := mos.Vertex[mos.SubObjectsCount - 1, mos.VertexesCount[mos.SubObjectsCount - 1] - 1].Y;
    end;

    MosList.Clear();
    mapCore.Mappl.GetLayerObjectsInMapPoint( mgtDatasources[ 'StopPlaces' ].layerCode, x, y, MOSList, 250000);

    i := 0;

    while i < MosList.Count do
      if stopPlaces.IndexOf(IntToStr(MOSList[i].oMUID)) < 0 then
        MosList.Delete(i)
      else
        inc(i);

    if (MosList.Count = 1) then
    begin
      if (dbRes.asInteger(2) = 1) then
        sql := 'stop_place_A_muid'
      else
        sql := 'stop_place_B_muid';

      sql := 'UPDATE gis_mgt.route_null_rounds rnr SET ' + sql + ' = ' + IntToStr(MosList[0].oMuid) +
             ' WHERE MUID = ' + dbRes.asString(0);

      if (conn.QueryExec(sql) <> 0) then
        raise EMgtException.Create('������ ��� ��������� �������� �� ��.' + #13#10 + sql);
    end;
  end;

  FreeAndNil(dbRes);
  FreeAndNil(MOS);
  FreeAndNil(stopPlaces);
end;

{**********************************************************************************************
* setNullRoundStopPlace
***********************************************************************************************}
procedure TMgtCore.setNullRoundStopPlace();
var
  sql: string;
  dbRes: TDBResult;
  MOS: TMapObjectStructure;
  MosList: TLayerObjectIdentificationList;
  x, y: double;
  i: integer;
begin
  MosList := TLayerObjectIdentificationList.Create();
  MOS := TMapObjectStructure.Create();

  sql := 'SELECT rnr.muid, rt.muid, IF(rnr.stop_place_A_muid IS NULL, rnr.stop_place_1_muid, NULL), ' +
         'IF(rnr.stop_place_B_muid IS NULL, rnr.stop_place_2_muid, NULL) ' +
         'FROM gis_mgt.route_null_rounds rnr INNER JOIN gis_mgt.route_trajectories rt ' +
         'ON rt.route_null_round_muid = rnr.muid ' +
         'WHERE rnr.sign_deleted = 0 AND rt.sign_deleted = 0 AND ' +
         '((rnr.stop_place_1_muid IS NOT NULL AND rnr.stop_place_A_muid IS NULL) OR ' +
          '(rnr.stop_place_2_muid IS NOT NULL AND rnr.stop_place_B_muid IS NULL))';

  if (conn.QueryOpen(sql, dbRes, true) <> 0) then
    raise EMgtException.Create('������ ��� ��������� �������� �� ��.' + #13#10 + sql);

  while dbRes.Fetch() do
  begin
    mapCore.Mappl.ReadMapObject(MOS, mgtDatasources[ 'RouteTrajectories' ].layerCode, dbRes.asInt64(1), idtMUID);

    if (dbRes.isNotNull(2)) then
    begin
      x := mos.Vertex[0, 0].X;
      y := mos.Vertex[0, 0].Y;

      MosList.Clear();
      mapCore.Mappl.GetLayerObjectsInMapPoint( mgtDatasources[ 'StopPlaces' ].layerCode, x, y, MOSList, 300000);

      for i := 0 to MosList.Count - 1 do
        if MOSList[i].oMUID = dbRes.asInt64(2) then
        begin
          sql := 'UPDATE gis_mgt.route_null_rounds rnr SET stop_place_A_muid = ' + dbRes.asString(2) +
                 ' WHERE MUID = ' + dbRes.asString(0);

          if (conn.QueryExec(sql) <> 0) then
            raise EMgtException.Create('������ ��� ��������� �������� �� ��.' + #13#10 + sql);
        end;
    end;

    if (dbRes.isNotNull(3)) then
    begin
      x := mos.Vertex[mos.SubObjectsCount - 1, mos.VertexesCount[mos.SubObjectsCount - 1] - 1].X;
      y := mos.Vertex[mos.SubObjectsCount - 1, mos.VertexesCount[mos.SubObjectsCount - 1] - 1].Y;

      MosList.Clear();
      mapCore.Mappl.GetLayerObjectsInMapPoint( mgtDatasources[ 'StopPlaces' ].layerCode, x, y, MOSList, 300000);

      for i := 0 to MosList.Count - 1 do
        if MOSList[i].oMUID = dbRes.asInt64(3) then
        begin
          sql := 'UPDATE gis_mgt.route_null_rounds rnr SET stop_place_B_muid = ' + dbRes.asString(3) +
                 ' WHERE MUID = ' + dbRes.asString(0);

          if (conn.QueryExec(sql) <> 0) then
            raise EMgtException.Create('������ ��� ��������� �������� �� ��.' + #13#10 + sql);
        end;
    end;
  end;

  FreeAndNil(dbRes);
  FreeAndNil(MOS);
end;

{*******************************************************************************
* getFileByURL
*******************************************************************************}
function TMgtCore.getFileXMLByURL( aURL : string; aHttpClient : TIdHTTP; aFlRetry : boolean ) : TNativeXml;
var
  dir, fileName, msg: string;
  oStream: TFileStream;
  i: integer;

begin
  Result := nil;

  try
    dir := killTrailingSlash( ExtractFileDir( ParamStr(0) ) );
    ForceDirectories( dir );

    fileName := dir + '\' + FormatDateTime( 'yyyy_mm_dd__hh_nn_ss', Now() ) + '_tmp.xml';
    ostream := TFileStream.Create( fileName, fmCreate );

    aHttpClient.Get( aURL, ostream );
    aHttpClient.Disconnect();
    FreeAndNil( ostream );

    if aHttpClient.ResponseCode <> 200 then
      raise Exception.Create( '�� �������� ������ URL = ' + aURL )
    else
    begin
      Result := TNativeXml.Create();
      Result.LoadFromFile( fileName );
    end;

    DeleteFile( fileName );
  except
    on e: Exception do
    begin
      msg := '������ ��� ������� �������. ';
      if oStream <> nil then
        msg := msg + '������ �����: ' + IntToStr( oStream.size );
      if e.Message <> '' then
        msg := msg + '. ���������: ' + e.message;

      FreeAndNil( ostream );
      DeleteFile( fileName );
    end;
  end;

  // ������
  if ( aFlRetry ) then
    for i := 1 to 5 do
      if ( Result = nil ) then
      begin
        sleep(1000);    // 1 ���
        Result := getFileXMLByURL( aURL, aHttpClient, false );
      end;
end;

{*******************************************************************************
* getSimpleSQL
*******************************************************************************}
function TMgtCore.getSimpleSQL(aSqlAlias: string; aSqlParams: TMapStrings;
  aXML: string): string;
begin
  Result := FsqlLoader.getSqlByAlias(ExtractFilePath(ParamStr(0)) + aXML, aSqlAlias, aSqlParams);
  if Result = '' then
    raise Exception.Create('������ ��� ��������� sql-������� � ������� ' + aSqlAlias + ' �� ����� ' + aXML);
end;

{*******************************************************************************
* checkSPGraphSections
*******************************************************************************}
procedure TMgtCore.checkStopPlacesGraphSections();
const
  SQL_INSERT = 'INSERT INTO gis_mgt_other.stop_place_check_graph ( ' +
  'stop_place_muid, ' +
  'source_graph_section_muid, ' +
  'destination_graph_section_muid, ' +
  'source_graph_section_offset, ' +
  'destination_graph_section_offset, ' +
  'source_graph_tram_section_muid, ' +
  'destination_graph_tram_section_muid, ' +
  'source_graph_tram_section_offset, ' +
  'destination_graph_tram_section_offset ' +
' ) SELECT ' ;

var
  sql, partSql : string;
  dbRes : TDBResult;
  spPoint : TDoublePoint;
  stopPlaceMuid, srcGSMuid, srcGTSMuid : int64;
  destGSMuid, destGTSMuid : int64;
  srcGSOffset, srcGTSOffset : double;
  destGSOffset, destGTSOffset : double;
  flGS, flGTS : boolean;

  {*****************************************************************************
  * init
  *****************************************************************************}
  procedure init();
  begin
    spPoint.X := 0;
    spPoint.Y := 0;
    destGSMuid := 0;
    destGTSMuid := 0;
    destGSOffset := 0;
    destGTSOffset := 0;
    flGS := false;
    flGTS := false;
    partSql := '';

    stopPlaceMuid := dbRes.asInt64('MUID');
    spPoint.X := dbRes.asFloat('MAXX');
    spPoint.Y := dbRes.asFloat('MAXY');
    srcGSMuid := dbRes.asInt64('graph_section_muid');
    srcGSOffset := dbRes.asFloat('graph_section_offset');
    srcGTSMuid := dbRes.asInt64('graph_tram_section_muid');
    srcGTSOffset := dbRes.asFloat('graph_tram_section_offset');

    sql := SQL_INSERT + IntToStr( stopPlaceMuid ) + ', ';
  end;

begin
  sql := 'SELECT sp.MUID, sp.MAXX, sp.MAXY, sp.graph_section_muid, sp.graph_section_offset,' +
    ' sp.graph_tram_section_muid, sp.graph_tram_section_offset FROM gis_mgt.stop_places' +
    ' sp WHERE sp.sign_deleted = 0 AND sp.OKEY > 1';

  if (Fconn.QueryOpen(sql, dbRes, true) < 0) then
  begin
    showDialog(dtError, dbsOK, '�� ������� ��������� SQL-������.', sql);
    exit;
  end;

  Fconn.TruncateTable('gis_mgt_other.stop_place_check_graph');

  try
    dbRes.initMapFields();
    while dbRes.Fetch() do
    begin
      try
        init();

        if ( spPoint.X = 0 ) and ( spPoint.Y = 0 ) then
          continue;

        partSql := ' NULL, NULL, NULL, NULL, ';

        if ( srcGSMuid > 0 ) then
        begin
          mapCore.GetNearestGraphSection( spPoint, tkBus, destGSMuid, destGSOffset );
          destGSOffset := destGSOffset / 1000;

          if ( destGSMuid > 0 ) and ( ( srcGSMuid <> destGSMuid ) or ( Abs( srcGSOffset - destGSOffset ) > 1 ) ) then
          begin
            flGS := true;
            partSql := IntToStr(srcGSMuid) + ', ' + IntToStr(destGSMuid) + ', ' + FloatToStr(srcGSOffset) + ', ' + FloatToStr(destGSOffset) + ', ';
          end;
        end;

        sql := sql + partSql;

        partSql := ' NULL, NULL, NULL, NULL';

        if ( srcGTSMuid = 0 ) then
        begin
          mapCore.GetNearestGraphSection( spPoint, tkTram, destGTSMuid, destGTSOffset );
          destGTSOffset := destGTSOffset / 1000;

          if ( destGTSMuid > 0 ) and ( ( srcGTSMuid <> destGTSMuid ) or ( Abs( srcGTSOffset - destGTSOffset/ 1000 ) > 1 ) ) then
          begin
            flGTS := true;
            partSql := IntToStr(srcGTSMuid) + ', ' + IntToStr(destGTSMuid) + ', ' + FloatToStr(srcGTSOffset) + ', ' + FloatToStr(destGTSOffset) + ', ';
          end;
        end;

        sql := sql + partSql;
        
        if flGS or flGTS then
          if FConn.QueryExec(sql, false) <> 0 then
            continue;
      except
      end;
    end;
  finally
    FreeAndNil( dbRes );
  end;
end;

{*******************************************************************************
* checkTrajectoryGraphSections
*******************************************************************************}
procedure TMgtCore.checkTrajectoryGraphSections();
const
  SQL_INSERT = 'INSERT INTO gis_mgt_other.check_trajectory_graph_sections ( ' +
  'trajectory_muid, ' +
  'transport_kind_muid, ' +
  'SPFirst, ' +
  'SPLast, ' +
  'srcGSFirst, ' +
  'destGSFirst, ' +
  'srcGSOFirst, ' +
  'destGSOFirst, ' +
  'srcGSLast, ' +
  'destGSLast, ' +
  'srcGSOLast, ' +
  'destGSOLast ' +
' ) SELECT ' ;

var
  sql, partSql : string;
  dbRes : TDBResult;
  trajectoryMuid : int64;
  transportKindMuid : int64;
  transportKind : EMgtRouteTransportKind;
  srcGSFirst, srcGSLast : int64;
  srcGSOFirst, srcGSOLast : double;
  destGSFirst, destGSLast : int64;
  destGSOFirst, destGSOLast : double;
  spDPFirst, spDPLast : TDoublePoint;
  spFirst, spLast : int64;
  flGSFirst, flGSLast : boolean;
  Count : integer;

  {*****************************************************************************
  * init
  *****************************************************************************}
  procedure init();
  begin
    trajectoryMuid := -1;
    transportKindMuid := -1;
    transportKind := tkUndefined;
    srcGSFirst := -1;
    srcGSLast := -1;
    srcGSOFirst := -1;
    srcGSOLast := -1;
    destGSFirst := -1;
    destGSLast := -1;
    destGSOFirst := -1;
    destGSOLast := -1;
    spDPFirst.X := 0;
    spDPFirst.Y := 0;
    spDPLast.X := 0;
    spDPLast.Y := 0;
    flGSFirst := false;
    flGSLast := false;
    partSql := '';
    spFirst := -1;
    spLast := -1;

    trajectoryMuid := dbRes.asInt64('trajectory_muid');
    transportKindMuid := dbRes.asInt64('transport_kind');
    srcGSFirst := dbRes.asInt64('gs_first');
    srcGSLast := dbRes.asInt64('gs_last');
    srcGSOFirst := dbRes.asFloat('gs_first_offset');
    srcGSOLast := dbRes.asFloat('gs_last_offset');
    spDPFirst.X := dbRes.asFloat('sp1X');
    spDPFirst.Y := dbRes.asFloat('sp1Y');
    spDPLast.X := dbRes.asFloat('sp2X');
    spDPLast.Y := dbRes.asFloat('sp2Y');
    spFirst := dbRes.asInt64('sp1Muid');
    spLast := dbRes.asInt64('sp2Muid');

    case transportKindMuid of
     1 : transportKind := tkBus;
     2 : transportKind := tkTrolley;
     3 : transportKind := tkTram;
     4 : transportKind := tkSpeedTram
    end;

    sql := SQL_INSERT + IntToStr( trajectoryMuid ) + ', ' + IntToStr( transportKindMuid ) + ', ' + IntToStr( spFirst ) + ', '  + IntToStr( spLast ) + ', ';
  end;

begin
  sql := 'SELECT ' +
  'rt.muid AS trajectory_muid, ' +
  'r.route_transport_kind_muid AS transport_kind, ' +
  'lnk_first.graph_section_muid AS gs_first, ' +
  'lnk_first.graph_section_start_offset AS gs_first_offset, ' +
  'lnk_last.graph_section_muid AS gs_last, ' +
  'lnk_last.graph_section_end_offset AS gs_last_offset, ' +
  'sp1.muid AS sp1Muid, ' +
  'sp1.MAXX AS sp1X, ' +
  'sp1.MAXY AS sp1Y, ' +
  'sp2.muid AS sp2Muid, ' +
  'sp2.MAXX AS sp2X, ' +
  'sp2.MAXY AS sp2Y  ' +
'FROM ' +
 ' gis_mgt.route_trajectories rt ' +
 ' LEFT JOIN gis_mgt.route_null_rounds rr ' +
   ' ON rt.route_null_round_muid = rr.muid ' +
    'AND rr.sign_deleted = 0 ' +
  'LEFT JOIN gis_mgt.route_variants rv ' +
   ' ON rr.route_variant_muid = rv.muid ' +
   ' AND rv.sign_deleted = 0 ' +
  'LEFT JOIN gis_mgt.routes r ' +
   ' ON rv.route_muid = r.muid ' +
    'AND r.sign_deleted = 0 ' +
  'INNER JOIN gis_mgt.lnk_route_trajectories__graph_sections lnk_first ' +
   ' ON lnk_first.route_trajectory_muid = rt.muid ' +
    'AND lnk_first.index = 1 ' +
    'AND lnk_first.sign_deleted = 0 ' +
  'INNER JOIN gis_mgt.lnk_route_trajectories__graph_sections lnk_last ' +
    'ON lnk_last.route_trajectory_muid = rt.muid ' +
    'AND lnk_last.is_last = 1 ' +
    'AND lnk_last.sign_deleted = 0 ' +
  'INNER JOIN gis_mgt.lnk_route_trajectories__stop_places sp_first ' +
    'ON sp_first.route_trajectory_muid = rt.muid ' +
    'AND sp_first.index = 1 ' +
    'AND sp_first.sign_deleted = 0 ' +
  'LEFT JOIN gis_mgt.stop_places sp1 ' +
    'ON sp_first.stop_place_muid = sp1.muid ' +
  'INNER JOIN gis_mgt.lnk_route_trajectories__stop_places sp_last ' +
    'ON sp_last.route_trajectory_muid = rt.muid ' +
    'AND sp_last.is_last = 1 ' +
    'AND sp_last.sign_deleted = 0 ' +
  'LEFT JOIN gis_mgt.stop_places sp2 ' +
    'ON sp_last.stop_place_muid = sp2.muid ' +
 'WHERE rt.sign_deleted = 0 ' +
  'AND rt.route_null_round_muid IS NOT NULL';

//  sql := 'SELECT ' +
//  'rt.muid AS trajectory_muid, ' +
//  'r.route_transport_kind_muid AS transport_kind, ' +
//  'lnk_first.graph_section_muid AS gs_first, ' +
//  'lnk_first.graph_section_start_offset AS gs_first_offset, ' +
//  'lnk_last.graph_section_muid AS gs_last, ' +
//  'lnk_last.graph_section_end_offset AS gs_last_offset, ' +
//  'sp1.muid AS sp1Muid, ' +
//  'sp1.MAXX AS sp1X, ' +
//  'sp1.MAXY AS sp1Y, ' +
//  'sp2.muid AS sp2Muid, ' +
//  'sp2.MAXX AS sp2X, ' +
//  'sp2.MAXY AS sp2Y  ' +
//'FROM ' +
// ' gis_mgt.route_trajectories rt ' +
// ' LEFT JOIN gis_mgt.route_rounds rr ' +
//   ' ON rt.route_round_muid = rr.muid ' +
//    'AND rr.sign_deleted = 0 ' +
//  'LEFT JOIN gis_mgt.route_variants rv ' +
//   ' ON rr.route_variant_muid = rv.muid ' +
//   ' AND rv.sign_deleted = 0 ' +
//  'LEFT JOIN gis_mgt.routes r ' +
//   ' ON rv.route_muid = r.muid ' +
//    'AND r.sign_deleted = 0 ' +
//  'INNER JOIN gis_mgt.lnk_route_trajectories__graph_sections lnk_first ' +
//   ' ON lnk_first.route_trajectory_muid = rt.muid ' +
//    'AND lnk_first.index = 1 ' +
//    'AND lnk_first.sign_deleted = 0 ' +
//  'INNER JOIN gis_mgt.lnk_route_trajectories__graph_sections lnk_last ' +
//    'ON lnk_last.route_trajectory_muid = rt.muid ' +
//    'AND lnk_last.is_last = 1 ' +
//    'AND lnk_last.sign_deleted = 0 ' +
//  'INNER JOIN gis_mgt.lnk_route_trajectories__stop_places sp_first ' +
//    'ON sp_first.route_trajectory_muid = rt.muid ' +
//    'AND sp_first.index = 1 ' +
//    'AND sp_first.sign_deleted = 0 ' +
//  'LEFT JOIN gis_mgt.stop_places sp1 ' +
//    'ON sp_first.stop_place_muid = sp1.muid ' +
//  'INNER JOIN gis_mgt.lnk_route_trajectories__stop_places sp_last ' +
//    'ON sp_last.route_trajectory_muid = rt.muid ' +
//    'AND sp_last.is_last = 1 ' +
//    'AND sp_last.sign_deleted = 0 ' +
//  'LEFT JOIN gis_mgt.stop_places sp2 ' +
//    'ON sp_last.stop_place_muid = sp2.muid ' +
// 'WHERE rt.sign_deleted = 0 ' +
//  'AND rt.route_round_muid IS NOT NULL';


  Count := 0;

  if (Fconn.QueryOpen(sql, dbRes, true) < 0) then
  begin
    showDialog(dtError, dbsOK, '�� ������� ��������� SQL-������.', sql);
    exit;
  end;

  Fconn.TruncateTable('gis_mgt_other.check_trajectory_graph_sections');
  try
    dbRes.initMapFields();
    while dbRes.Fetch() do
    begin
      try
        init();

        if ( ( spDPFirst.X = 0 ) and ( spDPFirst.Y = 0 ) ) or ( ( spDPLast.X = 0 ) and ( spDPLast.Y = 0 ) ) then
          continue;

        // ���������� ��������� ������� ������ ���������, ����� ���������
        if ( srcGSFirst > 0 ) and ( srcGSLast > 0 ) then
        begin
          partSql := ' NULL, NULL, NULL, NULL, ';
          mapCore.GetNearestGraphSection( spDPFirst, transportKind, destGSFirst, destGSOFirst );
          destGSOFirst := destGSOFirst / 1000;
          if ( destGSFirst > 0 ) and ( ( srcGSFirst <> destGSFirst ) or ( Abs( srcGSOFirst - destGSOFirst ) > 1 ) ) then
          begin
            flGSFirst := true;
            partSql := IntToStr( srcGSFirst ) + ', ' + IntToStr( destGSFirst ) + ', ' + FloatToStr( srcGSOFirst ) + ', ' + FloatToStr( destGSOFirst ) + ', ';
          end;
          sql := sql + partSql;

          partSql := ' NULL, NULL, NULL, NULL ';
          mapCore.GetNearestGraphSection( spDPLast, transportKind, destGSLast, destGSOLast );
          destGSOLast := destGSOLast /  1000;
          if ( destGSLast > 0 ) and ( ( srcGSLast <> destGSLast ) or ( Abs( srcGSOLast - destGSOLast ) > 1 ) ) then
          begin
            flGSLast := true;
            partSql := IntToStr( srcGSLast ) + ', ' + IntToStr( destGSLast ) + ', ' + FloatToStr( srcGSOLast ) + ', ' + FloatToStr( destGSOLast );
          end;
          sql := sql + partSql;

          Inc( Count );
          if Count = 0 then ;

          if flGSFirst or flGSLast then
            if FConn.QueryExec(sql, false) <> 0 then
              continue;
        end;
      except
      end;
    end;
  finally
    FreeAndNil( dbRes );
  end;
end;

{*******************************************************************************
* compareValueChange
*******************************************************************************}
function TMgtCore.compareValueChange(src, dest: string; aLevel : integer): string;
begin
  Result := #10#13 + getSpace(aLevel) + '   (���� "' + src + '" --- ����� "' + dest + '")';
end;

{*******************************************************************************
* compareValueChange
*******************************************************************************}
function TMgtCore.compareValueChange(src, dest: int64; aLevel : integer): string;
begin
  Result := #10#13 + getSpace(aLevel) + '   (���� "' + IntToStr(src) + '" --- ����� "' + IntToStr(dest) + '")';
end;

{*******************************************************************************
* compareValueChange
*******************************************************************************}
function TMgtCore.compareValueChange(src, dest: boolean; aLevel : integer): string;

  {*****************************************************************************
  * BoolToStrRus
  *****************************************************************************}
  function BoolToStrRus(aBool : boolean): string;
  begin
    Result := '���';

    if aBool then
      Result := '��';
  end;

begin
  Result := #10#13 + getSpace(aLevel) + '   (���� "' + BoolToStrRus(src) + '" --- ����� "' + BoolToStrRus(dest) + '")';
end;

{*******************************************************************************
* compareValueChange
*******************************************************************************}
function TMgtCore.compareValueChange(src, dest: TDate; aLevel : integer): string;
begin
  Result := #10#13 + getSpace(aLevel) + '   (���� ';
  if src < 0 then
    Result := Result + '<�����>'
  else
    Result := Result + '"' + DateTimeToStr(src) + '"';

  Result := Result + ' --- ����� ';
  if dest < 0 then
    Result := Result + '<�����>'
  else
    Result := Result + '"' + DateTimeToStr(dest) + '"';

  Result := Result + ')';
end;

{*******************************************************************************
* getSpace
*******************************************************************************}
function TMgtCore.getSpace(aLevel: integer): string;
var
  i : integer;
  count : integer;

begin
  Result := '';
  count := aLevel * 2;
  for i := 0 to count - 1 do
  begin
    Result := Result + ' ';
  end;
end;

{**********************************************************************************************
* EncodeFile
***********************************************************************************************}
procedure TMgtCore.EncodeFile (fileName : string);
var
  s, s2 : TStringList;
  i : integer;
begin
  s := TStringList.Create ();
  s2 := TStringList.Create ();
  try
    s.LoadFromFile(fileName);

    for i := 0 to s.Count -1 do
      s2.Add(EncodeBase64(s[i]));

    s2.SaveToFile(fileName);
  finally
    FreeAndNIl (s);
    FreeAndNIl (s2);
  end;
end;

{**********************************************************************************************
* DecodeFile
***********************************************************************************************}
procedure TMgtCore.DecodeFile (fileName : string);
var
  s, s2 : TStringList;
  i : integer;
begin
  s := TStringList.Create ();
  s2 := TStringList.Create ();
  try
    s.LoadFromFile(fileName);

    for i := 0 to s.Count -1 do
      s2.Add(DecodeBase64(s[i]));

    s2.SaveToFile(fileName);
  finally
    FreeAndNIl (s);
    FreeAndNIl (s2);
  end;
end;













end.
