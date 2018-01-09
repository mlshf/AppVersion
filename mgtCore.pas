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

  // названия полей, которые заполняет get_objects
  OBJECT_IN_TICKET = '__in_ticket';         // объект в тикете
  OBJECT_OPERATION = '__object_operation';  // операция над объектом

  // массив для преобразования (для свойства Visible контролов Devexpress)
  // из boolean в перечислимый тип TDxBarItemVisible
  mapBoolean2TDxBarItemVisible: array[boolean] of TDxBarItemVisible = (ivNever, ivAlways);

type
  //TCardOperations = array[EMgtCard] of TtrsOperationSet;
  // = array[TMgtDatasource] of TtrsOperationSet;

{**********************************************************************************************
* TMgtCore
* ядро приложения ГИС Мосгортранс -
* хранит в себе считанные настройки, запускает Mappl и авторизацию и т.д.
***********************************************************************************************}
  TMgtCore = class
  private
    // окно инициализации приложения
//    FStartupImage: TFMgtStartupImage;

    // обновление приложения
    FUpdateCore: TUpdateCore;

    // версия приложения
    FAppVersion: string;

    // тестовая версия приложения (на тестовом сервере) - считывается из Settings.xml
    FflTest: boolean;

    // авторизация
    FAuthSettings: TExtAuthSettings;
    FAuth: TFExtDXAuthorization;
    FUserPermissions: TMGTPermissions;

    // почта
    FMail: TMailSettings;

    // настройки доступа к службе загрузки расписаний и эксплуатационных показателей
    FExpParamsServiceHost : string;
    FExpParamsServicePort : integer;
    // клиент для доступа к службе загрузки ЭП
    httpClient: TIdHTTP;

    // проект карты
    FMapProject: string;

    // соединение с БД
    FDbaCore: TDbaCore;
    FConn: TDbaConnection;

    // класс для чтения запросов из файла по алиасу
    FsqlLoader: TSqlLoader;

    // параметры запроса
    FsqlParams: TMapStrings;

    // мэп для сохранения запросов по отрисовке карты
    FmapSQL: TMapStrings;

    // Тикет, редактируемого объекта
    FAdmin         : TExtAdministration;
    FAdminEngine   : TtrsAdmin;
    FClientEngine  : TtrsClient;
    FTicket        : TtrsGISMGTTicket;

    // Список обработанных запросов
    FCommitedTicketList: TStrings;

    // Событие коммита тикета
    FOnTicketCommit: TNotifyEvent;

    // Событие изменения данных тикетв
    FOnTicketChange: TtrsTicketChangedEvent;

    // параметры объекта в буфере обмена
    FbufferCardAlias : string;      // алиас карточки в буфере
    FbufferObjectMuid : int64;

    // Контейнер траекторий
    FMapTrajectories: TMapObjects;
    // Контейнер идентификаторов справочников
    FMapRefbooks: TMapInt64;

    // Временный контейнер отложенного перестроения траектория
    FPostTrajUniqueMuids: TMapInt64;

    // Контейнер для хранения траекторий, для фильтрации карты.
    FMapViewFilterTrajContainer: TMapInt64;

  private   // functions
    procedure readSettings();

    // Обработка файла MyConnections.xml
    procedure OnAuthSetSelectedConnStringHandler(aConnAliases: TConnectionAliases; var aConnAlias: string);

    // Добавить объект в тикет
    function  AddObjectToTicketInternal(aDataSource: TMgtDatasource; aMuid: Int64; aOperation: TtrsOperation;
                                        aFlSilent: Boolean = false): TtrsGISMGTObject; overload;
    function  AddObjectToTicketInternal(aDataSource: TMgtDatasource; aMuid: Int64; aOperation: TtrsOperation; aInitialData: string;
                                aVersion: Integer): TtrsGISMGTObject; overload;
    function  AddObjectToTicketInternal(aRefbookMuid, aMuid: Int64; aOperation: TtrsOperation; aInitialData: string;
                                aVersion: Integer): TtrsGISMGTObject; overload;

    // Валидация объекта перед его добавлением в запрос
    // P.S. пока здесь, но валидацию стоит вынести в отдельный класс
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

    // Преднастроенная фильтрация
    procedure AddMapRouteStoredFiltration();
    procedure AddMapStopPlacesStoredFiltration();

    function  getFilterItemList(aRoot: TcxFilterCriteriaItemList; aDisplayValues: TMapStrings): TcxFilterCriteriaItemList;
    procedure AddGridRouteStoredFiltration();
    procedure AddGridStopStoredFiltration(aDataSet: TMgtDataset);
    procedure AddGridRouteTrajectoryStoredFiltration(aDataSet: TMgtDataset);
    procedure AddGridRouteNullTrajectoryStoredFiltration();
    procedure AddTOStoredFiltration(aDataSet: TMgtDataset); overload;

    // Добавить траекторию в контейнер для фильтрации карты
    procedure AddRTToMapFilterContainer (aRouteTrajectoryMuid: string);
    // Удалить траекторию из контейнера
    procedure DeleteRTFromMapFilterContainer (aRouteTrajectoryMuid: string);

  public
    // Фильтрация
    StoredFiltration: TMgtStoredFiltration;
	// для службы пути ???
    PopupMessage: string;
  public    // functions
    constructor Create();
    destructor  Destroy(); override;

    function  doAuth(): boolean;
    function  init(): boolean;
    function  update(): boolean;

    function  checkForUpdate(): boolean;

    procedure showPopupMessage(aText: string = 'Операция выполняется...');
    procedure hidePopupMessage();

    // получить запрос по алиасу из нужного файла с подставленными параметрами
    // запросы ядра (которые могут быть использованы разными модулями)
    function  getCoreSQL(aSqlAlias: string; aSqlParams: TMapStrings = nil): string;
    // запросы из заданного xml (которые могут быть использованы разными модулями)
    function  getSimpleSQL(aSqlAlias: string; aSqlParams: TMapStrings = nil; aXML:string = ''): string;
    // запросы для загрузки списков объектов
    function  getGridSQL(aDataSet: TMgtDataset; aSqlParams: TMapStrings = nil): string;
    // запросы для поиска(?) по карте
    function  getMapSQL(aDataSet: TMgtDataset; aMUID: int64; aMapPostfix: string = ''): string; overload;
    function  getMapSQL(aSqlAlias: string; aSqlParams: TMapStrings = nil): string; overload;
    // запросы для найденных по карте объектов
    function  getMapObjectsSQL(aSqlAlias: string; aMUID: int64): string;
    // запросы для отображения эксплуатационных показателей
    function  getExpParamsSQL(aSqlAlias: string; aSqlParams: TMapStrings = nil): string;

    function  getReportsSQL(aSqlAlias: string; aSqlParams: TMapStrings = nil): string;
    function  getCardsSQL(aSqlAlias: string; aSqlParams: TMapStrings = nil): string;
    function  getSignpostsSQL(aSqlAlias: string; aSqlParams: TMapStrings = nil): string;

    // Получить папку для сохранения распечатки
    function  getPrintPath(): string;

    // получить список полей датасурса, исключить из них системные
    function  getDatasourceFields(aDataSource: TMgtDatasource): TMapStrings;

    // получить список объектов для указанной таблицы для подсветки на карте
    procedure getObjectsForHighlight(var List: TMapInt64; aDataSet: TMgtDataset; aMUID: int64; aMapPostfix: string = '');

    // операции работы с тикетом
    function  ReCreateTicket(flAfterCommit: boolean = false): TtrsGISMGTTicket;
    function  LoadBadTicket(): TtrsGISMGTTicket;
    procedure DeleteTicket();
    function  CommitTicket(aTicket: TtrsGISMGTTicket = nil): boolean;

    procedure DoOnTicketChange(aChangeType: TtrsChangeType; aTicket: TtrsTicket; aObject: TtrsObject;
                               aObjectData: TtrsObjectData; aObjectLink: TtrsObjectLink; aAttachment: TtrsAttachment);


    // Загрузка данных в комбобокс из датасета
    procedure loadLookupCombobox(aCombo: TcxLookupComboBox; aKeyField, aValueFields: string; aDataSet: TMgtDataset;
                                 aOnChange: TNotifyEvent = nil; aFlSilent: boolean = false);
    // Обработчик OnChange комбобокса для фильтрации
    procedure lookupComboboxPropertiesChangeSilent(Sender: TObject);
    procedure lookupComboboxPropertiesChange(Sender: TObject);

    // Добавляет траекторию в MapTrajectories
    // На момент вызова функции траетория должны быть в БД или тикете
    function  AddTrajectory(aMuid: int64; aTrajectoryType: EMgtRouteTrajectoryType = rttUndefined; aFlLoad: boolean = true): TMgtRouteTrajectory;
    // Умная функция добавления объекта в тикет
    // Анализирует наличие переданного объекта в тикете
    function  AddObjectToTicket(aDataSource: TMgtDatasource; aMuid: Int64; aOperation: TtrsOperation): TtrsGISMGTObject; overload;
    // Глупая функция. Добавляет объект в конкретный тикет, без анализа
    function  AddObjectToTicketSimple(aTicket: TtrsGISMGTTicket; aDataSource: TMgtDatasource; aMuid: Int64;
                                      aOperation: TtrsOperation): TtrsGISMGTObject;

    // Добавить ссылки на объекты запроса
    procedure AddObjectLinks(aDataSource: TMgtDatasource; aMuid: Int64);
    // Добавить прямую ссылку на объект запроса
    procedure AddObjectLink(aDataSourceFrom: TMgtDatasource; aMuidFrom: Int64;
                            aDataSourceTo: TMgtDatasource; aMuidTo: Int64; aLinkType: TtrsLinkTypeSet = [tltCascade, tltCommit]);
    // Удалить ссылку на объект запроса по параметрам
    procedure DeleteObjectLink(aDataSourceFrom: TMgtDatasource; aMuidFrom: Int64;
                               aDataSourceTo: TMgtDatasource; aMuidTo: Int64);

    // Добавить ссылки от места посадки-высадки к траекториям
    procedure AddStopPlaceTrajectoryObjectLinks(aStopPlaceMuid: Int64);
    // Добавить ссылку от места посадки-высадки к конкретной траектории
    procedure AddStopPlaceTrajectoryObjectLink(aStopPlaceMuid, aTrajectoryMuid: Int64);
    // Добавить ссылку от дуги графа к конкретной траектории
    procedure AddGraphSectionTrajectoryObjectLink(aGraphSectionMuid, aTrajectoryMuid: Int64);

    // Добавить slave data к объекту
    procedure AddSlaveDataToTicket(aObject: TtrsGISMGTObject; aDataSource: TMgtDatasource; aMuid: Int64; aOperation: TtrsOperation;
                                  aData: TMapStrings);
    procedure DeleteObjectFromTicket(aDataSource: TMgtDatasource; aMuid: Int64);

    // Получить первый объект в запросе по датасорсу
    function  GetFirstTicketObjectByDatasource(aDatasource: TMgtDatasource): TtrsGISMGTObject;

    // запиcать значение поля в тикет
    // строковое
    procedure setFieldValue(aObjectData: TtrsObjectData; aField: string; aValue: string; aFlNullable: boolean = true); overload;
    // целочисленное
    procedure setFieldValue(aObjectData: TtrsObjectData; aField: string; aValue: integer;
                            aFlSigned: boolean = true; aFl0AsNull: boolean = false); overload;
    // булевское
    procedure setFieldValue(aObjectData: TtrsObjectData; aField: string; aValue: boolean); overload;
    // для муидов (значения <=0 заменяет на NULL)
    procedure setFieldValue(aObjectData: TtrsObjectData; aField: string; aValue: int64); overload;
    // дата
    procedure setFieldValue(aObjectData: TtrsObjectData; aField: string; aValue: TDateTime; aFlOnlyDate: boolean = false); overload;

    // Получает список объектов (и значений указанных полей) по указанному условию из БД
    function  getObjectsFromDB(aDataSource: TMgtDatasource; aSqlCondition : string; aFields: array of string) : TlkJSONlist;

    // Получает список объектов (и значений указанных полей) по указанному условию
    // из БД наслаивая изменения из тикета, если есть
    // обязательно передавать в списке полей 'muid'
    // если aFlKeepDeleted = true, то объекты в тикете на удаление не будут исключаться из списка
    function getObjects(aDataSource: TMgtDatasource;  aConditionField, aConditionValue : string;
      aFields: array of string; aFlKeepDeleted : boolean = false;
      aSortField : string = ''; aSortMode: EMgtJsonListSortModes = jsmString) : TlkJSONlist;

    procedure mergeObjectsWithTicket(resObjectList: TlkJSONlist; aDataSource: TMgtDatasource;
                                     aConditionField, aConditionValue : string; aFields: array of string;
                                     aFlKeepDeleted : boolean = false);
    procedure sortJsonList(resObjectList: TlkJSONlist; aSortField : string; aSortMode: EMgtJsonListSortModes);

    // Получение Initial Data объекта в виде JSON строки
    function  getInitialData(aDataSource: TMgtDatasource; aMuid: int64; var vVersion: integer): String;

    function  getDelimitedText(aStringList: TStringList; aDelimiter: Char): string;

    // Получить значение поля для указанного объекта из тикета
    // возвращает, найден ли объект
    function  getObjectValueFromTicket(aDataSource: TMgtDatasource; aMuid : int64; aField: string;
                                       var resValue: string) : boolean; overload;
    // возвращает, найден ли объект
    function  getObjectValueFromTicket(aTableName: string; aMuid : int64; aField: string;
                                       var resValue: string) : boolean; overload;

    // Получить значения полей для указанного объекта из тикета
    // возвращает, найден ли объект
    function  getObjectValuesFromTicket(aDataSource: TMgtDatasource; aMuid : int64;
                                        FieldList: TMapStrings) : boolean; overload;
    // возвращает, найден ли объект
    function  getObjectValuesFromTicket(aTableName: string; aMuid : int64;
                                        FieldList: TMapStrings) : boolean; overload;

    // Получить значение поля для указанного объекта из БД
    // возвращает, найден ли объект
    function  getObjectValueFromDB(aDataSource: TMgtDatasource; aMuid : int64; aField: string;
                                   var resValue: string) : boolean; overload;
    // возвращает, найден ли объект
    function  getObjectValueFromDB(aTableName: string; aMuid : int64; aField: string;
                                   var resValue: string) : boolean; overload;

    // Получить значения полей для указанного объекта из БД
    // возвращает, найден ли объект
    function  getObjectValuesFromDB(aDataSource: TMgtDatasource; aMuid : int64;
                                    FieldList: TMapStrings) : boolean; overload;
    // возвращает, найден ли объект
    function  getObjectValuesFromDB(aTableName: string; aMuid : int64;
                                    FieldList: TMapStrings) : boolean; overload;

    // Получить значение поля для указанного объекта (из БД или тикета, если есть)
    function  getObjectValue(aDataSource: TMgtDatasource; aMuid : int64; aField: string; aDefaultValue: string = '') : string; overload;

    // Получить значение поля для указанного объекта (из БД или тикета, если есть)
    function  getObjectValue(aTableName: string; aMuid : int64; aField: string; aDefaultValue: string = '') : string; overload;

    // Получить значение blob поля для указанного объекта (из БД или тикета, если есть) и записать рез-т в поток
    function  getObjectValueBlob(aDataSource: TMgtDatasource; aMuid : int64; aField: string; var vStream: TMemoryStream) : boolean; overload;

    // Получить значение blob поля для указанного объекта (из БД или тикета, если есть) и записать рез-т в поток
    function  getObjectValueBlob(aTableName: string; aMuid : int64; aField: string; var vStream: TMemoryStream) : boolean; overload;

    // Получить значение blob поля для указанного объекта из БД
    // возвращает, найден ли объект
    function getObjectValueBlobFromDB(aDataSource: TMgtDatasource; aMuid: int64; aField: string; var vStream: TMemoryStream): boolean; overload;

    // Получить значение blob поля для указанного объекта из БД
    // возвращает, найден ли объект
    function getObjectValueBlobFromDB(aTableName: string; aMuid: int64; aField: string; var vStream: TMemoryStream): boolean; overload;

    // Получить значения полей для указанного объекта (из БД или тикета, если есть)
    function  getObjectValues(aDataSource: TMgtDatasource; aMuid : int64; FieldList: TMapStrings) : boolean; overload;

    // Получить значения полей для указанного объекта (из БД или тикета, если есть)
    function  getObjectValues(aTableName: string; aMuid : int64; FieldList: TMapStrings) : boolean; overload;

    // проверить существовыние объекта с указанным идентификатором в БД
    function isObjectExistsInDB(aDataSource: TMgtDatasource; aMuid : int64) : Boolean; overload;
    function isObjectExistsInDB(aTableName: string; aMuid : int64) : Boolean; overload;

    // Получить полное наименование объекта из БД
    function  getObjectFullNameFromDB(aTableName: string; aMuid : int64): string;
    // Получить полное наименование объекта
    function  getObjectFullName(aTableName: string; aMuid : int64): string; overload;
    // Получить полное наименование объекта
    function  getObjectFullName(aDataSource: TMgtDatasource; aMuid : int64): string; overload;

    // получить наименование остановки по муиду места посадки-высадки
    function  getStopPlaceName(aStopPlaceMuid: int64; aFlNameForTerminalPoint : boolean = false) : string;
    // получить полное наименование места посадки-высадки
    function  getStopPlaceCaption(aStopPlaceMuid: int64; aFlNameForTerminalPoint : boolean = false) : string;
    // получить улицу и направление движения по муиду места посадки-высадки
    function  getStopPlaceStreetDirection(aStopPlaceMuid: int64; var vStreet: string; var vDirection: string) : boolean;
    // Получить наименование типа проходящего транспорта
    function  getStopPlaceTransportKindName(aHasBus, aHasTrolley, aHasTram: boolean; flShort: boolean = false): string;
    // Получить статус (состояние) маршрута по статусу ГИС и признаку "временно"
    function  getRouteStatusByState(aState: EMgtRouteState; aFlTemporary: boolean): EMgtRouteState2;
    // Получить статус варианта маршрута по датам варианта
    function  getRouteVariantStateByDates(aStartDate, aEndDate: TDate): string;
    // Получить полный инвентарный номер павильона
    function  getStopPavilionFullInventoryNumber(aStopPavilionMuid: int64): string;
    // Проверить входит ли остановка в какую-либо траекторию маршрута "Мосгортранс"
    procedure stopPlaceHasRoutesByAgency(aStopPlaceMuid: int64; var flHasMGTRoutes, flHasCommercialRoutes: boolean);
    procedure stopPlacesHasRoutesByAgency(aStopPlaces: TStringList; var flHasMGTRoutes, flHasCommercialRoutes: boolean);

    // скопировать вариант (добавить в тикет на добавление), привязать к указанному маршруту
    function  copyRouteVariant(aFromMuid, aToRouteMuid: int64): TtrsGISMGTObject;
    // скопировать рейс (добавить в тикет на добавление), привязать к указанному варианту
    function  copyRouteRound(aFromMuid, aToVariantMuid: int64): TtrsGISMGTObject;
    // скопировать нулевой рейс (добавить в тикет на добавление), привязать к указанному варианту
    function  copyRouteNullRound(aFromMuid, aToVariantMuid: int64): TtrsGISMGTObject;
    // скопировать траекторию (добавить в тикет на добавление), привязать к указанному рейсу
    function  copyRouteTrajectory(aFromMuid, aToRoundMuid: int64; aTrajectoryRound : EMgtRouteTrajectoryRoundType; aTrajectoryType: EMgtRouteTrajectoryType = rttUndefined): TtrsGISMGTObject;
    // скопировать связи для указанного датасурса, для указанного родительского объекта
    procedure copyLnkObjects(aObject: TtrsGISMGTObject; aDataSource: TMgtDatasource; aParentField: string; aFromParentMuid, aToParentMuid: int64);

    // Изменить текущий вариант при изменении дат варианта
    procedure updateRouteByVariantDates(aMuid: int64);

    // Постобработка удаления объекта
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

    // Удаление приложенных файлов
    procedure onAttachmentsDeleting( aMuid: int64; aDatasource: TMgtDatasource );

    // Предобработка удаления объекта из запроса
    procedure onBeforeObjectDeleteFromTicket(aDatasource: TMgtDatasource; aMuid: int64);
    procedure onBeforeStopPlaceDeleteFromTicket(aMuid: int64);
    procedure onBeforeRouteTrajectoryDeleteFromTicket(aMuid: int64);
    // Постобработка удаления объекта из запроса
    procedure onAfterObjectDeleteFromTicket(aDatasource: TMgtDatasource; aMuid: int64);

    // Удаление ОП из запроса
    procedure deleteStopFromTicket(aMuid: int64);
    // Проверить вхождение МПВ в ОП
    function  checkStopPlaceInsideStop(aStopPlaceMuid, aStopMuid: int64): boolean;

    // удалить парк (добавить в тикет на удаление) с вложенными сушностями
    procedure deletePark(aMuid: int64);
    // удалить маршрут (добавить в тикет на удаление) с вложенными сушностями
    procedure deleteRoute(aMuid: int64);
    // удалить вариант (добавить в тикет на удаление) с вложенными сушностями
    procedure deleteRouteVariant(aMuid: int64);
    // удалить рейс (добавить в тикет на удаление) с вложенными сушностями
    procedure deleteRouteRound(aMuid: int64);
    // удалить нулевой рейс (добавить в тикет на удаление) с вложенными сушностями
    procedure deleteRouteNullRound(aMuid: int64);
    // удалить траекторию (добавить в тикет на удаление) с вложенными сушностями
    procedure deleteRouteTrajectory(aMuid: int64);
    // удалить связи (добавить в тикет на удаление) для указанного датасурса, для указанного родительского объекта
    procedure deleteLnkObjects(aObject: TtrsGISMGTObject; aDataSource: TMgtDatasource; aParentField: string; aParentMuid: int64);

    // удалить остановочный пункт (добавить в тикет на удаление) с вложенными сушностями
    procedure deleteStop(aMuid: int64);
    // удалить место посадки-высадки (добавить в тикет на удаление) с вложенными сушностями
    procedure deleteStopPlace(aMuid: int64);

    // удалить павильон ожидания (добавить в тикет на удаление) с вложенными сушностями
    procedure deleteStopPavilion(aMuid: int64);

    // Добавить в запрос и перепостроить траектории по идентификатору остановки
    procedure RebuildTrajectoriesByStopPlace(aStopPlaceMuid: int64; aRebuildSet : EMgtRouteTransportKindsSet = []);
    // Добавить в запрос и перепостроить траектории по идентификатору остановочного пункта
    procedure RebuildTrajectoriesByStop(aStopMuid: int64);
    // Добавить в запрос и перепостроить траектории по идентификатору секции
    procedure RebuildTrajectoriesByGraphSection(aGraphSectionMuid: int64; RebuildTransportTypeSet : EMgtRouteTransportKindsSet);
    // Добавить в запрос и перепостроить траектории по идентификатору узла
    procedure RebuildTrajectoriesByGraphNode(aGraphNodeMuid: int64);
    // Проверить наличие траекторий, неудавшиеся перепостроить
    function CheckIncorrctTrajectories(): boolean;

    // Добавить в тикет траектории и перестроить их
    procedure RebuildTrajectories(aMuidList: TMapInt64);
    // Перестроить всё нахрен
    procedure RebuildAllTrajectories();
    // Пересчитать всем траекториям хэш
    procedure RecalculateTrajectoriesHash();

    procedure RebuildNullTrajectories();

    // Построить графику всем остановочным пунктам (нахрен)
    procedure CreateAllStopGraphics();
    // Перепривязать все МПВ к графу
    procedure ReattachAllStopPlacesToGraph();

    // Добавить павильон в заявку
    procedure AddStopPavilionToPosterApplication(aStopPavilionMuid: int64);

    // Если нету - добавить, иначе удалить траекторию из контейнера
    procedure ProcessRTInMapFilterContainer (aRouteTrajectoryMuid: int64);

    // Очистить контейнер
    procedure ClearMapFilterContainer ();

    ///////////////////////////////
    // Методы редактирования графа
    ///////////////////////////////
    // Разбить дугу графа - выполняет все необходимые процедуры после разбиения дуги
    // Редактируемая дуга должна быть первой, вся информация о дугах уже должна быть в запросе
    procedure SplitSection(aEditedSection, aNewSection: int64);
    // Объединить дуги графа - выполняет все необходимые процедуры после объединения дуг
    // Редактируемая дуга должна быть первой, вся информация о дугах уже должна быть в запросе
    procedure UniteSection(aEditedSection, aRemovedSection: int64);

    // Скопировать семантику из одного узла графа в другой
    procedure CopyNodeData(aSrcNodeMuid, aDestNodeMuid: int64);
    // Скопировать семантику из одногой секции графа в другую
    procedure CopySectionData(aSrcSectionMuid, aDestSectionMuid: int64);

    //Перепривязать остановки к графу
    procedure ReboundStopPlacesToGraph(aSectionMuid, aNewSectionMuid: Int64; aSectionLength: double);
    //Перепривязать остановки к трамвайному графу
    procedure ReboundStopPlacesToTramGraph(aSectionMuid, aNewSectionMuid: Int64; aSectionLength: double);

    //Перерасчитать офсет у обычной дуги
    function RecalcOffsetStopPlacesToGraph(aSectionMuid : int64)  : boolean;
    //Перерасчитать офсет у трамвайной дуги
    function RecalcOffsetStopPlacesToTramGraph(aSectionMuid : int64)  : boolean;
    ///////////////////////////////
    // Конец: Методы редактирования графа
    ///////////////////////////////

    // получение параметров через запросы БЕЗ учета тикетов
    // получение муида маршрута по муиду траектории
    // (если это траектория основного рейса действующего варианта маршрута, иначе -1)
    function getDBMainRouteMuidByTrajectoryMuid(aTrajectoryMuid: int64): int64;

    // получение различных параметров С УЧЕТОМ ТИКЕТОВ (через getObjects и getObjectValues)
    // получить вид транспорта по траектории
    function GetTrajectoryTransportKind(ATrajectoryMuid: Int64): EMgtRouteTransportKind;
    // получить муид рейса по траектории
    function GetTrajectoryRoundMuid(ATrajectoryMuid: Int64; var VRoundMuid: int64): EMgtRouteTrajectoryRoundType;
    // получить муид варианта маршрута по траектории
    function GetTrajectoryVariantMuid(ATrajectoryMuid: Int64; var VVariantMuid: int64; out Datasource: TMgtDatasource): Integer;
    // получить муид маршрута по траектории
    function GetTrajectoryRouteMuid(ATrajectoryMuid: Int64; var VRouteMuid: int64): Boolean;
    // получить муиды прямой и обратной траектории рейса или нулевого рейса (если нет такой траектории -1)
    procedure GetTrajectoriesByRoundMuid(aRoundMuid: int64; aRoundType: EMgtRouteTrajectoryRoundType; var VForwardTrajectoryMuid: int64; var VBackwardTrajectoryMuid: int64);
    // получить муиды траекторий по муиду секции
    function GetTrajectoriesBySectionMuid(aSectionMuid: string; vTrajMuids: TMapInt64; flAppendToList : Boolean = false): Integer;
    // получить муиды мест посадки-высадки траектории
    function GetStopPlaces(aTrajectoryMuid: int64): TMapInt64;
    // получить муид действующего варианта маршрута
    function GetRouteCurrentVariant(ARouteMuid: Int64): int64;
    // получить муид основного рейса ('00') по муиду варианта
    function GetRouteMainRoundByVariant(AVariantMuid: Int64): int64;
    // получить муид основного рейса ('00') действующего варианта по муиду маршрута
    function GetRouteMainRound(ARouteMuid: Int64): int64;
    // получить муиды всех нулевых рейсов действующего варианта по муиду маршрута
    function GetRouteRounds(ARouteMuid: Int64; aRoundType: EMgtRouteTrajectoryRoundType; vRoundMuids: TMapInt64): integer;
    // получить муид траектории основного рейса ('00') действующего варианта по муиду маршрута
    function GetRouteTrajectoryMuid(ARouteMuid: Int64; ADirection: EMgtRouteTrajectoryType): int64;

    // получить смежные с узлом секции
    function GetGraphSectionsByGraphNode(aNodeMuid: int64; aSectionMuids: TMapInt64): integer;

    // получить объект из тикета
    function GetObjectFromTicket(aDataSource: TMgtDatasource; aMuid: int64): TtrsGISMGTObject; overload;
    function GetObjectFromTicket(aTableName: string; aMuid: int64): TtrsGISMGTObject; overload;
    function GetRefbookMUID(aDataSource: TMgtDatasource): int64; overload;
    function GetRefbookMUID(aTableName: string): int64; overload;

    // Автоматически создать зону контрольного пункта, (остановки) привязанную к месту посадки-высадки
    procedure CreateStopZone(aStopPlaceMuid: int64);
    // Автоматически создать остановочный пункт по месту посадки-высадки
    function  CreateStop(aStopPlaceMuid: int64): int64;
    // Автоматически создать графику остановочного пункта
    function  CreateStopGraphics(aStopMuid: int64; vStopGraphics: TMapObjectStructure): boolean;
    // Удалить остановочный пункт, если он пустой
    // aDeletedStopPlaceMuid - муид удаляемого МПВ.
    procedure DeleteStopIfNeeded(aStopMuid: int64);
    // Сформировать лист пар "улица - направление движения" по координате МПВ
    procedure formStrDirListBySPCoords(aPoint: TDoublePoint; vStrDirList: TMapIntegers);
    // Обновить фильтрацию
    procedure updateStoredFiltration(flRefreshMap: boolean = true);
    procedure AddTOStoredFiltration(aGridView: TcxGridExtendedDBTableView; aFlStringFilter: boolean = false); overload;

    // загрузить расписания по маршруту (при помощи службы загрузки ЭП)
    function loadRouteSchedules(aRouteErmID: integer; aFullReload: boolean) : boolean;

    // Получить кол-во нарядов на изготовление трафаретов, доступных данному пользователя
    function getUserTaskCount(): integer;


    //------------Trafarets---------------------------------------------------//
    procedure createJsonsAllTrafaretsInTasks();
    //------------Trafarets---------------------------------------------------//

    //------------Utils-------------------------------
    // Получить строковое представление дней недели
    function  GetDaysCaption( aDaysStr : string ) : string;
    // Проверить значение Variant переменной
    function  varIsValid(aVal : Variant; aAllowBlank: boolean = false) : boolean;
    // Смешать 2 цвета в пропорции
    // proportion: 0 - 100 (100: Result = Color1 ; 0: Result = Color2)
    function  blendColors(Color1, Color2: TColor; proportion: Byte): TColor;
    // Получить ФИО пользователя по МУИДу
    function  getUserFIO(aUserMuid: int64; aFullName: boolean = false): string;
    // Получить ФИО монтажника по МУИДу
    function  getInstallerFIO(aInstallerMuid: int64; aFullName: boolean = false): string;
    // Подготовить строчку для записи бинарного боля через ядро
    function  getBlobString(vStream: TMemoryStream; vFieldName: string): string;
    // загрузить файл CDR в поток и подготовить превью
    procedure loadLayoutByPath(aCDRPath, aPreviewPath: string; var vCDR: TMemoryStream; var vPreview: TdxPNGImage);
    // получить темповую директорию
    function  getTempDirectory(aSubdirectory: string = ''): string;
    // открыть директорию в проводнике
    procedure openDirectory(aDirectory: string);

    // Установить парки для нулевых рейсов
    procedure setNullRoundParkAsStopPlace();

    // Установить остановки для нулевых рейсов
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
    // авторизация
    property authSettings: TExtAuthSettings read FAuthSettings;
    property auth: TFExtDXAuthorization read FAuth;
    property userPermissions: TMGTPermissions read FUserPermissions;    

    // почта
    property mail: TMailSettings read FMail;

    // настройки доступа к службе загрузки расписаний и эксплуатационных показателей
    property expParamsServiceHost : string read FExpParamsServiceHost;
    property expParamsServicePort : integer read FExpParamsServicePort;

    // проект карты
    property mapProject: string read FMapProject;

    // соединение с БД
    property dbaCore: TDbaCore read FDbaCore;
    property conn: TDbaConnection read FConn;

    // параметры объекта в буфере обмена
    property bufferCardAlias : string read FbufferCardAlias write FbufferCardAlias;
    property bufferObjectMuid : int64 read FbufferObjectMuid write FbufferObjectMuid;

    property CommitedTicketList: TStrings read FCommitedTicketList;
    property MapTrajectories: TMapObjects read FMapTrajectories;

    // Тикет
    property Ticket: TtrsGISMGTTicket read FTicket;
    // ClientEngine
    property ClientEngine: TtrsClient read FClientEngine;
    // AdminEngine
    property AdminEngine: TtrsAdmin read FAdminEngine;
    // Ядро обновлений
    property UpdateCore: TUpdateCore read FUpdateCore;
    // Версия приложения
    property AppVersion: string read FAppVersion;
    // Тестовая версия
    property flTest: boolean read FflTest;

    // Событие обработки запроса
    property OnTicketCommit: TNotifyEvent read FOnTicketCommit write FOnTicketCommit;
    // Событие изменения данных запроса
    property OnTicketChange: TtrsTicketChangedEvent read FOnTicketChange write FOnTicketChange;

    // параметры для запросов
    property sqlParams: TMapStrings read FsqlParams;

    // Временный контейнер отложенного перестроения траектория
    property PostTrajUniqueMuids: TMapInt64 read FPostTrajUniqueMuids;

    // Контейнер для хранения траекторий, для фильтрации карты.
    property MapViewFilterTrajContainer: TMapInt64 read FMapViewFilterTrajContainer;

  end;

var
  core: TMgtCore;
  generator: TCorelGenerator;

implementation

uses cardFormsManager, mapplCore, main, trsObjectMap, IdHTTPHeaderInfo,
  MapObjectBase, spTask, trafaretDataConverter, spSignpostInTask;

var
  // функция jsonListObjectsCompare сравнивает объекты по значению поля, указанного в данной переменной
  // (не можем ее передать как параметр)
  jsonListSortField : string = '';
  // вид сортировки для jsonListObjectsCompare (не можем ее передать как параметр)
  jsonListSortMode: EMgtJsonListSortModes = jsmString;

{**********************************************************************************************
* jsonListObjectsCompare
// функция для сравнения объектов TLkJsonList, который мы получаем в getObjects
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
    // AsInteger (как и AsInt64) в lkJSON не работает адекватно, делаем такой костыль, но хорошо бы починить
      Result := CompareValue(StrToInt(val1), StrToInt(val2));
    jsmStringAsNumber:
    // AsInteger (как и AsInt64) в lkJSON не работает адекватно, делаем такой костыль, но хорошо бы починить
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
  FAppVersion := '0 (не обновлена)';
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

  // Раскодируем файл
  //DecodeFile(GetFullPathWithoutRelativePathSymbols(SETTINGS_DIR) + '\' + AUTH_ALL_CONNECTIONS_FILE);
  try
    FAuthSettings := TExtAuthSettings.Create('MGT', SETTINGS_DIR, [MAPPL_PERMISSIONS, MGT_PERMISSIONS], false, true, OnAuthSetSelectedConnStringHandler);

    if (FAuthSettings.flError) then
      exit;

    FAuth := TFExtDXAuthorization.CreateForm(FAuthSettings);

    //выставляем галочку сброса пользовательских настроек, если был апдейт
    if (Assigned(FUpdateCore)) and (FUpdateCore.NeedReset = True) then
    begin
      //возможно будет выставлено повторно - ничего страшного,
      //пока не хочу убирать из TFExtDXAuthorization аналогичные две строки 
      FAuth.CBResetAppParams.Enabled := false;
      FAuth.CBResetAppParams.Checked := true; 
    end;

    /////////////////////////////////////////////////////////////////////////
    // если у нас нет главной формы приложения, то нет активного окна процесса
    // в силу особенностей vcl модальная форма не получит фокуса ввода...
    // см CustomForm.ShowModal() - там дергается Show,
    // который просто переключает видимость формы и поднимает контрол
    // наверх по лестнице Z-order окон процесса.
    // Фокус окну никто не дает предполагается что это будет делать Parent,
    // а его то у тас и нет
    if Application.MainForm = nil then
    begin
      // поэтому
      // привлечем внимание пользователя
      // завлекательно мигая иконкой приложения
      // и становясь активным окном процесса
                           
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
    // Кодируем обратно
    //EncodeFile(GetFullPathWithoutRelativePathSymbols(SETTINGS_DIR) + '\' + AUTH_ALL_CONNECTIONS_FILE);
  end;          


end;

{**********************************************************************************************
* OnAuthSetSelectedConnStringHandler
***********************************************************************************************}
procedure TMgtCore.OnAuthSetSelectedConnStringHandler(aConnAliases: TConnectionAliases; var aConnAlias: string);
begin
  // Если есть файл явно заданных соединений, то берем оттуда
  if aConnAliases.KeysAliases.Count > 0 then
  begin
    aConnAlias := aConnAliases.ConnAlias[aConnAliases.KeysAliases.keys[0]];

    if (aConnAliases.AllConnections.ConnectionInfo[aConnAlias] <> nil) then
      TDbaCore.GetGlobalCore().Resolver.Aliases.addItem('local', aConnAliases.AllConnections.ConnectionInfo[aConnAlias].ConnString)
    else
      showDialog(dtError, dbsOK, 'Соединение с алиасом ' + aConnAlias + ' не найдено в списке соединений.');
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
      showDialog(dtError, dbsOK, 'Ошибка при чтении настроек почтового сервера', e.Message);
      exit;
    end;
  end;

  try
    readSettings();

  except
    on e: Exception do
    begin
      showDialog(dtError, dbsOK, 'Ошибка при чтении настроек приложения', e.Message);
      exit;
    end;
  end;

  FDbaCore := TDBACore.GetGlobalCore();
  FConn := FdbaCore.addConnection(authSettings.SelectedConnString, false);

  if (FConn = nil) then
  begin
    showDialog(dtError, dbsOK, 'Ошибка при создании экземпляра класса TDbConnection');
    exit;
  end;

  if (FConn.connect() <> 0) then
  begin
    showDialog(dtError, dbsOK, 'Не удалось установить подключение к серверу СУБД', 'Connstring: ' + authSettings.connString);
    exit;
  end;

  try
    ReCreateTicket();
  except
    on e: Exception do
    begin
      showDialog(dtError, dbsOk, 'Ошибка при инициализации пакета изменений', e.Message);
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
  showPopupMessage('Проверка обновлений...');

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
          showDialog(dtError, dbsOK, 'Ошибка проверки наличия новой версии ГИС "Мосгортранс".', e.message);
          exit;
        end;
      end;

      try
        if (FUpdateCore.CurrentVersion < version) then
        begin
          Result := true;
          
          showPopupMessage('Получение новой версии...');
          FUpdateCore.update(version);
        end;

      except
        on e: Exception do
          showDialog(dtError, dbsOK, 'Не удалось выполнить обновление версии ГИС "Мосгортранс".', e.message);
      end;

    except
      on e: Exception do
        showDialog(dtError, dbsOK, 'Не удалось запустить модуль обновления версий ГИС "Мосгортранс".', e.Message);
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
        raise EMgtException.Create('Ошибка при чтении файла настроек ' + SETTINGS_FILE + #13#10 + e.Message);
    end;

    // тестовая версия
    FflTest := xml.Root.ReadAttributeBool('test', false);

    // настройки карты
    node := xml.Root.NodeByName('map');
    if (node = nil) then
      raise EMgtException.Create('Ошибка при чтении файла настроек: В основном узле файла настроек нет узла `map`');

    FMapProject := GetFullPathWithoutRelativePathSymbols(node.ReadAttributeString('project', ''));

    if (FMapProject = '') then
      raise EMgtException.Create('Ошибка при чтении файла настроек: В узле `map` не задан атрибут `project`');

    // настройки службы для загрузки расписаний
    node := xml.Root.NodeByName('ExpParamsService');
    //if (node = nil) then
      //raise EMgtException.Create('Ошибка при чтении файла настроек: В основном узле файла настроек нет узла `ExpParamsService`');

    if (node <> nil) then
    begin
      FExpParamsServiceHost := node.ReadAttributeString('host', '');
      FExpParamsServicePort := node.ReadAttributeInteger('port', 0);
    end;

    //if (FExpParamsServiceHost = '') or (FExpParamsServicePort = 0) then
      //raise EMgtException.Create('Ошибка при чтении файла настроек: Не удалось считать атрибуты `host` и `port` узла `ExpParamsService`');

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
    raise Exception.Create('Ошибка при получении sql-запроса с алиасом ' + aSqlAlias + ' из файла ' + CORE_SQL_FILE);
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
    raise Exception.Create('Ошибка при получении sql-запроса с алиасом ' + aSqlAlias + ' из файла ' + CARDS_SQL_FILE);
end;

{**********************************************************************************************
* getSignpostsSQL
***********************************************************************************************}
function TMgtCore.getSignpostsSQL(aSqlAlias: string; aSqlParams: TMapStrings = nil): string;
begin
  Result := FsqlLoader.getSqlByAlias(ExtractFilePath(ParamStr(0)) + SIGNPOSTS_SQL_FILE, aSqlAlias, aSqlParams);

  if Result = '' then
    raise Exception.Create('Ошибка при получении sql-запроса с алиасом ' + aSqlAlias + ' из файла ' + SIGNPOSTS_SQL_FILE);
end;

{**********************************************************************************************
* getReportsSQL
***********************************************************************************************}
function TMgtCore.getReportsSQL(aSqlAlias: string; aSqlParams: TMapStrings = nil): string;
begin
  Result := FsqlLoader.getSqlByAlias(ExtractFilePath(ParamStr(0)) + REPORTS_SQL_FILE, aSqlAlias, aSqlParams);

  if Result = '' then
    raise Exception.Create('Ошибка при получении sql-запроса с алиасом ' + aSqlAlias + ' из файла ' + REPORTS_SQL_FILE);
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
    raise Exception.Create('Ошибка при получении sql-запроса с алиасом ' + aSqlAlias + ' из файла ' + MAP_OBJECTS_SQL_FILE);
end;

{**********************************************************************************************
* getMapSQL
***********************************************************************************************}
function TMgtCore.getMapSQL(aDataSet: TMgtDataset; aMUID: int64; aMapPostfix: string): string;
var
  key: string;
begin
  // Код запроса
  key := MAP_SELECT_PREFIX + aMapPostfix + '_' + aDataSet.Alias;

  // Если не закэширован
  if (not FmapSQL.hasKey(key)) then
  begin
    Result := FsqlLoader.getSqlByAlias(ExtractFilePath(ParamStr(0)) + MAP_SQL_FILE, key);
    FmapSQL.addItem(key, Result);
  end
  else
    Result := FmapSQL.itemsByKey[key];

  // Подставляем значение MUID-а
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
    raise Exception.Create('Ошибка при получении sql-запроса с алиасом ' + aSqlAlias + ' из файла ' + EXP_PARAMS_SQL_FILE);
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

        raise Exception.Create('Ошибка при чтении файла настроек ' + vPath + #13#10 + e.Message);
    end;

    node := xml.Root.NodeByName('settings');

    if node = nil then
    begin
      //raise Exception.Create('Ошибка при получении sql-запроса с алиасом ' + aAlias + ' из файла ' + aFile);
      exit;
    end;

    Result := node.AttributeByName['path'];
  finally
    FreeAndNil(xml);
  end;
end;

{**********************************************************************************************
* getDatasourceFields
// получить список полей датасурса, кроме системных
***********************************************************************************************}
function TMgtCore.getDatasourceFields(aDataSource: TMgtDatasource): TMapStrings;
var
  dbaFields : TStrings;
  systemField : string;
  i, j : integer;

begin
  dbaFields := TStringList.Create();

  if not conn.GetTableFields( aDataSource.TableName, dbaFields, fncLower) then
    raise EMgtException.Create('Ошибка при получении списка полей таблицы ' + aDataSource.TableName);

  // удаляем из списка полей системные
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
    showDialog(dtError, dbsOK, 'Не удалось выполнить SQL-запрос.', sql);
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
    showPopupMessage('Обновление открытых карточек...');

    try
      try
        if (flAfterCommit) then
          cardsManager.reloadOpenFormsAfterCommit()
        else
          cardsManager.reloadOpenFormsAfterClear();
      except
        on e: Exception do
          showDialog(dtError, dbsOK, 'Ошибка при обновлении карточки', e.message);
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

    showPopupMessage('Обновление открытых карточек...');

    try
      try
        cardsManager.reloadOpenFormsByTicket();

      except
        on e: Exception do
          showDialog(dtError, dbsOK, 'Ошибка при обновлении карточки', e.message);
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
      showPopupMessage('Удаление пакета изменений...');
      FTicket.Delete(FTicket.CreatorMUID);

    except
      on e: Exception do
      begin
        body := TStringList.Create();
        body.Add('Идентификатор запроса: ' + IntToStr(FTicket.MUID) + #13#10);
        body.Add('TUID запроса: ' + FTicket.TUID + #13#10);
        body.Add('Пользователь: ' + FAuth.User.FIO + #13#10);
        body.Add('Дата: ' + FormatDateTime('yyyy-mm-dd hh:nn:ss', Now()) + #13#10);
        body.Add(e.Message);
        FMail.sendMail('Ошибка при удалении пакета изменений ' + FTicket.TUID + ' пользователем ' + FAuth.User.FIO, body);
        FreeAndNil(body);

//        hidePopupMessage();

        showDialog(dtError, dbsOK, 'Ошибка при удалении пакета изменений. Пакет будет обработан администратором системы.', e.Message);

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

  showPopupMessage('Сохранение изменений...');
  try
    try
      tct.MakeTransition(ttForward);
      Result := true;
//      hidePopupMessage();

    except
      on e: Exception do
      begin
        body := TStringList.Create();
        body.Add('Идентификатор запроса: ' + IntToStr(tct.MUID) + #13#10);
        body.Add('TUID запроса: ' + tct.TUID + #13#10);
        body.Add('Пользователь: ' + FAuth.User.FIO + #13#10);
        body.Add('Дата: ' + FormatDateTime('yyyy-mm-dd hh:nn:ss', Now()) + #13#10);
        body.Add(e.Message);
        FMail.sendMail('Ошибка при сохранении пакета изменений ' + tct.TUID + ' пользователем ' + FAuth.User.FIO, body);
        FreeAndNil(body);

//        hidePopupMessage();

        showDialog(dtError, dbsOK, 'Ошибка при сохранении пакета изменений. Пакет будет обработан администратором системы.', e.Message);

        // Если запрос - наш основной, то пересоздаем его
        if (tct = FTicket) then
          ReCreateTicket(true);

        exit;
      end;
    end;     

    if (tct.State = tcsAccepted) and Assigned(FOnTicketCommit) then
    begin
      showPopupMessage('Обновление таблиц...');
      FCommitedTicketList.Add(IntToStr(tct.MUID));
      FOnTicketCommit(tct);
//    sleep(1); // Так почему-то работает, какая-то муть с потоками...
//      hidePopupMessage();
    end;

  finally
    // Если запрос - наш основной, то пересоздаем его
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
    //Анализируем входные параметры и вызываем необходимую функцию
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
            vMsg := 'Невозможно удалить остановочный пункт с идентификатором ' + IntToStr(aMuid) +
                    '. На привязанные места посадки-высадки присутствуют следующие ссылки:';
        end
        else if aDataSource.Alias = 'StopPlaces' then
        begin
          if not validateStopPlaceBeforeDelete(aMuid, vObjectList) then
            vMsg := 'Невозможно удалить место посадки-высадки с идентификатором ' + IntToStr(aMuid) +
                    '. На данный объект присутствуют следующие ссылки:';
        end
        else if aDataSource.Alias = 'RouteVariants' then
        begin
          if not validateRouteVariantBeforeDelete(aMuid, vObjectList) then
            vMsg := 'Невозможно удалить вариант с идентификатором ' + IntToStr(aMuid) +
                    '. Данный вариант выбран действующим у маршрута:';
        end
        else if aDataSource.Alias = 'TerminalPointZones' then
        begin
          if not validateTerminalPointZoneBeforeDelete(aMuid, vObjectList) then
            vMsg := 'Невозможно удалить зону посадки-высадки с идентификатором ' + IntToStr(aMuid) +
                    '. На данный объект присутствуют следующие ссылки:';
        end
        else if aDataSource.Alias = 'TerminalStations' then
        begin
          if not validateTerminalStationBeforeDelete(aMuid, vObjectList) then
            vMsg := 'Невозможно удалить конечную станцию с идентификатором ' + IntToStr(aMuid) +
                    '. На данный объект присутствуют следующие ссылки:';
        end
        else if aDataSource.Alias = 'Orders' then
        begin
          if not validateOrderBeforeDelete(aMuid, vObjectList) then
            vMsg := 'Невозможно удалить приказ с идентификатором ' + IntToStr(aMuid) +
                    '. На данный объект присутствуют следующие ссылки:';
        end
        else if aDataSource.Alias = 'StopPavilionOrders' then
        begin
          if not validateStopPavilionOrderBeforeDelete(aMuid, vObjectList) then
            vMsg := 'Невозможно удалить приказ установки с идентификатором ' + IntToStr(aMuid) +
                    '. На данный объект присутствуют следующие ссылки:';
        end
        else if aDataSource.Alias = 'CompensatoryPoints' then
        begin
          if not validateCompensatoryPointBeforeDelete(aMuid, vObjectList) then
            vMsg := 'Невозможно удалить точку компенсационного маршрута с идентификатором ' + IntToStr(aMuid) +
                    '. На данный объект присутствуют следующие ссылки:';
        end
        else
        // остальные справочники
        begin
          vLinksTo := mgtLinks.getLinksTo(aDataSource.Alias);

          if vLinksTo.Count > 0 then
          begin
            vCount := validateReferenceBeforeDelete(aDataSource, aMuid, vObjectList);
            if vCount > 0 then
              vMsg := 'Невозможно удалить объект справочника ' + aDataSource.Caption + ' с идентификатором ' + IntToStr(aMuid) +
                      '. На объект существуют следующие ссылки:';          
          end;

          vLinksTo.Free();
          // до рефакторинга стояла проверка только вспомогательных справочников
          (*
            if mgtRefLinksTo.hasKey(aDataSource) then
            begin
              vCount := validateReferenceBeforeDelete(aDataSource, aMuid, vObjectList);
              if vCount > 0 then
                vMsg := 'Невозможно удалить объект справочника ' + aDataSource.Caption + ' с идентификатором ' + IntToStr(aMuid) +
                        '. На объект существуют следующие ссылки:';
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
          vMsg := vMsg + #13#10 + 'Ещё ' + IntToStr(vCount - 10) + ' объект(-ов)...' ;
          break;
        end;

        vMsg := vMsg + #13#10;
        vMsg := vMsg + GetMgtDataSourceById( vObjectList.items[i] ).Caption  + ': ' + vObjectList.keys[i];
      end;
      // Не разрешаем добавлять в запрос более 50 объектов
      if vCount > MAX_TICKET_OBJECTS_ADD then
        showDialog(dtAlert, dbsOK, vMsg)
      else
      begin
        vMsg := vMsg + #13#10 + 'Вы хотите добавить данные объекты в запрос на редактирование?';
        if showDialog(dtAlert, dbsYesNoCancel, vMsg) = ID_YES then
        begin
          core.showPopupMessage('Добавление объектов в запрос...');
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
    //Ошибка считывания листа
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
// !Todo: проверить, как удаляется остановка, если есть привязанное ИТ (в идеале у ИТ надо сбрасывать битую ссылку)

  Result := true;
  vListTraj := getObjects(mgtDatasources[ 'LnkRouteTrajectoriesStopPlaces' ], 'stop_place_muid', IntToStr(aMuid), ['muid', 'route_trajectory_muid']);
  vListRoundA := getObjects(mgtDatasources[ 'RouteRounds' ], 'stop_place_A_muid', IntToStr(aMuid), ['muid']);
  vListRoundB := getObjects(mgtDatasources[ 'RouteRounds' ], 'stop_place_B_muid', IntToStr(aMuid), ['muid']);
  vListNullRound1 := getObjects(mgtDatasources[ 'RouteNullRounds' ], 'stop_place_1_muid', IntToStr(aMuid), ['muid']);
  vListNullRound2 := getObjects(mgtDatasources[ 'RouteNullRounds' ], 'stop_place_2_muid', IntToStr(aMuid), ['muid']);
  vListNullRound3 := getObjects(mgtDatasources[ 'RouteNullRounds' ], 'stop_place_3_muid', IntToStr(aMuid), ['muid']);
  vListInstallations := getObjects(mgtDatasources[ 'StopPavilionInstallations' ], 'stop_place_muid', IntToStr(aMuid), ['muid']);

  //Ошибка считывания листов
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
  // по варианту получаем маршрут
  try
    routeMuid := getObjectValue( mgtDatasources[ 'RouteVariants' ], aMuid, 'route_muid');

    vRoutes := getObjects( mgtDatasources[ 'Routes' ] , 'muid', routeMuid, ['muid', 'current_route_variant_muid']);

    // вариант ссылается на удаляемый маршрут
    if vRoutes.Count < 1 then
      exit;

    currentVariantMuid := vRoutes.asObject[0].asString['current_route_variant_muid'];
    // Основной вариант не задан
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

  //Ошибка считывания листов
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

  //Ошибка считывания листов
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

  //Ошибка считывания листов
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


  //Ошибка считывания листов
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

  //Ошибка считывания листов
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
  // донастройка комбобокса в runtime (вдруг в дизайн-тайме забыли)
  aCombo.Properties.ListFieldNames := aValueFields;
  aCombo.Properties.ListFieldIndex := 0;

  // ключевое поле
  aCombo.Properties.KeyFieldNames := aKeyField;

  aCombo.Properties.IncrementalSearch := false;
  aCombo.Properties.IncrementalFiltering := false;    // используем свой фильтр

  aCombo.Properties.DropDownListStyle := lsEditList;

  // сортировка по первому столбцу
  if aCombo.Properties.ListColumns.Count > 0 then
  begin
    col :=  aCombo.Properties.ListColumns.Items[0];
    col.SortOrder := soAscending;
  end;
  // загрузка
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
  // вызываем Validate, т.к. в OnValidate перечитываем из БД строчку грида
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
  vOperation: TtrsOperation;      // операция в тикете
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

    // объект уже был добавлен в тикет с другой операцией
    if aOperation = oAdd then
      raise EMgtException.Create('Невозможно добавить объект в запрос с операцией add. Объект уже добавлен в запрос с другой операцией');

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

  // получаем линки с данного датасурса
  vLinksFrom := mgtLinks.getLinksFrom(aDataSource.Alias);
  for i := 0 to vLinksFrom.Count - 1 do
  begin
    vLink := vLinksFrom.items[i];

    // Получаем значение условия
    vValue := getObjectValue(aDataSource, aMuid, vLink.FieldFrom);
    if vValue = '' then
      Continue;

    vList := getObjects( mgtDatasources[vLink.DatasourceTo], vLink.FieldTo, vValue, ['muid'], true);

    // Проставляем линки
    if Assigned(vList) then
    begin
      for j := 0 to vList.Count - 1 do
      begin
        vObj := vList.asObject[j];
        vMuid := StrToInt64(vObj.asString['muid']);

        // Обрабатываем прямые ссылки
        // Если ссылка с дуги на узел, то удалять нельзя
        if (aDataSource.Alias = 'GraphSections') and (vLink.DatasourceTo = 'GraphNodes') then
          vLinkType := vLinkType + [tltDenyDelete];

        AddObjectLink(aDataSource, aMuid, mgtDatasources[vLink.DatasourceTo], vMuid, vLinkType);
      end;
    end;

    FreeAndNil(vList);
  end;
  vLinksFrom.Free();

  // получаем линки на данный датасурс
  vLinksTo := mgtLinks.getLinksTo(aDataSource.Alias);
  for i := 0 to vLinksTo.Count - 1 do
  begin
    vLink := vLinksTo.items[i];

    // Получаем значение условия
    vValue := getObjectValue(aDataSource, aMuid, vLink.FieldTo);
    if vValue = '' then
      Continue;

    vList := getObjects( mgtDatasources[vLink.DatasourceFrom], vLink.FieldFrom, vValue, ['muid'], true);

    // Проставляем линки
    if Assigned(vList) then
    begin
      for j := 0 to vList.Count - 1 do
      begin
        vObj := vList.asObject[j];
        vMuid := StrToInt64(vObj.asString['muid']);

        // Обрабатываем обратные ссылки
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
  // Если остановки нет в запросе, линки ставить не надо
  if not Assigned(vTicketObjTo) then
    exit;

  vListTraj := getObjects( mgtDatasources[ 'LnkRouteTrajectoriesStopPlaces' ], 'stop_place_muid', IntToStr(aStopPlaceMuid),
    ['muid', 'route_trajectory_muid'], true);

  try
    // Проставляем линки
    if Assigned(vListTraj) then
    begin
      vLastMuid := -1;
      for i := 0 to vListTraj.Count - 1 do
      begin
        vTraj := vListTraj.asObject[i];
        vTrajMuid := StrToInt64(vTraj.asString['route_trajectory_muid']);
        // Траекторию обрабатывали уже, проходим
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

  // Если остановки или траетории нет в запросе, линки ставить не надо
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

  // Если дуги или траетории нет в запросе, линки ставить не надо
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

  // Ищем этот объект в slave data
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

    // объект уже был добавлен в тикет с другой операцией
    if aOperation = oAdd then
      raise EMGTException.Create('Невозможно добавить объект в запрос с операцией add. Объект уже добавлен в запрос с другой операцией');

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

  // Формируем JSON строку
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
    // Передаём пустую графику, то есть удаляем объект с карты
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
// запиcать значение поля в тикет
***********************************************************************************************}
procedure TMgtCore.setFieldValue(aObjectData: TtrsObjectData; aField: string; aValue: string; aFlNullable: boolean);
begin
  if not Assigned(aObjectData) then
    raise Exception.Create('В setFieldValue передан пустой объект TtrsObjectData');

  if (aValue = '') and (aFlNullable) then
    aValue := NULL_VALUE;

  aObjectData.SetFieldValue(aField, aValue);
end;

{**********************************************************************************************
* setFieldValue
// запиcать значение поля в тикет
// Signed - беззнаковый integer (преобразовывает отрицательные числа в NULL)
// 0AsNull - меняет число 0 на NULL
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
// запиcать значение поля (boolean) в тикет
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
// запиcать значение поля в тикет
// для муидов (значения <=0 заменяет на NULL)
***********************************************************************************************}
procedure TMgtCore.setFieldValue(aObjectData: TtrsObjectData; aField: string; aValue: int64);
var
  value : string;
begin
  if aValue <= 0 then    // если передан муид <=0
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
  if aValue <= 0 then    // если дата неопределена
    value := NULL_VALUE
  else if (aFlOnlyDate) then
    value := DateToMySqlStr(aValue)
  else
    value := DateTimeToMySqlStr(aValue);

  setFieldValue(aObjectData, aField, value);
end;

{**********************************************************************************************
* getObjectsFromDB
// Получает список объектов (и значений указанных полей) по указанному условию из БД
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
    raise Exception.Create('getObjectsFromDB: не задан параметр aDataSource ');

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
      raise EMgtException.Create('Ошибка при получении объектов из БД.' + #13#10 + sql);

    Result := TlkJSONlist.Create();

    dbRes.initMapFields();
    while dbRes.Fetch() do
    begin
      jsonObj := TlkJSONobject.Create();

      if dbRes.getFieldNum('muid') = - 1 then
        raise Exception.Create('В запросе не найдено поле muid.' + #13#10 + sql);

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
// Получает список объектов (и значений указанных полей) по указанному условию
// из БД наслаивая изменения из тикета, если есть
// обязательно передавать в списке полей 'muid'
// если aFlKeepDeleted = true, то объекты в тикете на удаление не будут исключаться из списка
***********************************************************************************************}
function TMgtCore.getObjects(aDataSource: TMgtDatasource; aConditionField, aConditionValue : string;
  aFields: array of string; aFlKeepDeleted: boolean = false;
  aSortField : string = ''; aSortMode: EMgtJsonListSortModes = jsmString) : TlkJSONlist;
var
  conditionStr : string;
begin
  if (aDataSource = nil) then
    raise Exception.Create('getObjects: не задан параметр aDataSource ');

  conditionStr := conn.quoteName(aConditionField) + ' = ' + conn.quoteValue(aConditionValue);
  Result := getObjectsFromDB(aDataSource, conditionStr, aFields);

  if Result = nil then    // ошибка при чтении объектов из БД
    exit;

  // Соединяем результат из БД с тикетом
  mergeObjectsWithTicket(Result, aDataSource, aConditionField, aConditionValue, aFields, aFlKeepDeleted);

  // сортируем список
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
// обновляет значения полей в объекте JSON по указанной ObjectData тикета
  procedure updateJsonObjectByObjectData(aJsonObject: TlkJSONobject; aObjectData: TtrsObjectData);
  var
    i : integer;
    val : string;
  begin
    // обновляем значения полей в объекте из тикета
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

  // проверяет и включает, если нужна данный объект в результат
  procedure checkAndAddObjectByObjectData(aObjectData: TtrsObjectData);
  var
    jsonObj : TlkJSONobject;
  begin
    if (aObjectData.Datasource = aDataSource.TableName)
    and (
      // новый объект
      ((aObjectData.Operation = oAdd) and (aObjectData.getActualValue(aConditionField) = aConditionValue))
      // объект отредактирован, и у него сменилось значение, по которому отбирали объект, на нужное нам      
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
    raise Exception.Create('mergeObjectsWithTicket: не задан параметр aDataSource ');

  if not Assigned(resObjectList) then
    raise EMgtException.Create('Не создан список объектов!');

  // перебираем объекты из БД, накладываем изменения из тикета (редактирование и удаление)
  i := 0;
  while i < resObjectList.Count do     // цикл по объектам из БД
  begin
    jsonObj := resObjectList.asObject[i];
    vDatasource := jsonObj.asString['datasource'];
    if vDatasource = '' then
      vDatasource := aDataSource.TableName;

    if not jsonObj.hasField('muid') then
      raise Exception.Create('В списке полей для получения из тикета не найдено поле muid.');

    muid := StrToInt64( jsonObj.asString['muid'] );

    // заполняем служебные поля - наличие объекта в тикете, операция
    jsonObj.asBoolean[ OBJECT_IN_TICKET ] := false;
    jsonObj.asInteger[ OBJECT_OPERATION ] := -1;

    // ищем объект в тикете (на редактирование и удаление)
    for j := 0 to Ticket.Objects.Count - 1 do
    begin
      obj := Ticket.Objects.items[j];

      // ищем в maindata
      objData := obj.MainData;
      objDataFound := nil;
      if (objData.Datasource = vDatasource)
      and (objData.ObjMUID = muid)
      and (objData.Operation in [oEdit, oDelete]) then
        objDataFound := objData;

      if (objDataFound = nil) then
      begin
        // ищем в slavedata
        for k := 0 to obj.SlaveData.Count - 1 do
        begin
          objData := obj.SlaveData.items[k];
          if (objData.Datasource = vDatasource)
          and (objData.ObjMUID = muid) 
          and (objData.Operation in [oEdit, oDelete]) then
          begin   // нашли
            objDataFound := objData;
            break;
          end;
        end;
      end;

      if Assigned(objDataFound) then  // объект найден
      begin
        // заполняем служебные поля - наличие объекта в тикете, операция
        jsonObj.asBoolean[ OBJECT_IN_TICKET ] := True;
        jsonObj.asInteger[ OBJECT_OPERATION ] := Integer( objDataFound.Operation );

        if (objDataFound.Operation = oEdit) then
        begin

          if objDataFound.GetActualValue(aConditionField) <> aConditionValue then
          begin
            // в объекте поменялось значение поля, по которому отбирали объекты => удаляем из результатов
            resObjectList.Delete(i);   // удаляем объект из списка объектов БД (если не стоит флаг "сохранять удаленные")
            dec(i);
          end
          else
          begin
            // обновляем значения полей в объекте из тикета
            updateJsonObjectByObjectData(jsonObj, objDataFound);
          end;
        end
        else if (objData.Operation = oDelete) and (not aFlKeepDeleted) then   //
        begin
          // удаляем объект из списка объектов БД (если не стоит флаг "сохранять удаленные")
          resObjectList.Delete(i);
          dec(i);
        end;

        break;
      end;
    end;

    inc(i);
  end;

  // перебираем объекты в тикете на добавление
  for j := 0 to Ticket.Objects.Count - 1 do
  begin
    obj := Ticket.Objects.items[j];
    
    // ищем в maindata
    objData := obj.MainData;

    checkAndAddObjectByObjectData(objData);

    // ищем в slavedata
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
      raise EMgtException.Create('Ошибка при чтении данных объекта' + #13#10 + sql);

  try
    if not dbRes.Fetch() then
      raise Exception.Create('Не найден объект' + #13#10
        + 'Таблица: ' + aDataSource.TableName + #13#10
        + 'Идентификатор: ' + IntToStr(aMuid)
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
// Получить значение поля для указанного объекта из тикета (из MainData)
// возвращает, найден ли объект
***********************************************************************************************}
function TMgtCore.getObjectValueFromTicket(aDataSource: TMgtDatasource; aMuid : int64; aField: string; var resValue: string) : boolean;
begin
  Result := getObjectValueFromTicket(aDataSource.TableName, aMuid, aField, resValue);
end;

{**********************************************************************************************
* getObjectValueFromTicket
// Получить значение поля для указанного объекта из тикета (из MainData)
// возвращает, найден ли объект
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

  // если список пуст - выходим
  if (FieldList = nil) or (FieldList.Count = 0) then
    exit;

  objDataFound := nil;

  // ищем объект в тикете
  for j := 0 to Ticket.Objects.Count - 1 do
  begin
    obj := Ticket.Objects.items[j];

    // ищем в maindata
    objData := obj.MainData;
    if (objData.Datasource = aTableName) and (objData.ObjMUID = aMuid) then
      objDataFound := objData;

    if objDataFound = nil then
    begin
      // ищем в slavedata
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

    if Assigned(objDataFound) then  // объект найден
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
// Получить значение поля для указанного объекта из БД
// возвращает, найден ли объект
***********************************************************************************************}
function TMgtCore.getObjectValueFromDB(aDataSource: TMgtDatasource; aMuid : int64; aField: string; var resValue: string) : boolean;
begin
  Result := getObjectValueFromDB(aDataSource.TableName, aMuid, aField, resValue);
end;

{**********************************************************************************************
* getObjectValueFromDB
// Получить значение поля для указанного объекта из БД
// возвращает, найден ли объект
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

  // если список пуст - выходим
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

  { убрали из запроса sign_deleted - теперь по муиду можно получить любой объект, в том числе удаленный 
  if conn.CheckFieldExistence(aDataSource, 'sign_deleted') = 0 then
    FsqlParams.itemsByKey['sign_deleted'] := 'sign_deleted = 0'
  else
    FsqlParams.itemsByKey['sign_deleted'] := '1 = 1';
  }

  sql := getCardsSql('get_object_values', FsqlParams);

  if (conn.QueryOpen(sql, dbRes, false) <> 0) then
      raise EMgtException.Create('Ошибка при получении объекта из БД.' + #13#10 + sql);

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
          if vFieldInfo.flBinary then         // если поле бинарное грузим содержимое в строку.
            SetString(field,dbres.asDataPointer(i),dbres.sizeValue(i))
            //TODO: а ещё если поле пустое (null) - можно вернуть '__NULL__'  а не пустую строку... 
          else
            field:= dbres.asString(i);       // иначе как строку...

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
// Получить значение поля для указанного объекта (из БД или тикета, если есть)
***********************************************************************************************}
function TMgtCore.getObjectValue(aDataSource: TMgtDatasource; aMuid : int64; aField: string; aDefaultValue: string) : string;
begin
  Result := getObjectValue(aDataSource.TableName, aMuid, aField, aDefaultValue);
end;

{**********************************************************************************************
* getObjectValue
// Получить значение поля для указанного объекта (из БД или тикета, если есть)
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

  // если список пуст - выходим
  if (FieldList = nil) or (FieldList.Count = 0) then
    exit;

  Result := getObjectValuesFromTicket(aTableName, aMuid, FieldList);

  if Result then
    exit;   // нашли в тикете

  Result := getObjectValuesFromDB(aTableName, aMuid, FieldList);

  {
  if not Result then
    raise Exception.Create('Не найден объект.' + #13#10 +
                           'Таблица: ' + aDataSource + #13#10 +
                           'Идентификатор: ' + IntToStr(aMuid));
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

  // поток не создан - выходим
  if not Assigned(vStream) then
    exit;

  Result := getObjectValueFromTicket(aTableName, aMuid, aField, vResValue);

  if Result then
  begin
    // нашли в тикете, конвертируем в поток
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

  // поток не создан - выходим
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
      raise EMgtException.Create('Ошибка при получении объекта из БД.' + #13#10 + sql);

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
    showDialog(dtError, dbsOk, 'Ошибка при получении наименования объекта', vSql);
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

  // Пробуем сперва получить наименование объекта напрямую из БД
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

      // Вариант
      id := FieldList.items[1];

      if (id <> '') then
      begin
        val := getObjectFullName( mgtDatasources[ 'RouteVariants' ], StrToInt64(id));

        if (val <> '') then
          Result := val + ', ' + Result;
      end;

      // КП1
      id := FieldList.items[2];

      if (id <> '') then
      begin
        val := getStopPlaceName(StrToInt64(id), true);

        if (val <> '') then
          Result := Result + ', ' + val;
      end;

      id := FieldList.items[3];

      // КП2
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

      // Вариант
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

    Result := 'Зона остановки ' + Result;
  end
  else if aDataSource.Alias = 'StopPavilions' then
  begin
    FieldList.addItem('inventory_district', '');
    FieldList.addItem('inventory_year', '');
    FieldList.addItem('inventory_number', '');

    if (getObjectValues(aDataSource, aMuid, FieldList)) then
    begin
      Result := 'Павильон: ' + FieldList.items[0] + '-' + FieldList.items[1] + '-' + FieldList.items[2];
    end;
  end
  else if aDataSource.Alias = 'StopPavilionInstallations' then
  begin
    id := getObjectValue(aDataSource, aMuid, 'installation_date');

    Result := 'Установка павильона: ' + id;
  end
  else if aDataSource.Alias = 'TerminalPointZones' then
  begin
    FieldList.addItem('route_muid', '');
    FieldList.addItem('stop_place_muid', '');

    if (getObjectValues(aDataSource, aMuid, FieldList)) then
    begin
      Result := 'Зона КП: маршрут ';
      id := FieldList.items[0];

      if (id <> '') then
        Result := Result + getObjectFullName( mgtDatasources[ 'Routes' ], StrToInt64(id))
      else
        Result := Result + '-';

      Result := Result + ', остановка ';
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

    Result := 'Зона парка ' + Result;
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
  end //служба пути
  else if aDataSource.Alias = 'DMR_transport_hubs' then
      Result := getObjectValue(aDataSource, aMuid, 'TPUName')
  else if aDataSource.Alias = 'Boundary_points' then
    Result := 'Точка № '  +getObjectValue(aDataSource, aMuid, 'no')
  else if aDataSource.Alias = 'Turnouts' then
    Result := 'Эпюр № '   +getObjectValue(aDataSource, aMuid, 'no')
  else if aDataSource.Alias = 'Nodes' then
    Result := 'Узел № '   +getObjectValue(aDataSource, aMuid, 'NODE_NUMBER')
  else if aDataSource.Alias = 'Sites_passport' then
    Result := 'Участок № '+getObjectValue(aDataSource, aMuid, 'no') //----------
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
    Result := 'Произвольный контур: ' + getObjectValue(aDataSource, aMuid, 'name')
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
    Result := 'Табло ' + getObjectValue(aDataSource, aMuid, 'code')
  else if aDataSource.Alias = 'WifiEquipment' then
    Result := 'Wifi ' + getObjectValue(aDataSource, aMuid, 'serial_number')
  else if aDataSource.Alias = 'VideoCameras' then
    Result := 'Видеокамера ' + getObjectValue(aDataSource, aMuid, 'serial_number')
  else if ( aDataSource.Alias = 'DisplayPanelServiceContracts' )
          or ( aDataSource.Alias = 'WifiEquipmentServiceContracts' ) then
    Result := 'Контракт ' + getObjectValue(aDataSource, aMuid, 'number')
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
    Result := 'Заявка ' + getObjectValue(aDataSource, aMuid, 'name')
  else if aDataSource.Alias = 'SubwayStations' then
    Result := 'Станция метро ' + getObjectValue(aDataSource, aMuid, 'name')
  else if aDataSource.Alias = 'SubwayLines' then
    Result := 'Линия метро ' + getObjectValue(aDataSource, aMuid, 'name')
  else if aDataSource.Alias = 'lnkSubwayStationsStations' then
  begin
    Result := 'Пересадка метро ';

    muid := StrToInt64Def( getObjectValue(aDataSource, aMuid, 'subway_staion_1_muid'), -1 );
    if muid <> -1 then
      Result := Result + getObjectValue(mgtDatasources[ 'SubwayStations' ], muid, 'name') + '-';

    muid := StrToInt64Def( getObjectValue(aDataSource, aMuid, 'subway_staion_2_muid'), -1 );
    if muid <> -1 then
      Result := Result + getObjectValue(mgtDatasources[ 'SubwayStations' ], muid, 'name');
  end
  else if aDataSource.Alias = 'SubwayStationEntrances' then
  begin
    Result := 'Вход/выход станции метро ';

    muid := StrToInt64Def( getObjectValue(aDataSource, aMuid, 'subway_station_muid'), -1 );
    if muid <> -1 then
      Result := Result + getObjectValue(mgtDatasources[ 'SubwayStations' ], muid, 'name');
  end
  else if aDataSource.Alias = 'SubwayTracks' then
  begin
    Result := 'Трасса линии метро ';
    muid := StrToInt64Def( getObjectValue(aDataSource, aMuid, 'subway_line_muid'), -1 );
    if muid <> -1 then
      Result := Result + getObjectValue(mgtDatasources[ 'SubwayLines' ], muid, 'name')
  end
  else if aDataSource.Alias = 'LnkSubwayTracksStation' then
  begin
    Result := 'Станция в трассе линии метро ';

    muid := StrToInt64Def(  getObjectValue(aDataSource, aMuid, 'subway_track_muid'), -1 );
    if muid <> -1 then
    begin
      muid := StrToInt64Def( getObjectValue(mgtDatasources[ 'SubwayTracks' ], muid, 'subway_line_muid'), -1 );
      if muid <> -1 then
        Result := Result + getObjectValue(mgtDatasources[ 'SubwayLines' ], muid, 'name');
    end;
  end
  else if aDataSource.Alias = 'AeroexpressStations' then
    Result := 'Станция аэроэесспреса ' + getObjectValue(aDataSource, aMuid, 'name')
  else if aDataSource.Alias = 'AeroexpressLines' then
    Result := 'Линия аэроэесспреса ' + getObjectValue(aDataSource, aMuid, 'name')
  else if aDataSource.Alias = 'AeroexpressSchedules' then
  begin
    Result := 'Расписание аэроэкспресса ';

    muid := StrToInt64Def( getObjectValue(aDataSource, aMuid, 'aeroexpress_station_muid'), -1 );
    if muid <> -1 then
      Result := Result + getObjectValue(mgtDatasources[ 'AeroexpressStations' ], muid, 'name');
  end
  else
    Result := getObjectValue(aDataSource, aMuid, 'name');

  if (Result = '') then
    Result := 'Без названия';

  FreeAndNil(FieldList);
end;

{**********************************************************************************************
* getStopPlaceName
// получить наименование остановки по муиду места посадки-высадки
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
  Result := 'Новая остановка';
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
    Result := 'Автобус';

  if aHasTrolley then
  begin
    if Result <> '' then
      Result := Result + ', ' + 'троллейбус'
    else
      Result := 'Троллейбус';
  end;

  if aHasTram then
  begin
    if Result <> '' then
      Result := Result + ', ' + 'трамвай'
    else
      Result := 'Трамвай';
  end;

  if Result = '' then
    Result := 'Не определён';
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
  Result := 'не определён';

  if (aStartDate < 0) or (aStartDate > Date()) then
    Result := 'планируемый'
  else if (aStartDate > 0) and (aEndDate > 0) and (aEndDate < Date()) then
    Result := 'архивный'
  else if (aStartDate <= Date()) then
    Result := 'действующий';
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
    raise EMgtException.Create('Ошибка при определении вхождения остановки в маршрут "Мосгортранс"');

  if dbRes.Fetch() then
  begin
    flHasMGTRoutes := dbRes.asBool(0);
    flHasCommercialRoutes := dbRes.asBool(1);
  end;

  FreeAndNil(dbRes);
end;

{**********************************************************************************************
* copyRouteVariant
// скопировать вариант (добавить в тикет на добавление), привязать к указанному маршруту
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

  // получаем значения полей объекта (из БД и тикета)
  getObjectValues(datasource, aFromMuid, copyFields);
  muid := GenerateMUID();
  Result := AddObjectToTicket(datasource, muid, oAdd);

  // копируем поля
  for i := 0 to copyFields.Count - 1 do
  begin
    Result.MainData.SetFieldValue(copyFields.keys[i], copyFields.items[i]);
  end;

  // связь с маршрутом и некопируемые поля
  Result.MainData.SetFieldValue('route_muid', IntToStr(aToRouteMuid));
  Result.MainData.SetFieldValue('order_muid', NULL_VALUE);
  Result.MainData.SetFieldValue('start_date', NULL_VALUE);
  Result.MainData.SetFieldValue('end_date', NULL_VALUE);

  // проставляем линки (повторно, т.к. только тут проставлен родительский муид)
  AddObjectLinks(datasource, muid);

  // копируем рейсы
  // получаем рейсы текущего варианта
  jsonObjectList := core.getObjects(mgtDatasources[ 'RouteRounds' ], 'route_variant_muid', IntToStr(aFromMuid), ['muid']);
  for i := 0 to jsonObjectList.count - 1 do
  begin
    jsonObj := jsonObjectList.asObject[i];
    muid := StrToInt64(jsonObj.asString['muid']);

    // копируем каждый рейс, привязываем к новому варианту
    copyRouteRound(muid, Result.ObjMUID);
  end;
  FreeAndNil(jsonObjectList);

  // копируем нулевые рейсы
  // получаем рейсы текущего варианта
  jsonObjectList := core.getObjects(mgtDatasources[ 'RouteNullRounds' ], 'route_variant_muid', IntToStr(aFromMuid), ['muid']);
  for i := 0 to jsonObjectList.count - 1 do
  begin
    jsonObj := jsonObjectList.asObject[i];
    muid := StrToInt64(jsonObj.asString['muid']);

    // копируем каждый рейс, привязываем к новому варианту
    copyRouteNullRound(muid, Result.ObjMUID);
  end;
  FreeAndNil(jsonObjectList);

  copyFields.Free();
end;

{**********************************************************************************************
* copyRouteRound
// скопировать рейс (добавить в тикет на добавление), привязать к указанному варианту
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
  // получаем значения полей объекта (из БД и тикета)
  getObjectValues(datasource, aFromMuid, copyFields);
  muid := GenerateMUID();
  Result := AddObjectToTicket(datasource, muid, oAdd);

  // копируем поля
  for i := 0 to copyFields.Count - 1 do
  begin
    Result.MainData.SetFieldValue(copyFields.keys[i], copyFields.items[i]);
  end;

  // связь с вариантом и некопируемые поля
  Result.MainData.SetFieldValue('route_variant_muid', IntToStr(aToVariantMuid));
  // проставляем линки (повторно, т.к. только тут проставлен родительский муид)
  AddObjectLinks(datasource, muid);

  // копируем траектории
  jsonObjectList := core.getObjects(mgtDatasources[ 'RouteTrajectories' ], 'route_round_muid', IntToStr(aFromMuid), ['muid']);
  for i := 0 to jsonObjectList.count - 1 do
  begin
    jsonObj := jsonObjectList.asObject[i];
    muid := StrToInt64(jsonObj.asString['muid']);

    // копируем каждую траекторию, привязываем к новому рейсу
    copyRouteTrajectory(muid, Result.ObjMUID, rtrRound);
  end;
  FreeAndNil(jsonObjectList);

  copyFields.Free();
end;

{**********************************************************************************************
* copyRouteNullRound
// скопировать нулевой рейс (добавить в тикет на добавление), привязать к указанному варианту
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

  // получаем значения полей объекта (из БД и тикета)
  getObjectValues(datasource, aFromMuid, copyFields);
  muid := GenerateMUID();
  Result := AddObjectToTicket(datasource, muid, oAdd);

  // копируем поля
  for i := 0 to copyFields.Count - 1 do
  begin
    Result.MainData.SetFieldValue(copyFields.keys[i], copyFields.items[i]);
  end;

  // связь с вариантом и некопируемые поля
  Result.MainData.SetFieldValue('route_variant_muid', IntToStr(aToVariantMuid));
  // проставляем линки (повторно, т.к. только тут проставлен родительский муид)
  AddObjectLinks(datasource, muid);

  // копируем траектории
  jsonObjectList := core.getObjects(mgtDatasources[ 'RouteTrajectories' ], 'route_null_round_muid', IntToStr(aFromMuid), ['muid']);
  for i := 0 to jsonObjectList.count - 1 do
  begin
    jsonObj := jsonObjectList.asObject[i];
    muid := StrToInt64(jsonObj.asString['muid']);

    // копируем каждую траекторию, привязываем к новому рейсу
    copyRouteTrajectory(muid, Result.ObjMUID, rtrNullRound);
  end;
  FreeAndNil(jsonObjectList);

  copyFields.Free();
end;

{**********************************************************************************************
* copyRouteTrajectory
// скопировать траекторию (добавить в тикет на добавление), привязать к указанному рейсу
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
  // получаем значения полей объекта (из БД и тикета)
  getObjectValues(datasource, aFromMuid, copyFields);
  muid := GenerateMUID();
  Result := AddObjectToTicket(datasource, muid, oAdd);

  // копируем поля
  for i := 0 to copyFields.Count - 1 do
  begin
    Result.MainData.SetFieldValue(copyFields.keys[i], copyFields.items[i]);
  end;

  // связь с рейсом/нулевым рейсом
  case aTrajectoryRound of
    rtrUndefined: raise Exception.Create('Передан неправильный тип рейса для траектории');
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
  if aTrajectoryType <> rttUndefined then   // если явно передали тип траектории, проставляем его
    Result.MainData.SetFieldValue('trajectory_type_muid', IntToStr(Integer(aTrajectoryType)) );

  // проставляем линки (повторно, т.к. только тут проставлен родительский муид)
  AddObjectLinks(datasource, muid);

  // копируем связи с остановками
  copyLnkObjects(Result, mgtDatasources[ 'LnkRouteTrajectoriesStopPlaces' ], 'route_trajectory_muid', aFromMuid, Result.ObjMUID);
  // копируем связи с чекпоинтами
  copyLnkObjects(Result, mgtDatasources[ 'LnkRouteTrajectoriesCheckPoints' ], 'route_trajectory_muid', aFromMuid, Result.ObjMUID);
  // копируем связи с графом
  copyLnkObjects(Result, mgtDatasources[ 'LnkRouteTrajectoriesGraphSections' ], 'route_trajectory_muid', aFromMuid, Result.ObjMUID);
  // копируем связи с районами
  copyLnkObjects(Result, mgtDatasources[ 'LnkRouteTrajectoriesRegions' ], 'route_trajectory_muid', aFromMuid, Result.ObjMUID);
  // копируем связи с улицами
  copyLnkObjects(Result, mgtDatasources[ 'LnkRouteTrajectoriesStreets' ], 'route_trajectory_muid', aFromMuid, Result.ObjMUID);

  copyFields.Free();
end;

{**********************************************************************************************
* copyLnkObjects
// скопировать связи для указанного датасурса, для указанного родительского объекта
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
  // связь с родительским объектом
  copyFields.addItem(aParentField, IntToStr(aToParentMuid));

  jsonObjectList := core.getObjects(aDataSource, aParentField, IntToStr(aFromParentMuid), ['muid']);
  for i := 0 to jsonObjectList.count - 1 do
  begin
    jsonObj := jsonObjectList.asObject[i];
    muid := StrToInt64(jsonObj.asString['muid']);

    // получаем значения полей объекта (из БД и тикета)
    getObjectValues(aDataSource, muid, copyFields);
    muid := GenerateMUID();
    // связь с родительским объектом
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
  // По умолчанию - на утверждении и нет основного варианта
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

  // Если маршрут удаляется, ничего делать не надо
  if Assigned(vRouteObj) and (vRouteObj.Operation = oDelete) then
    exit;

  // Получаем список всех вариантов маршрута
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

      //Есть варианты в будущем
      if ((vVariantStartDate > vCurDate) and (vVariantStartDate > 0)) then
      begin
        vRouteState := rsForApproval;
        flVariantsInFuture := true;
      end;
    end;
  finally
    FreeAndNil(jsonObjectList);
  end;

  // если отличается статус
  if vRouteStateMuid <> IntToStr(Integer(vRouteState)) then
  begin
    if not Assigned(vRouteObj) then
      vRouteObj := core.AddObjectToTicket(mgtDatasources[ 'Routes' ], vRouteMuid, oEdit);

    vRouteState2 := getRouteStatusByState(vRouteState, flTempRoute);
    core.setFieldValue(vRouteObj.MainData, 'route_state_muid', Integer(vRouteState));
    core.setFieldValue(vRouteObj.MainData, 'route_state2_muid', Integer(vRouteState2));
    // Маршрут открывается
    if vRouteState = rsOpened then
      core.setFieldValue(vRouteObj.MainData, 'close_date', '');
    flHasChanges := true;
  end;

  // если отличается вариант
  if vSettedVariantMuid <> vCurrentVariantMuid then
  begin
    if not Assigned(vRouteObj) then
      vRouteObj := core.AddObjectToTicket(mgtDatasources[ 'Routes' ], vRouteMuid, oEdit);
    core.setFieldValue(vRouteObj.MainData, 'current_route_variant_muid', vCurrentVariantMuid);

    flHasChanges := true;
  end;

  // если отличается дата открытия
  if ((vStartDate > vNewStartDate) or (vStartDate < 0)) and (vNewStartDate > 0) then
  begin
    if not Assigned(vRouteObj) then
      vRouteObj := core.AddObjectToTicket(mgtDatasources[ 'Routes' ], vRouteMuid, oEdit);
    core.setFieldValue(vRouteObj.MainData, 'open_date', DateToMySqlStr(vNewStartDate));
    flHasChanges := true;
  end;
  // если отличается дата закрытия
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
  
  // Перезагружаем открытые формы
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
  showPopupMessage('Удаление зон парка...');

  vObject := GetObjectFromTicket(mgtDatasources[ 'Parks' ], aMuid);

  datasource := mgtDatasources[ 'ParkZones' ];
  // зоны остановки
  jsonObjectList := core.getObjects(datasource, 'park_muid', IntToStr(aMuid), ['muid']);
  for i := 0 to jsonObjectList.count - 1 do
  begin
    jsonObj := jsonObjectList.asObject[i];
    muid := StrToInt64(jsonObj.asString['muid']);

    AddObjectToTicket(datasource, muid, oDelete);
  end;
  FreeAndNil(jsonObjectList);

  // удаляем связи с типами подвижного состава
  deleteLnkObjects(vObject, mgtDatasources[ 'LnkParksVehicleTypes' ], 'park_muid', aMuid);
  // удаляем связи с перевозчиками
  deleteLnkObjects(vObject, mgtDatasources[ 'LnkParksAgencies' ], 'park_muid', aMuid);
  hidePopupMessage ();
end;

{**********************************************************************************************
* onOrderDeleting
***********************************************************************************************}
procedure TMgtCore.onOrderDeleting(aMuid: int64);
begin
  showPopupMessage('Удаление приложений...');
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
  showPopupMessage('Удаление мест посадки-высадки...');

  datasource := mgtDatasources[ 'StopPlaces' ];
  // получаем места посадки высадки
  jsonObjectList := core.getObjects(datasource, 'stop_muid', IntToStr(aMuid), ['muid']);
  for i := 0 to jsonObjectList.count - 1 do
  begin
    jsonObj := jsonObjectList.asObject[i];
    muid := StrToInt64(jsonObj.asString['muid']);

    // удаляем каждое место посадки-высадки
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
  showPopupMessage('Удаление зон...');

  vStopMuid := StrToInt64(core.getObjectValue(mgtDatasources[ 'StopPlaces' ], aMuid, 'stop_muid', '-1'));
  AddStopPlaceTrajectoryObjectLinks(aMuid);

  DeleteStopIfNeeded(vStopMuid);

  datasource := mgtDatasources[ 'StopZones' ];
  // зоны остановки
  jsonObjectList := core.getObjects(datasource, 'stop_place_muid', IntToStr(aMuid), ['muid']);
  for i := 0 to jsonObjectList.count - 1 do
  begin
    jsonObj := jsonObjectList.asObject[i];
    muid := StrToInt64(jsonObj.asString['muid']);

    AddObjectToTicketInternal(datasource, muid, oDelete);
  end;
  FreeAndNil(jsonObjectList);

  datasource := mgtDatasources[ 'TerminalPointZones' ];
  // зоны КП
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
  showPopupMessage('Удаление установок...');
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

  showPopupMessage('Удаление приложений...');
  onAttachmentsDeleting(aMuid, mgtDatasources[ 'AttachmentsStopPavilions' ]);
  hidePopupMessage();
end;

{**********************************************************************************************
* onStopPavilionOrderDeleting
***********************************************************************************************}
procedure TMgtCore.onStopPavilionOrderDeleting(aMuid: int64);
begin
  showPopupMessage('Удаление приложений...');
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
  showPopupMessage('Удаление вариантов...');

  vObject :=  GetObjectFromTicket(mgtDatasources[ 'Routes' ], aMuid);

  datasource := mgtDatasources[ 'RouteVariants' ];
  // получаем варианты текущего маршрута
  jsonObjectList := core.getObjects(datasource, 'route_muid', IntToStr(aMuid), ['muid']);
  for i := 0 to jsonObjectList.count - 1 do
  begin
    jsonObj := jsonObjectList.asObject[i];
    muid := StrToInt64(jsonObj.asString['muid']);

    // удаляем каждый вариант
    AddObjectToTicket(datasource, muid, oDelete);
  end;
  FreeAndNil(jsonObjectList);

  // удаляем связи с парками
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
  showPopupMessage('Удаление рейсов, траекторий...');
  updateRouteByVariantDates(aMuid);

  datasource := mgtDatasources[ 'RouteRounds' ];
  // получаем рейсы текущего маршрута
  jsonObjectList := core.getObjects(datasource, 'route_variant_muid', IntToStr(aMuid), ['muid']);
  for i := 0 to jsonObjectList.count - 1 do
  begin
    jsonObj := jsonObjectList.asObject[i];
    muid := StrToInt64(jsonObj.asString['muid']);

    // удаляем каждый рейс
    AddObjectToTicket(datasource, muid, oDelete);
  end;
  FreeAndNil(jsonObjectList);

  datasource := mgtDatasources[ 'RouteNullRounds' ];
  // получаем нулевые рейсы текущего маршрута
  jsonObjectList := core.getObjects(datasource, 'route_variant_muid', IntToStr(aMuid), ['muid']);
  for i := 0 to jsonObjectList.count - 1 do
  begin
    jsonObj := jsonObjectList.asObject[i];
    muid := StrToInt64(jsonObj.asString['muid']);

    // удаляем каждый рейс
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
  showPopupMessage('Удаление траекторий...');

  datasource := mgtDatasources[ 'RouteTrajectories' ];
  // получаем траектории текущего рейса
  jsonObjectList := core.getObjects(datasource, 'route_round_muid', IntToStr(aMuid), ['muid']);
  for i := 0 to jsonObjectList.count - 1 do
  begin
    jsonObj := jsonObjectList.asObject[i];
    muid := StrToInt64(jsonObj.asString['muid']);

    // удаляем каждую траекторию
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
  showPopupMessage('Удаление траекторий...');
  datasource := mgtDatasources[ 'RouteTrajectories' ];
  // получаем траектории текущего рейса
  jsonObjectList := core.getObjects(datasource, 'route_null_round_muid', IntToStr(aMuid), ['muid']);
  for i := 0 to jsonObjectList.count - 1 do
  begin
    jsonObj := jsonObjectList.asObject[i];
    muid := StrToInt64(jsonObj.asString['muid']);

    // удаляем каждую траекторию
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
  showPopupMessage('Удаление траектории...');

  vObject := GetObjectFromTicket(mgtDatasources[ 'RouteTrajectories' ], aMuid);

  // удаляем связи с остановками
  deleteLnkObjects(vObject, mgtDatasources[ 'LnkRouteTrajectoriesStopPlaces' ], 'route_trajectory_muid', aMuid);
  // удаляем связи с чекпоинтами
  deleteLnkObjects(vObject, mgtDatasources[ 'LnkRouteTrajectoriesCheckPoints' ], 'route_trajectory_muid', aMuid);
  // удаляем связи с графом
  deleteLnkObjects(vObject, mgtDatasources[ 'LnkRouteTrajectoriesGraphSections' ], 'route_trajectory_muid', aMuid);
  // удаляем связи с районами
  deleteLnkObjects(vObject, mgtDatasources[ 'LnkRouteTrajectoriesRegions' ], 'route_trajectory_muid', aMuid);
  // удаляем связи с улицами
  deleteLnkObjects(vObject, mgtDatasources[ 'LnkRouteTrajectoriesStreets' ], 'route_trajectory_muid', aMuid);
   // удаляем связи с компенсационными точками
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
  showPopupMessage('Удаление станции...');
  vObject := GetObjectFromTicket(mgtDatasources[ 'SubwayStations' ], aMuid);

  datasource := mgtDatasources[ 'SubwayStationEntrances' ];
  // получаем варианты текущего маршрута
  jsonObjectList := core.getObjects(datasource, 'subway_station_muid', IntToStr(aMuid), ['muid']);
  try
    for i := 0 to jsonObjectList.count - 1 do
    begin
      jsonObj := jsonObjectList.asObject[i];
      muid := StrToInt64(jsonObj.asString['muid']);

      // удаляем каждый вариант
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
  showPopupMessage('Удаление приложений...');
  onAttachmentsDeleting(aMuid, mgtDatasources[ 'AttachmentsDisplayPanelPhotos' ]);
  hidePopupMessage();
end;

{*******************************************************************************
* onDisplayPanelServiceContractDeleting
*******************************************************************************}
procedure TMgtCore.onDisplayPanelServiceContractDeleting(aMuid: int64);
begin
  showPopupMessage('Удаление приложений...');
  onAttachmentsDeleting(aMuid, mgtDatasources[ 'AttachmentsDisplayPanelServiceContracts' ]);
  hidePopupMessage();
end;

{*******************************************************************************
* onWifiEquipmentDeleting
*******************************************************************************}
procedure TMgtCore.onWifiEquipmentDeleting(aMuid: int64);
begin
  showPopupMessage('Удаление приложений...');
  onAttachmentsDeleting(aMuid, mgtDatasources[ 'AttachmentsWifiEquipmentPhotos' ]);
  hidePopupMessage();
end;

{*******************************************************************************
* onVideoCameraDeleting
*******************************************************************************}
procedure TMgtCore.onVideoCameraDeleting(aMuid: int64);
begin
  showPopupMessage('Удаление приложений...');
  onAttachmentsDeleting(aMuid, mgtDatasources[ 'AttachmentsVideoCamerasPhotos' ]);
  hidePopupMessage();
end;

{*******************************************************************************
* onWifiEquipmentServiceContractDeleting
*******************************************************************************}
procedure TMgtCore.onWifiEquipmentServiceContractDeleting(aMuid: int64);
begin
  showPopupMessage('Удаление приложений...');
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
  showPopupMessage('Удаление зон парка...');
  AddObjectToTicketInternal(mgtDatasources[ 'Parks' ], aMuid, oDelete);
  hidePopupMessage();
end;

{**********************************************************************************************
* deleteRoute
// удалить маршрут (добавить в тикет на удаление) с вложенными сушностями
***********************************************************************************************}
procedure TMgtCore.deleteRoute(aMuid: int64);
begin
  showPopupMessage('Удаление вариантов...');
  AddObjectToTicketInternal(mgtDatasources[ 'Routes' ], aMuid, oDelete);
  hidePopupMessage();
end;

{**********************************************************************************************
* deleteRouteVariant
// удалить вариант (добавить в тикет на удаление) с вложенными сушностями
***********************************************************************************************}
procedure TMgtCore.deleteRouteVariant(aMuid: int64);
begin
  showPopupMessage('Удаление рейсов, траекторий...');
  // удаление рейсов, траекторий
  AddObjectToTicketInternal(mgtDatasources[ 'RouteVariants' ], aMuid, oDelete);
  hidePopupMessage();
end;

{**********************************************************************************************
* deleteRouteRound
// удалить рейс (добавить в тикет на удаление) с вложенными сушностями
***********************************************************************************************}
procedure TMgtCore.deleteRouteRound(aMuid: int64);
begin
  showPopupMessage('Удаление траекторий...');
  AddObjectToTicketInternal(mgtDatasources[ 'RouteRounds' ], aMuid, oDelete);
  hidePopupMessage();
end;

{**********************************************************************************************
* deleteRouteNullRound
// удалить нулевой рейс (добавить в тикет на удаление) с вложенными сушностями
***********************************************************************************************}
procedure TMgtCore.deleteRouteNullRound(aMuid: int64);
begin
  showPopupMessage('Удаление траекторий...');
  AddObjectToTicketInternal(mgtDatasources[ 'RouteNullRounds' ], aMuid, oDelete);
  hidePopupMessage();
end;

{**********************************************************************************************
* deleteRouteTrajectory
// удалить траекторию (добавить в тикет на удаление) с вложенными сушностями
***********************************************************************************************}
procedure TMgtCore.deleteRouteTrajectory(aMuid: int64);
begin
  showPopupMessage('Удаление траектории...');
  AddObjectToTicketInternal(mgtDatasources[ 'RouteTrajectories' ], aMuid, oDelete);
  hidePopupMessage();
end;

{**********************************************************************************************
* deleteLnkObjects
// удалить связи (добавить в тикет на удаление) для указанного датасурса, для указанного родительского объекта
// добавить как slave data переданному объекту
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
// удалить остановочный пункт (добавить в тикет на удаление) с вложенными сушностями
***********************************************************************************************}
procedure TMgtCore.deleteStop(aMuid: int64);
begin
  showPopupMessage('Удаление мест посадки-высадки...');
  AddObjectToTicketInternal(mgtDatasources[ 'Stops' ], aMuid, oDelete);
  hidePopupMessage();
end;

{**********************************************************************************************
* deleteStopPlace
// удалить место посадки-высадки (добавить в тикет на удаление) с вложенными сушностями
***********************************************************************************************}
procedure TMgtCore.deleteStopPlace(aMuid: int64);
begin
  showPopupMessage('Удаление зон...');
  AddObjectToTicketInternal(mgtDatasources[ 'StopPlaces' ], aMuid, oDelete);
  hidePopupMessage();
end;

{**********************************************************************************************
* deleteStopPavilion
// удалить павильон ожидания (добавить в тикет на удаление) с вложенными сушностями
***********************************************************************************************}
procedure TMgtCore.deleteStopPavilion(aMuid: int64);
begin
  showPopupMessage('Удаление павильона...');
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
// Постобработка удаления объекта из запроса
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
  // Сразу меняем ссылку МПВ на первоначальный ОП
  setFieldValue(vStopPlaceObj.MainData, 'stop_muid', vInitStopMuid);

  // Если операция была добавление или редактирование, проверяем надо ли удалить ОП
  if vStopPlaceObj.Operation <> oDelete then
    DeleteStopIfNeeded(vStopMuid);

  // Не задана первоначальная связь с ОП -> это неправильно
  if vInitStopMuid <= 0 then
    exit;

  vInitStopObj := GetObjectFromTicket(mgtDatasources[ 'Stops' ], vInitStopMuid);
  // ОП нет в запросе -> ничего делать не надо.
  if not Assigned(vInitStopObj) then
    Exit;

  // Возможно такого стопа уже не существует
  vStop := getObjects(mgtDatasources[ 'Stops' ], 'muid', IntToStr(vStopMuid), ['muid']);
  if vStop.Count > 0 then
    if (vStopPlaceObj.Operation = oEdit) and (vInitStopMuid <> vStopMuid) then
      DeleteStopIfNeeded(vStopMuid);
  FreeAndNil(vStop);

  // Проверить как работает функция с удаляемыми объектами
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
  // Получаем список всех привязанных к ОП МПВ, включая удаляемые
  vStopPlaceList := getObjects(datasource, 'stop_muid', IntToStr(aMuid), ['muid'], true);

  vStopObj := GetObjectFromTicket(mgtDatasources[ 'Stops' ], aMuid);
  if vStopObj.Operation <> oDelete then
    // Удаляем все линки
    for i := 0 to vStopPlaceList.Count - 1 do
    begin
      vStopPlaceMuid := StrToInt64(vStopPlaceList.asObject[i].asString['muid']);
      DeleteObjectLink(datasource, vStopPlaceMuid, mgtDatasources[ 'Stops' ], aMuid);
    end;

  // Здесь может стоит удалить из тикета напрямую, функцией ядра
  DeleteObjectFromTicket(mgtDatasources[ 'Stops' ], aMuid);

  // Проверяем связи с МПВ
  for i := 0 to vStopPlaceList.Count - 1 do
  begin
    vStopPlaceMuid := StrToInt64(vStopPlaceList.asObject[i].asString['muid']);
    vStopPlaceObj := GetObjectFromTicket(datasource, vStopPlaceMuid);
    // Если объекта нет в запросе или он удаляется, ничего не делаем
    if (not Assigned(vStopPlaceObj)) or (vStopPlaceObj.Operation = oDelete) then
      Continue;

    // МПВ не внутри ОП, удаляем его из запроса
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

  // Зона уже есть, ничего не делаем
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
  // Получаем графику места посадки-высадки
  if not mapCore.GetMapObject( mgtDatasources[ 'StopPlaces' ], aStopPlaceMuid, vStopPlaceMos) then
  begin
    FreeAndNil(vStopPlaceMos);
    exit;
  end;

  vStopMos := TMapObjectStructure.Create();
  Result := GenerateMuid();
  vStopObject := AddObjectToTicket(mgtDatasources[ 'Stops' ], Result, oAdd);

  // Формируем графику
  vStopPlaces := TMapInt64.Create();
  vStopPlaces.addItem(IntToStr(aStopPlaceMuid) , -1);
  if not mapCore.CreateStopGraphics(vStopMos, vStopPlaces) then
    mapCore.MakeSquareBuffer(vStopMos, vStopPlaceMos.FirstVertex[0], 5000);

  vStopMos.lCode := mgtDatasources[ 'Stops' ].layerCode;
  vStopMos.oType := POLYGON_TYPE;
  vStopMos.oStyleID := mgtDatasources[ 'Stops' ].MapStyleID;
  vStopMos.oMUID := Result;
  vGeom := mapCore.GetGeometryAsBase64String(vStopMos, vStopMos.lCode);

  // Кладём в запрос графику
  setFieldValue(vStopObject.MainData, MOS_TAG, vGeom);

  // Пишем наименование
  // Поскольку тут ещё не изменена ссылка МПВ, имя возмётся из предыдущего стопа
  vName := getStopPlaceName(aStopPlaceMuid);
  setFieldValue(vStopObject.MainData, 'name', vName);
  // Не забываем про трафаретные названия
  setFieldValue(vStopObject.MainData, 'signpost_caption', SignpostCaption(vName, true));

  vName := getStopPlaceName(aStopPlaceMuid, true);
  setFieldValue(vStopObject.MainData, 'name_for_terminal_point', vName);

  setFieldValue(vStopObject.MainData, 'signpost_narrow_name', SignpostNarrowName(vName, true));
  setFieldValue(vStopObject.MainData, 'signpost_wide_name', SignpostWideName(vName, true));

  // Связи с АО и МР
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

  // Проверяем что все МПВ внутри ОП
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

  // Ищем все привязанные к ОП места посадки-высадки
  jsonObjectList := core.getObjects(mgtDatasources[ 'StopPlaces' ], 'stop_muid', IntToStr(aStopMuid), ['muid']);
  try
    if jsonObjectList.Count = 0 then
    begin
      vStop := getObjects(mgtDatasources[ 'Stops' ], 'muid', IntToStr(aStopMuid), ['muid']);
      if vStop.Count > 0 then // Если такого ОП уже нет, мы удалили его раньше
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
  //Ошибка считывания листа
  if not Assigned(vListTraj) then
    raise EMgtException.Create('Ошибка при получении списка проходящих траекторий!');

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
        core.showPopupMessage('Построение траекторий ' + IntToStr(i + 1) + ' из ' + IntToStr(vListTraj.Count));
        vTraj := vListTraj.asObject[i];
        vTrajMuid := StrToInt64(vTraj.asString['route_trajectory_muid']);

        // Если явно указан тип подвижного состава перепостраиваемых для остановки траекторий - то перепостраиваем только их.
        if (aRebuildSet <> []) and (not (GetTrajectoryTransportKind(vTrajMuid) in aRebuildSet)) then
          continue;

        // Эту траекторию уже обрабатывали, проходим
        if vLastMuid = vTrajMuid then
          continue;

        vLastMuid := vTrajMuid;

        // Добавляем траекторию в запрос
        vTicketObject := AddObjectToTicket(datasource, vTrajMuid, oEdit);

        // Добавляем в траекторию массив траекторий
        vTrajectory := AddTrajectory(vTrajMuid);

        // Перепостраиваем траекторию
        vRes := vTrajectory.RebuildTrajectoryByStopPlaces(vStopPlaces);

        // Сохраняем изменения в запрос
        vTrajectory.SaveToTicket();

        // Обновляем карточку
        cardsManager.refreshTrajectoryForm(vTrajectory.Muid);

        if not vRes then
        begin
          Inc(ErrCnt);
          vTrajectory.Correct := false;
        end;
        // преобразуем длины к 3му знаку после запятой
        // Если длина изменилась более чем на 10 метров, ставим уведомляющий статус
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

    // Теперь ищем траектории, которые имеют эту остановку, но в тикете этих данных ещё нет (не нажали apply на карточке траектории)
    try
      for i := 0 to FMapTrajectories.Count - 1 do
      begin
        vTrajectory := TMgtRouteTrajectory(FMapTrajectories.items[i]);
        // Траектория синхронизированна с запросом, мы её уже обработали
        if vTrajectory.Saved then
          Continue;

        // В противном случае проверяем нужно ли перепостраивать эту траекторию
        for j := 0 to vTrajectory.ControlPoints.Count - 1 do
        begin
          if vTrajectory.ControlPoints.ItemStopPlaceMuid[j] = aStopPlaceMuid then
          begin
            // Наш клиент
            core.showPopupMessage('Построение несохранённых траекторий');
            // Добавляем линк
            AddStopPlaceTrajectoryObjectLink(aStopPlaceMuid, vTrajectory.Muid);

            // Перепостраиваем траекторию
            vRes := vTrajectory.RebuildTrajectoryByStopPlaces(vStopPlaces);

            // Сохраняем изменения в запрос
            vTicketObject := vTrajectory.SaveToTicket();
            // Обновляем карточку
            cardsManager.refreshTrajectoryForm(vTrajectory.Muid);

            if not vRes then
            begin
              Inc(ErrCnt);
              vTrajectory.Correct := false;
            end;
            // преобразуем длины к 3му знаку после запятой
            // Если длина изменилась более чем на 20 метров, открываем карточку
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
      ErrWrnMessage := ErrWrnMessage + 'Длины одной или нескольких траекторий сильно изменились' + sLineBreak;

    if ErrCnt <> 0 then
      ErrWrnMessage := ErrWrnMessage + 'Не удалось перепостроить одну или несколько траекторий' + sLineBreak;

    if Length(ErrWrnMessage) <> 0 then
      showDialog(dtAlert, dbsOK, ErrWrnMessage + 'Подробности в пакете изменений.');

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
  // получаем список всех МПВ в ОП
  for i := 0 to vListSP.Count - 1 do
    vStopPlaces.Add(vListSP.asObject[i].asString['muid']);
  FreeAndNil(vListSP);
  datasource := mgtDatasources[ 'LnkRouteTrajectoriesStopPlaces' ];
  // Собираем список уникальных траекторий проходящих через этот ОП
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
      core.showPopupMessage('Построение траекторий ' + IntToStr(i + 1) + ' из ' + IntToStr(vTrajectories.Count));
      vTrajMuid := StrToInt64(vTrajectories.keys[i]);

      // Добавляем траекторию в запрос
      vTicketObject := AddObjectToTicket(datasource, vTrajMuid, oEdit);

      // Добавляем в траекторию массив траекторий
      vTrajectory := AddTrajectory(vTrajMuid);

      // Перепостраиваем траекторию
      vRes := vTrajectory.RebuildTrajectoryByStopPlaces(vStopPlaces);

      // Сохраняем изменения в запрос
      vTrajectory.SaveToTicket();
      // Обновляем карточку
      cardsManager.refreshTrajectoryForm(vTrajectory.Muid);

      if not vRes then
      begin
        showDialog(dtAlert, dbsOK, 'Не удалось построить траекторию с идентификатором: ' + IntToStr(vTrajectory.Muid));
        vTrajectory.Correct := false;
      end;
      // преобразуем длины к 3му знаку после запятой
      // Если длина изменилась более чем на 10 метров, ставим уведомляющий статус
      oldLength := UniStrToFloatDef(vTicketObject.MainData.GetInitialValue('length'), 0);
      newLength := UniStrToFloatDef(vTicketObject.MainData.GetActualValue('length'), 0);
      lengthDiff := Abs(Round(oldLength * 1000) - Round(newLength * 1000));
      if lengthDiff > 10 then
        vTrajectory.setStatus(mtosWarning);
    end;
  finally
    FreeAndNil(vTrajectories);
  end;

  // Теперь ищем траектории, которые имеют эту остановку, но в тикете этих данных ещё нет
  try
    for i := 0 to FMapTrajectories.Count - 1 do
    begin
      vTrajectory := TMgtRouteTrajectory(FMapTrajectories.items[i]);
      // Траектория синхоронизированна с запросом, мы её уже обработали
      if vTrajectory.Saved then
        Continue;

      // В противном случае проверяем нужно ли перепостраивать эту траекторию
      for j := 0 to vTrajectory.ControlPoints.Count - 1 do
      begin
        if vStopPlaces.IndexOf(IntToStr(vTrajectory.ControlPoints.ItemStopPlaceMuid[j])) >= 0 then
        begin
          // Наш клиент
          core.showPopupMessage('Построение несохранённых траекторий');
          // Добавляем линк
          AddStopPlaceTrajectoryObjectLink(vTrajectory.ControlPoints.ItemStopPlaceMuid[j], vTrajectory.Muid);

          // Перепостраиваем траекторию
          vRes := vTrajectory.RebuildTrajectoryByStopPlaces(vStopPlaces);

          // Сохраняем изменения в запрос
          vTicketObject := vTrajectory.SaveToTicket();
          // Обновляем карточку
          cardsManager.refreshTrajectoryForm(vTrajectory.Muid);

          if not vRes then
          begin
            showDialog(dtAlert, dbsOK, 'Не удалось построить траекторию с идентификатором: ' + IntToStr(vTrajectory.Muid));
            vTrajectory.Correct := false;
          end;
          // преобразуем длины к 3му знаку после запятой
          // Если длина изменилась более чем на 20 метров, открываем карточку
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
  //Ошибка считывания листа
  if not Assigned(vListTraj) then
    raise EMgtException.Create('Ошибка при получении списка проходящих траекторий!');

  FailedRebuildRouteList := TStringList.Create ();
  try
    vLastMuid := -1;
    for i := 0 to vListTraj.Count - 1 do
    begin
      {$IFNDEF GRAPH_POST_COMMIT}
      core.showPopupMessage('Построение траекторий ' + IntToStr(i + 1) + ' из ' + IntToStr(vListTraj.Count));
      {$ENDIF}

      vTraj := vListTraj.asObject[i];
      vTrajMuid := StrToInt64(vTraj.asString['route_trajectory_muid']);


      // Эту траекторию уже обрабатывали, проходим
      if vLastMuid = vTrajMuid then
        Continue;

      vLastMuid := vTrajMuid;

      if not (GetTrajectoryTransportKind(vTrajMuid) in RebuildTransportTypeSet) then
        Continue;

       ////// Временная херня отложенного перестроения траекторий ///////
       {$IFDEF GRAPH_POST_COMMIT}
       FPostTrajUniqueMuids.addItem(IntToStr (vTrajMuid), aGraphSectionMuid);
       Continue;
       {$ENDIF}
      ////// Временная херня отложенного перестроения траекторий ///////

      // Добавляем траекторию в запрос
      AddObjectToTicket(mgtDatasources[ 'RouteTrajectories' ], vTrajMuid, oEdit);
      // Добавляем линк
      AddGraphSectionTrajectoryObjectLink(aGraphSectionMuid, vTrajMuid);
  
      // Добавляем в траекторию массив траекторий
      vTrajectory := AddTrajectory(vTrajMuid);

      // Перепостраиваем траекторию
      if not vTrajectory.RebuildTrajectoryBySection(aGraphSectionMuid) then
        FailedRebuildRouteList.Add(getObjectFullName(mgtDatasources[ 'RouteTrajectories' ], vTrajMuid));

      // Сохраняем изменения в запрос
        vTrajectory.SaveToTicket();
      // Обновляем карточку
      cardsManager.refreshTrajectoryForm(vTrajectory.Muid);
    end;
  finally
    if (FailedRebuildRouteList.Count <> 0) then
      showDialog(dtAlert, dbsOK, 'Не удалось построить траекторию с идентификатором: ' + FailedRebuildRouteList.DelimitedText);

    FreeAndNil(vListTraj);
    FreeAndnIl (FailedRebuildRouteList);
  end;

  // Теперь ищем траектории, которые имеют эту дугу, но в тикете этих данных ещё нет
  try
    for i := 0 to FMapTrajectories.Count - 1 do
    begin
      vTrajectory := TMgtRouteTrajectory(FMapTrajectories.items[i]);
      // Траектория синхоронизированна с запросом, мы её уже обработали
      if vTrajectory.Saved then
        Continue;

      // В противном случае проверяем нужно ли перепостраивать эту траекторию
      for j := 0 to vTrajectory.SectionList.Count - 1 do
      begin
        if vTrajectory.SectionList.items[j] = aGraphSectionMuid then
        begin
           ////// Временная херня отложенного перестроения траекторий ///////
           {$IFDEF GRAPH_POST_COMMIT}
           FPostTrajUniqueMuids.addItem(inttoStr (vTrajectory.muid), aGraphSectionMuid);
            Continue;
          {$ENDIF}
          ////// Временная херня отложенного перестроения траекторий ///////

          // Наш клиент
          core.showPopupMessage('Построение несохранённых траекторий');
          // Добавляем линк
          AddGraphSectionTrajectoryObjectLink(aGraphSectionMuid, vTrajectory.Muid);

          vTrajectory.RebuildTrajectoryBySection(aGraphSectionMuid);
          vTrajectory.SaveToTicket(); // Точно ли сохранять в тикет изменения??
          // Обновляем карточку
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

  // получаем дуги которые начинаются или заканчиваются на переданный идентификатор узла
  vAdjasentSections1 := getObjects(datasource,dbaMapplFieldNames[mfStartNodeMUID],IntToStr(aGraphNodeMuid),['muid']);
  vAdjasentSections2 := getObjects(datasource,dbaMapplFieldNames[mfEndNodeMUID],IntToStr(aGraphNodeMuid),['muid']);

  // список идентификаторов затротутых секций
  vTrajUniqueMuids := TMapInt64.Create(mcIgnore,True);
  try
    // если оба списка пусты - до свиданья, этого объекта нет
    if not ( Assigned(vAdjasentSections1) and Assigned(vAdjasentSections2) ) then
      Exit;

    // обрабатываем 1 список
    if Assigned(vAdjasentSections1) then
    begin

      for i := 0 to vAdjasentSections1.Count -1 do
      begin
        vSectionMuid:= TlkJSONobject(vAdjasentSections1[i]).asString['muid'];

        GetTrajectoriesBySectionMuid(vSectionMuid,vTrajUniqueMuids,True);
      end;
    end;

    // обрабатываем 2 список
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
      core.showPopupMessage('Построение траекторий ' + IntToStr(i + 1) + ' из ' + IntToStr(vTrajUniqueMuids.Count));
      {$ENDIF}

      vTrajMuid := StrToInt64Def(vTrajUniqueMuids.keys[i],-1);
      vSectionMuidInt64 := vTrajUniqueMuids.items[i];

      ////// Временная херня отложенного перестроения траекторий ///////
      {$IFDEF GRAPH_POST_COMMIT}
      FPostTrajUniqueMuids.addItem(vTrajUniqueMuids.keys[i], vSectionMuidInt64);
      Continue;
      {$ENDIF}
      ////// Временная херня отложенного перестроения траекторий ///////

      if (vTrajMuid > 0) and (vSectionMuidInt64 > 0) then
      begin
        // Добавляем объект      
        AddObjectToTicket(mgtDatasources[ 'RouteTrajectories' ], vTrajMuid, oEdit);
        // Добавляем линк
        AddGraphSectionTrajectoryObjectLink(vSectionMuidInt64, vTrajMuid);
        // Добавляем в траекторию массив траекторий
        vTrajectory := AddTrajectory(vTrajMuid);

        // Перепостраиваем траекторию
        if not vTrajectory.RebuildTrajectoryBySectionList() then
          showDialog(dtAlert, dbsOK, 'Не удалось построить траекторию с идентификатором: ' + IntToStr(vSectionMuidInt64));
        // Сохраняем изменения в запрос
        vTrajectory.SaveToTicket();
        // Обновляем карточку
        cardsManager.refreshTrajectoryForm(vTrajectory.Muid);

        // Теперь ищем траектории, которые имеют эту дугу, но в тикете этих данных ещё нет
        for iCard := 0 to FMapTrajectories.Count - 1 do
        begin
          vTrajectory := TMgtRouteTrajectory(FMapTrajectories.items[i]);
          // Траектория синхоронизированна с запросом, мы её уже обработали
          if vTrajectory.Saved then
            Continue;

          // В противном случае проверяем нужно ли перепостраивать эту траекторию
          for j := 0 to vTrajectory.SectionList.Count - 1 do
          begin
            if vTrajectory.SectionList.items[j] = vSectionMuidInt64 then
            begin
              // Наш клиент
              core.showPopupMessage('Построение несохранённых траекторий');
              // Добавляем линк
              AddGraphSectionTrajectoryObjectLink(vSectionMuidInt64,vTrajMuid);

              // Перепостраиваем траекторию
              if not vTrajectory.RebuildTrajectoryBySectionList() then
                showDialog(dtAlert, dbsOK, 'Не удалось построить траекторию с идентификатором: ' + IntToStr(vTrajectory.Muid));

              vTrajectory.SaveToTicket(); // Точно ли сохранять в тикет изменения??
              // Обновляем карточку
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
* СheckIncorrctTrajectories
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
* RebuildAllTrajectories -- функция выполняет специфические действия - без спроса НЕ ВЫЗЫВАТЬ
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
    raise EMgtException.Create('Не удалось считать список перепостраиваемых траекторий');

  i := 1;
  AssignFile(vFile, 'Incorrect trajectories.txt');
  Append(vFile);
  while dbRes.Fetch do
  begin
    vMuid := dbRes.asInt64(0);
    if vMuid < 1 then
      Continue;

    showPopupMessage(IntToStr(i) + ' из ' + IntToStr(dbRes.numRows));
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
    raise EMgtException.Create('Не удалось считать список перепостраиваемых траекторий');

  if dbRes.numRows = 0 then
    showDialog(dtInfo, dbsOK, 'По заданным параметрам не найдено ни одной необработанной траектории нулевого рейса!');

  i := 1;
  while dbRes.Fetch() do
  begin
    vTrajectory := nil;
    vMuid := dbRes.asInt64(0);
    if vMuid < 1 then
      Continue;

    showPopupMessage(IntToStr(i) + ' из ' + IntToStr(dbRes.numRows));
    try
      AddObjectToTicket(mgtDatasources[ 'RouteTrajectories' ], vMuid, oEdit);
      vTrajectory := TMgtRouteTrajectory(FMapTrajectories.itemsByKey[IntToStr(vMuid)]);
      if not Assigned(vTrajectory) then
        Continue;

      //Изменение набора контрольных точек

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

      //Изменение набора контрольных точек

      if vTrajectory.RebuildTrajectory() then
      begin
        vTicketObject := vTrajectory.SaveToTicket();

        // преобразуем длины к 3му знаку после запятой
        // Если длина изменилась более чем на 100 метров, ставим уведомляющий статус
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
//        showDialog(dtError, dbsOK, 'Не удалось перепостроить траекторию! MUID: ' + IntToStr(vMuid));
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
* RecalculateTrajectoriesHash - пересчитать всем траекториям хэш (сервисная функция)
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
    raise EMgtException.Create('Не удалось считать список траекторий!');

  i := 1;
  try
    while dbRes.Fetch() do
    begin
      core.showPopupMessage('Вычисление хэша для траектории ' + IntToStr(i) + '...');
      vSql := 'SELECT lnk.stop_place_muid FROM gis_mgt.lnk_route_trajectories__stop_places lnk ' +
              ' WHERE lnk.route_trajectory_muid = ' + dbRes.asString(0) +
              ' AND lnk.sign_deleted = 0 ' +
              ' AND lnk.stop_mode_muid <> 5 ' +
              ' ORDER BY lnk.index ';

      if FConn.QueryOpen(vSql, dbRes2, false) <> 0 then
      begin
        showDialog(dtError, dbsOK, 'Не удалось считать список остановок траектории ' + dbRes.asString(0) + ' !');
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
          showDialog(dtError, dbsOK, 'Не удалось записать хэш траектории ' + dbRes.asString(0) + ' !');
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
* CreateAllStopGraphics -- функция выполняет специфические действия - без спроса НЕ ВЫЗЫВАТЬ
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
      raise EMgtException.Create('Ошибка при выполнении sql-запроса: ' + sql);

    while dbRes.Fetch do
    begin
      core.showPopupMessage('Построение графики остановки с OKEY ' + dbRes.asString(0));
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
      raise EMgtException.Create('Ошибка при выполнении SQL-запроса: ' + sql);

    while dbRes.Fetch do
    begin
      core.showPopupMessage('Привязка к графу МПВ с OKEY ' + dbRes.asString(0));
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
    raise EMgtException.Create('Список идентификаторов траекторий не задан!');

  vFailedCount := 0;

  try
    datasource := mgtDatasources[ 'RouteTrajectories' ];
    for i := 0 to aMuidList.Count - 1 do
    begin
      showPopupMessage('Добавление траекторий...' + IntToStr(i + 1) + ' из ' + IntToStr(aMuidList.Count));
      vMuid := aMuidList.items[i];
      try
        vTicketObject := AddObjectToTicket(datasource, vMuid, oEdit);
        vTrajectory := AddTrajectory(vMuid);

        //vRes := vTrajectory.RebuildTrajectoryBySectionList();
        //if not vRes then
          vRes := vTrajectory.RebuildTrajectory();

        vTrajectory.SaveToTicket();
        // Обновляем карточку
        cardsManager.refreshTrajectoryForm(vTrajectory.Muid);

        if not vRes then
        begin
          Inc(vFailedCount);
          vTrajectory.Correct := false;
        end;

        // преобразуем длины к 3му знаку после запятой
        // Если длина изменилась более чем на 20 метров, открываем карточку
        oldLength := UniStrToFloatDef(vTicketObject.MainData.GetInitialValue('length'), 0);
        newLength := UniStrToFloatDef(vTicketObject.MainData.GetActualValue('length'), 0);
        lengthDiff := Abs(Round(oldLength * 1000) - Round(newLength * 1000));
        if lengthDiff > TRAJ_MIN_VALUE_DIFF then
          vTrajectory.setStatus(mtosWarning);
      except
        showDialog(dtAlert, dbsOK, 'Не удалось добавить в запрос траекторию с идентификатором ' + IntToStr(vMuid));
        Continue;
      end;
    end;
  finally
    hidePopupMessage();
    if vFailedCount > 0 then
       showDialog(dtAlert, dbsOK, 'Не удалось перестроить траектории: ' + IntToStr(vFailedCount) + ' из ' + IntToStr(aMuidList.Count));
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
    core.showPopupMessage('Добавление павильона');

    if not (oAdd in mgtCards[ 'PosterApplications' ].Operations) then
      exit;

    if aStopPavilionMuid <= 0 then
      exit;

    vRefbookMUID := GetRefbookMUID(mgtDatasources[ 'PosterApplications' ]);
    vObjCount := core.Ticket.GetObjectsCountByRefbook(vRefbookMuid);

    if vObjCount > 1 then
    begin
      showDialog(dtInfo, dbsOk, 'В данный момент более одной заявки находятся в режиме редактирования!' +
                                #13#10 + 'Для добавления павильона в заявку оставьте не более одной.'  );
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
        showDialog(dtInfo, dbsOk, 'Данный павильон уже присутствует в открытой заявке: ' + vFields.itemsByKey['name'] + '!');
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
      setFieldValue(vObj.MainData, 'name', 'Новая заявка');
    end;
  //  for i := 0 to vObj.SlaveData.Count - 1 do
  //  begin
  //    vObjData := vObj.SlaveData.items[i];
  //    if vObjData.GetActualValue('stop_pavilion_muid') = IntToStr(aStopPavilionMuid) then
  //    begin
  //      showDialog(dtInfo, dbsOk, 'Данный павильон уже присутствует в заявке!');
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
    showDialog(dtError, dbsOK, 'Ошибка при выполнении SQL-запроса.', sql);
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
      // идентификатор рейса
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

  // получили идентификатор рейса, определились с тем где будем искать значения...
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

    // получаем значения, возвращаем результат...
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
// получить муиды прямой и обратной траектории (если нет -1)
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

  // получаем прямую и обратную траектории рейса или нулевого рейса
  jsonTrajectories := core.getObjects(mgtDatasources[ 'RouteTrajectories' ], roundFieldName, IntToStr(aRoundMuid),
    ['muid', 'trajectory_type_muid']);

  if jsonTrajectories.Count > 2 then
    raise Exception.Create('Для данного рейса существует больше двух траекторий');

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
      //raise EMgtException.Create('Ошибочный тип траектории');
  end;

  FreeAndNil(jsonTrajectories);
end;

{**********************************************************************************************
* GetStopPlaces
// получить муиды траекторий по муиду секции
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

  // если не передан флаг добавления - чистим исходный список
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
  // Получаем исходящие дуги
  vAdjasentSections := getObjects(datasource,dbaMapplFieldNames[mfStartNodeMUID],IntToStr(aNodeMuid),['muid']);
  for i := 0 to vAdjasentSections.Count -1 do
  begin
    vSectionMuid := StrToInt64(TlkJSONobject(vAdjasentSections[i]).asString['muid']);

    aSectionMuids.addItem(datasource.TableName, vSectionMuid);
  end;
  FreeAndNil(vAdjasentSections);

  // Получаем входящие дуги
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
// получить муиды мест посадки-высадки траектории
***********************************************************************************************}
function TMgtCore.GetStopPlaces(aTrajectoryMuid: int64): TMapInt64;
var
  jsonStopPlaces : TlkJSONlist;
  jsonObject : TlkJSONobject;
  i : integer;
  muid: int64;
begin
  Result := TMapInt64.Create();

  // сортируем по номеру остановки
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
// получить муид действующего варианта маршрута
***********************************************************************************************}
function TMgtCore.GetRouteCurrentVariant(ARouteMuid: Int64): int64;
begin
  Result := StrToInt64Def(getObjectValue(mgtDatasources[ 'Routes' ], ARouteMuid, 'current_route_variant_muid'), -1);
end;

{**********************************************************************************************
* GetRouteMainRoundByVariant
// получить муид основного рейса ('00') по муиду варианта
***********************************************************************************************}
function TMgtCore.GetRouteMainRoundByVariant(AVariantMuid: Int64): int64;
var
  i: integer;
  jsonRounds: TlkJSONlist;
begin
  Result := -1;
  if AVariantMuid <=0 then
    exit;

  // получаем муид основного рейса
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
// получить муид основного рейса ('00') действующего варианта по муиду маршрута
***********************************************************************************************}
function TMgtCore.GetRouteMainRound(ARouteMuid: Int64): int64;
var
  variantMuid: int64;
begin
  Result := -1;
  variantMuid := GetRouteCurrentVariant(ARouteMuid);

  if variantMuid = -1 then
    exit; // У данного маршрута не задан основной вариант

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
    exit; // У данного маршрута не задан основной вариант

  if aRoundType = rtrRound then
    vDatasource := mgtDatasources[ 'RouteRounds' ]
  else if aRoundType = rtrNullRound then
    vDatasource := mgtDatasources[ 'RouteNullRounds' ]
  else
    exit;

  // получаем муид основного рейса
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
    exit; // У данного маршрута не задан основной вариант или нет основного рейса

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
    raise EMgtException.Create('Не переданы идентификаторы дуг графа!');

  vFieldList := TMapStrings.Create();
  vFieldList.Add(MOS_TAG);
  vFieldList.Add('has_bus');
  vFieldList.Add('has_trolley');
  vFieldList.Add('has_tram');
  vFieldList.Add('startNodeMUID');
  vFieldList.Add('endNodeMUID');

  getObjectValues(mgtDatasources[ 'GraphSections' ], aEditedSection, vFieldList);
  // Получаем графику объекта
  vBase64Mos := vFieldList.itemsByKey[MOS_TAG];
  // Получаем новую длину дуги
  mapCore.GetGeometryLength(vBase64Mos, vEditedSectionLength, muM);

  vIsRegular := (vFieldList.itemsByKey['has_bus'] = '1') or
                (vFieldList.itemsByKey['has_trolley'] = '1');

  vIsTram    := (vFieldList.itemsByKey['has_tram'] = '1');

  // Перепривязываем остановки
  if vIsRegular then
    ReboundStopPlacesToGraph(aEditedSection, aNewSection, vEditedSectionLength);
  if vIsTram then
    ReboundStopPlacesToTramGraph(aEditedSection, aNewSection, vEditedSectionLength);

  vSrcNodeMuid  := StrToInt64Def(vFieldList.itemsByKey['startNodeMUID'], -1);
  vDestNodeMuid := StrToInt64Def(vFieldList.itemsByKey['endNodeMUID'], -1);
  // Копируем семантику узла
  if (vSrcNodeMuid > 0) and (vDestNodeMuid > 0) then
    CopyNodeData(vSrcNodeMuid, vDestNodeMuid);

  // Копируем семантику секции
  if (aEditedSection > 0) and (aNewSection > 0) then
    CopySectionData (aEditedSection, aNewSection);

  // Получаем траектории привязанные к этой дуге
  // Траектории в контейнере не уникальные, поскольку траектория может ходить по дуге несколько раз
  vTrajList := getObjects(mgtDatasources[ 'LnkRouteTrajectoriesGraphSections' ], 'graph_section_muid', IntToStr(aEditedSection),
                ['muid', 'route_trajectory_muid']);

  try
    vLastMuid := -1;
    for i := 0 to vTrajList.Count - 1 do
    begin
      core.showPopupMessage('Построение траекторий ' + IntToStr(i + 1) + ' из ' + IntToStr(vTrajList.Count));
      vTraj := vTrajList.asObject[i];
      vTrajMuid := StrToInt64(vTraj.asString['route_trajectory_muid']);
      // Эту траекторию уже обрабатывали, проходим
      if vLastMuid = vTrajMuid then
        Continue;

      vLastMuid := vTrajMuid;

      // Добавляем траекторию в запрос
      AddObjectToTicket(mgtDatasources[ 'RouteTrajectories' ], vTrajMuid, oEdit);
      // Добавляем линк
      AddGraphSectionTrajectoryObjectLink(aEditedSection, vTrajMuid);

      // Добавляем в траекторию массив траекторий
      vTrajectory := AddTrajectory(vTrajMuid);

      // Разбиваем дугу траектории
      vRes := vTrajectory.SplitSection(aEditedSection, aNewSection, vEditedSectionLength);

      // Сохраняем изменения в запрос
      vTrajectory.SaveToTicket();
      // Обновляем карточку
      cardsManager.refreshTrajectoryForm(vTrajectory.Muid);

      if not vRes then
      begin
        showDialog(dtAlert, dbsOK, 'Не удалось построить траекторию с идентификатором: ' + IntToStr(vTrajectory.Muid));
        vTrajectory.Correct := false;
      end;
    end;
  finally
    FreeAndNil(vTrajList);
  end;

  // Теперь ищем траектории, которые имеют эту дугу, но в тикете этих данных ещё нет
  try
    for i := 0 to FMapTrajectories.Count - 1 do
    begin
      vTrajectory := TMgtRouteTrajectory(FMapTrajectories.items[i]);
      // Траектория синхоронизированна с запросом, мы её уже обработали
      if vTrajectory.Saved then
        Continue;

      // В противном случае проверяем нужно ли перепостраивать эту траекторию
      for j := 0 to vTrajectory.SectionList.Count - 1 do
      begin
        if vTrajectory.SectionList.items[i] = aEditedSection then
        begin
          // Наш клиент
          core.showPopupMessage('Построение несохранённых траекторий');
          // Добавляем линк
          AddGraphSectionTrajectoryObjectLink(aEditedSection, vTrajectory.Muid);

          // Перепостраиваем траекторию
          vRes := vTrajectory.SplitSection(aEditedSection, aNewSection, vEditedSectionLength);

          // Сохраняем изменения в запрос
          vTrajectory.SaveToTicket();
          // Обновляем карточку
          cardsManager.refreshTrajectoryForm(vTrajectory.Muid);

          if not vRes then
          begin
            showDialog(dtAlert, dbsOK, 'Не удалось построить траекторию с идентификатором: ' + IntToStr(vTrajectory.Muid));
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


//Перерасчитать офсет у обычной дуги
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
  
  // Если начальный или конечный узел данной дуги в тикете есть а в базе нет (допущение)
  // То значит дуга бъется и перепривязка произойдет в splitSection

  getObjectValueFromTicket(dsSections, aSectionMuid, 'StartNodeMuid', vTempMuid);
  startNodeDB := getObjectValueFromDB(dsNodes, StrToInt64Def(vTempMuid, -1), 'muid', vTempMuid);

  getObjectValueFromTicket(dsSections, aSectionMuid, 'EndNodeMuid', vTempMuid);
  endNodeDB := getObjectValueFromDB(dsNodes, StrToInt64Def(vTempMuid, -1), 'muid', vTempMuid);

  // Если хотя бы одного узла нету в БД - значит дуга разбивается - выходим
  if (not startNodeDB) or (not endNodeDB) then
    Exit;

  // Получаем все привязанные к этой дуге места посадки-высадки
  dsStopPlaces := mgtDatasources[ 'StopPlaces' ];  
  vStopPlaces := getObjects(dsStopPlaces, 'graph_section_muid', IntToStr(aSectionMuid), ['muid']);

  for i := 0 to vStopPlaces.Count - 1 do
  begin
    vStopPlace := vStopPlaces.asObject[i];
    vMuid := StrToInt64(vStopPlace.asString['muid']);
    vOffset := mapcore.GetStopPlaceGraphSectionOffset(vMuid, aSectionMuid);

    //если обнаружились проблемы при пересчете офсета
     if vOffset < 0 then
     begin
       Result := false;
       exit;
     end;

    // Добавляем остановку в запрос с перепривязкой
    vTicketObj := AddObjectToTicket(dsStopPlaces, vMuid, oEdit);
    AddObjectLink(dsStopPlaces, vMuid, mgtDatasources[ 'GraphSections' ], aSectionMuid, [tltCascade, tltCommit, tltDenyDelete]);
    setFieldValue(vTicketObj.MainData, 'graph_section_offset', UniFormatFloat('0.###', vOffset));

  end;
  FreeAndNil(vStopPlaces);
end;

//Перерасчитать офсет у трамвайной дуги
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

  // Если начальный или конечный узел данной дуги в тикете есть а в базе нет (допущение)
  // То значит дуга бъется и перепривязка произойдет в splitSection

  getObjectValueFromTicket(dsSections, aSectionMuid, 'StartNodeMuid', vTempMuid);
  startNodeDB := getObjectValueFromDB(dsNodes, StrToInt64Def(vTempMuid, -1), 'muid', vTempMuid);

  getObjectValueFromTicket(dsSections, aSectionMuid, 'EndNodeMuid', vTempMuid);
  endNodeDB := getObjectValueFromDB(dsNodes, StrToInt64Def(vTempMuid, -1), 'muid', vTempMuid);

  // Если хотя бы одного узла нету в БД - значит дуга разбивается - выходим
  if (not startNodeDB) or (not endNodeDB) then
    Exit;

  // Получаем все привязанные к этой дуге места посадки-высадки
  dsStopPlaces := mgtDatasources[ 'StopPlaces' ];  
  vStopPlaces := getObjects(dsStopPlaces, 'graph_tram_section_muid', IntToStr(aSectionMuid), ['muid']);

  for i := 0 to vStopPlaces.Count - 1 do
  begin
    vStopPlace := vStopPlaces.asObject[i];
    vMuid := StrToInt64(vStopPlace.asString['muid']);
    vOffset := mapcore.GetStopPlaceGraphSectionOffset(vMuid, aSectionMuid);

    //если обнаружились проблемы при пересчете офсета
     if vOffset < 0 then
     begin
       Result := false;
       exit;
     end;

    // Добавляем остановку в запрос с перепривязкой
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
  // Получаем все привязанные к этой дуге места посадки-высадки
  vStopPlaces := getObjects(datasource, 'graph_section_muid', IntToStr(aSectionMuid),
                ['muid', 'graph_section_offset']);
  for i := 0 to vStopPlaces.Count - 1 do
  begin
    vStopPlace := vStopPlaces.asObject[i];
    vOffset := UniStrToFloatDef(vStopPlace.asString['graph_section_offset'], -1);
    // Оффсет не проставлен, ничего не делаем
    if vOffset < 0 then
      Continue;

    // Проверяем надо ли перепривязать место посадки-высадки на новую дугу
    if vOffset > aSectionLength then
    begin
      // Добавляем остановку в запрос с перепривязкой
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
  // Получаем все привязанные к этой дуге места посадки-высадки
  vStopPlaces := getObjects(datasource, 'graph_tram_section_muid', IntToStr(aSectionMuid),
                ['muid', 'graph_tram_section_offset']);
  for i := 0 to vStopPlaces.Count - 1 do
  begin
    vStopPlace := vStopPlaces.asObject[i];
    vOffset := UniStrToFloat(vStopPlace.asString['graph_tram_section_offset']);
    // Оффсет не проставлен, ничего не делаем
    if vOffset < 0 then
      Continue;

    // Проверяем надо ли перепривязать место посадки-высадки на новую дугу
    if vOffset > aSectionLength then
    begin
      // Добавляем остановку в запрос с перепривязкой
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
  //Получить с карты идентификаторы дуги и оффсеты для остановки
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
  // Слой траекторий
  vLayerCode := mgtDatasources[ 'RouteTrajectories' ].layerCode;

  // Варианты
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
    
  // Виды транспорта
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

  // Маршруты
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

  // Рейсы
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

  // Статусы маршрутов
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

  // Меняем формулу
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
  // Слой мест посадки\высадки
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

  // Меняем формулу
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
    // Вид транспорта
    vFieldList.addItem('filter_route_transport_kind', '');
    filterItemList := getFilterItemList(filter.Root, vFieldList);

    if Assigned(filterItemList) then
      filterItemList.Clear()
    else
      filterItemList := filter.Root.AddItemList(fboOr);

    vColumn := gridView.GetColumnByFieldName('filter_route_transport_kind');
    if StoredFiltration.TransportKindBus then
      filterItemList.addItem(vColumn, foEqual, 'А', 'А');
    if StoredFiltration.TransportKindTrolley then
      filterItemList.addItem(vColumn, foEqual, 'Тб', 'Тб');
    if StoredFiltration.TransportKindTram then
      filterItemList.addItem(vColumn, foEqual, 'Тм', 'Тм');

    if filterItemList.IsEmpty then
      filterItemList.addItem(vColumn, foEqual, Null, 'Пусто');

    if (StoredFiltration.TransportKindBus and
        StoredFiltration.TransportKindTrolley and
        StoredFiltration.TransportKindTram) then
      FreeAndNil(filterItemList);

    // Перевозчик
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
      filterItemList.addItem(vColumn, foEqual, Null, 'Пусто');

    if (StoredFiltration.RouteIsMGT and
        StoredFiltration.RouteIsCommercial) then
      FreeAndNil(filterItemList);

    // Маршрут
    vFieldList.Clear();
    vFieldList.addItem('route_state2', '');
    filterItemList := getFilterItemList(filter.Root, vFieldList);
    if Assigned(filterItemList) then
      filterItemList.Clear()
    else
      filterItemList := filter.Root.AddItemList(fboOr);

    vColumn := gridView.GetColumnByFieldName('route_state2');
    if StoredFiltration.RouteStateForApproval then
      filterItemList.addItem(vColumn, foEqual, 'на утверждении', 'на утверждении');
    if StoredFiltration.RouteStateOpened then
      filterItemList.addItem(vColumn, foEqual, 'открыт', 'открыт');
    if StoredFiltration.RouteStateTempOpened then
      filterItemList.addItem(vColumn, foEqual, 'временно открыт', 'временно открыт');
    if StoredFiltration.RouteStateTempClosed then
      filterItemList.addItem(vColumn, foEqual, 'временно закрыт', 'временно закрыт');
    if StoredFiltration.RouteStateClosed then
      filterItemList.addItem(vColumn, foEqual, 'закрыт', 'закрыт');

    if filterItemList.IsEmpty then
      filterItemList.addItem(vColumn, foEqual, Null, 'Пусто');

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

    // Недействующие
    if StoredFiltration.StopPlaceInactive then
    begin
      filterItemListTmp := filterItemList.AddItemList(fboAnd);

      vColumn := gridView.GetColumnByFieldName('has_bus');
      filterItemListTmp.addItem(vColumn, foEqual, '0', 'Нет');
      vColumn := gridView.GetColumnByFieldName('has_trolley');
      filterItemListTmp.addItem(vColumn, foEqual, '0', 'Нет');
      vColumn := gridView.GetColumnByFieldName('has_tram');
      filterItemListTmp.addItem(vColumn, foEqual, '0', 'Нет');
    end;

    // Действующие
    if StoredFiltration.StopPlaceActive then
    begin
      with filterItemList.AddItemList(fboAnd) do
      begin
        // Виды транспорта
        filterItemListTmp := AddItemList(fboOr);

        vColumn := gridView.GetColumnByFieldName('has_bus');
        if StoredFiltration.TransportKindBus  then
          filterItemListTmp.addItem(vColumn, foEqual, '1', 'Есть');
        vColumn := gridView.GetColumnByFieldName('has_trolley');
        if StoredFiltration.TransportKindTrolley then
          filterItemListTmp.addItem(vColumn, foEqual, '1', 'Есть');
        vColumn := gridView.GetColumnByFieldName('has_tram');
        if StoredFiltration.TransportKindTram then
          filterItemListTmp.addItem(vColumn, foEqual, '1', 'Есть');

        if filterItemListTmp.IsEmpty then
          filterItemListTmp.addItem(vColumn, foEqual, Null, 'Пусто');

        if (StoredFiltration.TransportKindBus and
          StoredFiltration.TransportKindTrolley and
          StoredFiltration.TransportKindTram) and
          (StoredFiltration.StopPlaceActive = StoredFiltration.StopPlaceInactive) then
          FreeAndNil(filterItemListTmp);

        // перевозчики
        filterItemListTmp := AddItemList(fboOr);
        vColumn := gridView.GetColumnByFieldName('has_mgt_routes');
        if StoredFiltration.RouteIsMGT then
          filterItemListTmp.addItem(vColumn, foEqual, '1', '1');
        vColumn := gridView.GetColumnByFieldName('has_commercial_routes');
        if StoredFiltration.RouteIsCommercial then
          filterItemListTmp.addItem(vColumn, foEqual, '1', '1');

        if filterItemListTmp.IsEmpty then
          filterItemListTmp.addItem(vColumn, foEqual, Null, 'Пусто');

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
      filterItemList.addItem(vColumn, foEqual, Null, 'Пусто');
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
    // Вид транспорта
    vFieldList.addItem('filter_route_transport_kind', '');
    filterItemList := getFilterItemList(filter.Root, vFieldList);

    if Assigned(filterItemList) then
      filterItemList.Clear()
    else
      filterItemList := filter.Root.AddItemList(fboOr);

    vColumn := gridView.GetColumnByFieldName('filter_route_transport_kind');
    if StoredFiltration.TransportKindBus then
      filterItemList.addItem(vColumn, foEqual, 'А', 'А');
    if StoredFiltration.TransportKindTrolley then
      filterItemList.addItem(vColumn, foEqual, 'Тб', 'Тб');
    if StoredFiltration.TransportKindTram then
      filterItemList.addItem(vColumn, foEqual, 'Тм', 'Тм');

    if filterItemList.IsEmpty then
      filterItemList.addItem(vColumn, foEqual, Null, 'Пусто');

    if (StoredFiltration.TransportKindBus and
        StoredFiltration.TransportKindTrolley and
        StoredFiltration.TransportKindTram) then
      FreeAndNil(filterItemList);

    // Перевозчик
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
      filterItemList.addItem(vColumn, foEqual, Null, 'Пусто');

    if (StoredFiltration.RouteIsMGT and
        StoredFiltration.RouteIsCommercial) then
      FreeAndNil(filterItemList);

    // Рейсы
    vFieldList.Clear();
    vFieldList.addItem('is_main_round', '');
    filterItemList := getFilterItemList(filter.Root, vFieldList);
    if Assigned(filterItemList) then
      filterItemList.Clear()
    else
      filterItemList := filter.Root.AddItemList(fboOr);

    vColumn := gridView.GetColumnByFieldName('is_main_round');
    if StoredFiltration.RoundMain then
      filterItemList.addItem(vColumn, foEqual, '1', 'Да');
    if StoredFiltration.RoundAdditional then
      filterItemList.addItem(vColumn, foEqual, '0', 'Нет');

    if filterItemList.IsEmpty then
      filterItemList.addItem(vColumn, foEqual, Null, 'Пусто');

    if (StoredFiltration.RoundMain and
        StoredFiltration.RoundAdditional) then
      FreeAndNil(filterItemList);

    // Маршрут
    vFieldList.Clear();
    vFieldList.addItem('route_state2', '');
    filterItemList := getFilterItemList(filter.Root, vFieldList);
    if Assigned(filterItemList) then
      filterItemList.Clear()
    else
      filterItemList := filter.Root.AddItemList(fboOr);

    vColumn := gridView.GetColumnByFieldName('route_state2');
    if StoredFiltration.RouteStateForApproval then
      filterItemList.addItem(vColumn, foEqual, 'на утверждении', 'на утверждении');
    if StoredFiltration.RouteStateOpened then
      filterItemList.addItem(vColumn, foEqual, 'открыт', 'открыт');
    if StoredFiltration.RouteStateTempOpened then
      filterItemList.addItem(vColumn, foEqual, 'временно открыт', 'временно открыт');
    if StoredFiltration.RouteStateTempClosed then
      filterItemList.addItem(vColumn, foEqual, 'временно закрыт', 'временно закрыт');
    if StoredFiltration.RouteStateClosed then
      filterItemList.addItem(vColumn, foEqual, 'закрыт', 'закрыт');

    if filterItemList.IsEmpty then
      filterItemList.addItem(vColumn, foEqual, null, 'Пусто');

    if (StoredFiltration.RouteStateForApproval and
        StoredFiltration.RouteStateOpened and
        StoredFiltration.RouteStateTempOpened and
        StoredFiltration.RouteStateTempClosed and
        StoredFiltration.RouteStateClosed) then
      FreeAndNil(filterItemList);

    // Варианты
    vFieldList.Clear();
    vFieldList.addItem('variant_state', '');
    filterItemList := getFilterItemList(filter.Root, vFieldList);
    if Assigned(filterItemList) then
      filterItemList.Clear()
    else
      filterItemList := filter.Root.AddItemList(fboOr);

    vColumn := gridView.GetColumnByFieldName('variant_state');
    if StoredFiltration.VariantActive then
      filterItemList.addItem(vColumn, foEqual, 'действующий', 'действующий');
    if StoredFiltration.VariantPlanned then
      filterItemList.addItem(vColumn, foEqual, 'планируемый', 'планируемый');
    if StoredFiltration.VariantArchive then
      filterItemList.addItem(vColumn, foEqual, 'архивный', 'архивный');

    if filterItemList.IsEmpty then
      filterItemList.addItem(vColumn, foEqual, null, 'Пусто');

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
    // Вид транспорта
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
        filterItemList.addItem(vColumn, foEqual, 'А', 'А');
      if StoredFiltration.TransportKindTrolley then
        filterItemList.addItem(vColumn, foEqual, 'Тб', 'Тб');
      if StoredFiltration.TransportKindTram then
        filterItemList.addItem(vColumn, foEqual, 'Тм', 'Тм');
    end;

    if filterItemList.IsEmpty then
      filterItemList.addItem(vColumn, foEqual, null, 'Пусто');

    if (StoredFiltration.TransportKindBus and
        StoredFiltration.TransportKindTrolley and
        StoredFiltration.TransportKindTram and
        StoredFiltration.RoundNull) then
      FreeAndNil(filterItemList);

    // Перевозчик
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
      filterItemList.addItem(vColumn, foEqual, null, 'Пусто');

    if (StoredFiltration.RouteIsMGT and
        StoredFiltration.RouteIsCommercial) then
      FreeAndNil(filterItemList);

    // Маршрут
    vFieldList.Clear();
    vFieldList.addItem('route_state2', '');
    filterItemList := getFilterItemList(filter.Root, vFieldList);
    if Assigned(filterItemList) then
      filterItemList.Clear()
    else
      filterItemList := filter.Root.AddItemList(fboOr);

    vColumn := gridView.GetColumnByFieldName('route_state2');
    if StoredFiltration.RouteStateForApproval then
      filterItemList.addItem(vColumn, foEqual, 'на утверждении', 'на утверждении');
    if StoredFiltration.RouteStateOpened then
      filterItemList.addItem(vColumn, foEqual, 'открыт', 'открыт');
    if StoredFiltration.RouteStateTempOpened then
      filterItemList.addItem(vColumn, foEqual, 'временно открыт', 'временно открыт');
    if StoredFiltration.RouteStateTempClosed then
      filterItemList.addItem(vColumn, foEqual, 'временно закрыт', 'временно закрыт');
    if StoredFiltration.RouteStateClosed then
      filterItemList.addItem(vColumn, foEqual, 'закрыт', 'закрыт');

    if filterItemList.IsEmpty then
      filterItemList.addItem(vColumn, foEqual, null, 'Пусто');

    if (StoredFiltration.RouteStateForApproval and
        StoredFiltration.RouteStateOpened and
        StoredFiltration.RouteStateTempOpened and
        StoredFiltration.RouteStateTempClosed and
        StoredFiltration.RouteStateClosed) then
      FreeAndNil(filterItemList);

    // Варианты
    vFieldList.Clear();
    vFieldList.addItem('variant_state', '');
    filterItemList := getFilterItemList(filter.Root, vFieldList);
    if Assigned(filterItemList) then
      filterItemList.Clear()
    else
      filterItemList := filter.Root.AddItemList(fboOr);

    vColumn := gridView.GetColumnByFieldName('variant_state');
    if StoredFiltration.VariantActive then
      filterItemList.addItem(vColumn, foEqual, 'действующий', 'действующий');
    if StoredFiltration.VariantPlanned then
      filterItemList.addItem(vColumn, foEqual, 'планируемый', 'планируемый');
    if StoredFiltration.VariantArchive then
      filterItemList.addItem(vColumn, foEqual, 'архивный', 'архивный');

    if filterItemList.IsEmpty then
      filterItemList.addItem(vColumn, foEqual, null, 'Пусто');

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
    // Номер ТО
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
            filterItemList.addItem(vColumnZel, foEqual, '0', 'Не согласовано');
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
    raise EMgtException.Create('Не удалось считать список нарядов');

  try
    while dbRes.Fetch() do
    begin
      showPopupMessage( 'Создание Json. Наряд...' + IntToStr(n) + ' из ' + IntToStr( dbRes.numRows() ) );
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
                raise EMgtException.Create('Не удалось считать список трафаретов');

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
* Получить строковое предстовление дней недели
* aDays - Строка перечислений дней недели вида: '1,2,3,4,5,6,7' (из Mysql)
*******************************************************************************}
function TMgtCore.GetDaysCaption(aDaysStr: string ): string;
const
  captionsFull  : Array [0..7] of String = ('', 'Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье');
  captionsShort : Array [0..7] of String = ('', 'Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс');
var
  list : TStringList;
  i : integer;

begin
  Result := 'Неопределено';

  if aDaysStr = '' then
    exit;

  if aDaysStr = '1,2,3,4,5,6,7' then
    Result := 'Единое'
  else if aDaysStr = '1,2,3,4,5' then
    Result := 'Будни'
  else if aDaysStr = '6,7' then
    Result := 'Выходные'
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
        // последний день
        if ( i = list.Count -1 ) then
        begin
          if ( StrToInt( list[i] ) = StrToInt( list[i-1] ) + 1 ) then
            Result := Result + '-' + captionsShort[ StrToInt( list[i] ) ]
          else
            Result := Result + ', ' + captionsShort[ StrToInt( list[i] ) ];

          Exit;
        end;

        // предыдущий день меньше на 1 и следующий больше на 1
        if ( StrToInt( list[i] ) = StrToInt( list[i-1] ) + 1 )
          and ( StrToInt( list[i] ) = StrToInt( list[i+1] ) - 1 ) then
        begin
          Continue;
        end
         // предыдущий день меньше на 1, а следующий не равен +1 к нынещнему
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
         // предыдущий не равен -1 к нынещнему, а следующий больше на 1
        else if ( StrToInt( list[i] ) <> StrToInt( list[i-1] ) + 1 )
              and ( StrToInt( list[i] ) = StrToInt( list[i+1] ) - 1 ) then
        begin
          Result := Result + ', '  + captionsShort[ StrToInt( list[i] ) ];
        end
         // предыдущий не равен -1 к нынещнему и следующий не равен +1 к нынещнему
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
    raise EMgtException.Create('Ошибка при получении объектов из БД.' + #13#10 + sql);

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
// Получить кол-во нарядов на изготовление трафаретов, доступных данному пользователя
***********************************************************************************************}
function TMgtCore.getUserTaskCount(): integer;
var
  sql : string;
  dbRes: TDbResult;
  tdo : ETDO;
  // список статусов нарядов, которые доступны для обработки данным пользовтелем (в соответствии с правами)
  statuses : TMapStrings;
  // список ТДО, остановки которых доступны данному пользователю
  tdos : TMapStrings;
  canApproveRoutesCondition, canApproveSchedulesCondition, canApproveSignpostsCondition : string;
  canSendToDLSCondition: string;
  tdoCondition, statusCondition, operatorCondition : string;

begin
  Result := 0;

  if (conn = nil) or (not conn.flConnected) then
    exit;

  // по правам пользователя формируем условия для sql-запроса
  statuses := TMapStrings.Create(mcReplace);
  tdos := TMapStrings.Create(mcReplace);

  operatorCondition := '1';
  canApproveRoutesCondition := '0';
  canApproveSchedulesCondition := '0';
  canApproveSignpostsCondition := '0';
  canSendToDLSCondition := '0';

//  право утвердить маршруты
//  статус = 2 (на формировании) и не утверждены маршруты
  if FUserPermissions.Signposts.flApproveRoutesMGT or
     FUserPermissions.Signposts.flApproveRoutesComm then
  begin
    statuses.addItem('2', '');
    canApproveRoutesCondition := '(routes_approved = 0)';
  end;

//  право утвердить расписания
//  статус = 2 (на формировании) и не утверждены расписания
//  TODO: нужно делать более сложный запрос для оповещений, чтобы зеленоградским не сыпались московские,
//        а московским - зеленоградские 
  if FUserPermissions.Signposts.flApproveSchedulesMSC or
     FUserPermissions.Signposts.flApproveSchedulesZelAK or
     FUserPermissions.Signposts.flApproveSchedulesComm then
  begin
    statuses.addItem('2', '');
    canApproveSchedulesCondition := '(schedules_approved = 0)';
  end;

//  право утвердить трафареты наряда, подтвердить выполнение наряда
//  статус = 2 (на формировании) и в наряде есть трафареты данного ТДО, которые не утверждены
//  или статус = 9 (исполнен) и в наряде есть трафареты данного ТДО, которые не подтверждены
  for tdo := Low(ETDO) to High(ETDO) do
  begin
    if tdo = tdoUndefined then
      continue;

    statuses.addItem('2', '');
    statuses.addItem('9', '');

//ОСТАЛОСЬ ДОПИСАТЬ, чтобы показывались только те наряды, которые находятся
//в ведении конкретного ТДО и хотя бы одна из остановок не утверждена или хотя бы по одной нет подтверждения выполнения наряда
    canApproveSignpostsCondition := '(signposts_approved = 0)';

    if FUserPermissions.Signposts.flApproveSignpostsByTDO[tdo] then
      tdos.addItem(IntToStr( Integer(tdo) ), '');
  end;

//  право Отправить в ДЛС
//  статус = 2 (на формировании) и все утверждено
  if FUserPermissions.Signposts.flSendTasksToDLS then
  begin
    statuses.addItem('2', '');
    canSendToDLSCondition := '( (routes_approved = 1) AND (schedules_approved = 1) AND (signposts_approved = 1) )'
  end;

//  право назначить оператора
//  статус = 3 (cформирован)
  if FUserPermissions.Signposts.flAssignDlsOperator then
  begin
    statuses.addItem('3', '');
  end;

//  право генерации макетов
//  статус = 4 (подготовка макетов) и operator_muid = текущий пользователь
  if FUserPermissions.Signposts.flLayoutPreparing then
  begin
    statuses.addItem('4', '');
    operatorCondition := '(t.operator_muid = ' + IntToStr(FAuth.User.MUID) + ')';
  end;

//  право Отправить на печать - ... - пометить наряд исполненным
//  статус = 5, 6, 7, 8 (готов к печати - ... - выдан для монтажа) и operator_muid = текущий пользователь
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

  // формируем параметры sql-запроса
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
// загрузить расписания по маршруту (при помощи службы загрузки ЭП)
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

    showPopupMessage('Загрузка расписаний...');

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

      msg := 'Загрузка расписаний завершена.'
            + #13#10 + 'Ошибки в расписаниях: ' + IntToStr(errorCount) + '.';

      msg2 := 'Время загрузки: ' + timeSecs + '.';

      if flSuccess and (errorCount > 0) then
      begin
        // пришел файл с ошибками, закрываем (сохраняем) его
        FreeAndNil(oStream);

        msg := msg + #13#10 + 'Открыть файл с ошибками?';
        res := showDialog(dtInfo, dbsYesNo, msg, msg2);

        if (res = IDYES) then
        begin
          ShellExecute(0, 'open', PChar(fileName), nil, nil, SW_SHOWNORMAL) ;
        end;
      end
      else
      begin
        // html-файла с ошибками в данных нет, есть сообщение, считываем его, удаляем файл
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
          showDialog(dtError, dbsOK, 'Ошибка при загрузке расписаний', msg2 + #13#10 + contentText);
        end;
      end;

      Result := flSuccess and (errorCount = 0);
    end
    else
    begin
      SetLength(contentText, oStream.Size);
      oStream.Position := 0;
      oStream.Read(Pointer(contentText)^, oStream.Size);

      msg2 := 'Ответный код: ' + IntToStr(httpClient.ResponseCode)
          + #13#10 + httpClient.ResponseText;

      showDialog(dtError, dbsOK, 'Ошибка при загрузке расписаний', url + #13#10 + msg2 + #13#10 + contentText);

      FreeAndNil(oStream);
      DeleteFile(fileName);
    end;

  except
    on e: Exception do
    begin
      msg2 := 'Ответный код: ' + IntToStr(httpClient.ResponseCode)
          + #13#10 + httpClient.ResponseText;

      if e.Message <> '' then
        msg2 := msg2 + '. Сообщение: ' + e.message;

      showDialog(dtError, dbsOK, 'Ошибка при загрузке расписаний', url + #13#10 + msg2);
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
    raise EMgtException.Create('Ошибка при получении объектов из БД.' + #13#10 + sql);

  while dbRes.Fetch() do
    stopPlaces.add(dbRes.asString(0));

  FreeAndNil(dbRes);

  MOS := TMapObjectStructure.Create();

  sql := 'SELECT rnr.muid, rt.muid, IF(rnr.park_1_muid IS NOT NULL, 1, 2) FROM gis_mgt.route_null_rounds rnr INNER JOIN gis_mgt.route_trajectories rt ' +
         'ON rt.route_null_round_muid = rnr.muid ' +
         'WHERE rnr.sign_deleted = 0 AND rt.sign_deleted = 0 AND ' +
         '((rnr.park_1_muid IS NOT NULL AND rnr.stop_place_A_muid IS NULL) OR (rnr.park_2_muid IS NOT NULL AND rnr.stop_place_B_muid IS NULL))';

  if (conn.QueryOpen(sql, dbRes, true) <> 0) then
    raise EMgtException.Create('Ошибка при получении объектов из БД.' + #13#10 + sql);

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
        raise EMgtException.Create('Ошибка при получении объектов из БД.' + #13#10 + sql);
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
    raise EMgtException.Create('Ошибка при получении объектов из БД.' + #13#10 + sql);

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
            raise EMgtException.Create('Ошибка при получении объектов из БД.' + #13#10 + sql);
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
            raise EMgtException.Create('Ошибка при получении объектов из БД.' + #13#10 + sql);
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
      raise Exception.Create( 'Не выполнен запрос URL = ' + aURL )
    else
    begin
      Result := TNativeXml.Create();
      Result.LoadFromFile( fileName );
    end;

    DeleteFile( fileName );
  except
    on e: Exception do
    begin
      msg := 'Ошибка при попытке запроса. ';
      if oStream <> nil then
        msg := msg + 'Размер файла: ' + IntToStr( oStream.size );
      if e.Message <> '' then
        msg := msg + '. Сообщение: ' + e.message;

      FreeAndNil( ostream );
      DeleteFile( fileName );
    end;
  end;

  // Ретрай
  if ( aFlRetry ) then
    for i := 1 to 5 do
      if ( Result = nil ) then
      begin
        sleep(1000);    // 1 сек
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
    raise Exception.Create('Ошибка при получении sql-запроса с алиасом ' + aSqlAlias + ' из файла ' + aXML);
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
    showDialog(dtError, dbsOK, 'Не удалось выполнить SQL-запрос.', sql);
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
    showDialog(dtError, dbsOK, 'Не удалось выполнить SQL-запрос.', sql);
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

        // необходимо проверить сначала первую остановку, затем последнюю
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
  Result := #10#13 + getSpace(aLevel) + '   (Было "' + src + '" --- Стало "' + dest + '")';
end;

{*******************************************************************************
* compareValueChange
*******************************************************************************}
function TMgtCore.compareValueChange(src, dest: int64; aLevel : integer): string;
begin
  Result := #10#13 + getSpace(aLevel) + '   (Было "' + IntToStr(src) + '" --- Стало "' + IntToStr(dest) + '")';
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
    Result := 'нет';

    if aBool then
      Result := 'да';
  end;

begin
  Result := #10#13 + getSpace(aLevel) + '   (Было "' + BoolToStrRus(src) + '" --- Стало "' + BoolToStrRus(dest) + '")';
end;

{*******************************************************************************
* compareValueChange
*******************************************************************************}
function TMgtCore.compareValueChange(src, dest: TDate; aLevel : integer): string;
begin
  Result := #10#13 + getSpace(aLevel) + '   (Было ';
  if src < 0 then
    Result := Result + '<пусто>'
  else
    Result := Result + '"' + DateTimeToStr(src) + '"';

  Result := Result + ' --- Стало ';
  if dest < 0 then
    Result := Result + '<пусто>'
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
