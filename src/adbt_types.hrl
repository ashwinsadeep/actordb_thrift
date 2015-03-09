-ifndef(_adbt_types_included).
-define(_adbt_types_included, yeah).

-define(ADBT_ERRORCODE_NOTLOGGEDIN, 1).
-define(ADBT_ERRORCODE_EMPTYACTORNAME, 2).
-define(ADBT_ERRORCODE_INVALIDACTORNAME, 3).
-define(ADBT_ERRORCODE_INVALIDTYPE, 4).
-define(ADBT_ERRORCODE_NOTPERMITTED, 5).
-define(ADBT_ERRORCODE_SQLERROR, 6).
-define(ADBT_ERRORCODE_ERROR, 7).

%% struct 'Val'

-record('Val', {'bigint' :: integer(),
                'integer' :: integer(),
                'smallint' :: integer(),
                'real' :: float(),
                'bval' :: boolean(),
                'text' :: string() | binary(),
                'isnull' :: boolean()}).
-type 'Val'() :: #'Val'{}.

%% struct 'ReadResult'

-record('ReadResult', {'success' :: boolean(),
                       'hasMore' :: boolean(),
                       'columns' = [] :: list(),
                       'rows' = [] :: list()}).
-type 'ReadResult'() :: #'ReadResult'{}.

%% struct 'WriteResult'

-record('WriteResult', {'error' :: string() | binary(),
                        'lastChangeRowid' :: integer(),
                        'rowsChanged' :: integer()}).
-type 'WriteResult'() :: #'WriteResult'{}.

%% struct 'LoginResult'

-record('LoginResult', {'success' :: boolean(),
                        'error' :: string() | binary(),
                        'readaccess' :: list(),
                        'writeaccess' :: list()}).
-type 'LoginResult'() :: #'LoginResult'{}.

%% struct 'Result'

-record('Result', {'read' :: 'ReadResult'(),
                   'write' :: 'WriteResult'()}).
-type 'Result'() :: #'Result'{}.

%% struct 'InvalidRequestException'

-record('InvalidRequestException', {'code' :: integer(),
                                    'info' :: string() | binary()}).
-type 'InvalidRequestException'() :: #'InvalidRequestException'{}.

-endif.
