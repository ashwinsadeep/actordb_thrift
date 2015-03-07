%%
%% Autogenerated by Thrift Compiler (0.9.2)
%%
%% DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
%%

-module(adbt_types).

-include("adbt_types.hrl").

-export([struct_info/1, struct_info_ext/1]).

struct_info('Val') ->
  {struct, [{1, i64},
          {2, i32},
          {3, i16},
          {4, double},
          {5, bool},
          {6, string},
          {7, i32}]}
;

struct_info('ReadResult') ->
  {struct, [{1, bool},
          {2, string},
          {3, bool},
          {4, {list, string}},
          {5, {list, {map, string, {struct, {'adbt_types', 'Val'}}}}}]}
;

struct_info('WriteResult') ->
  {struct, [{1, bool},
          {2, string},
          {3, i64},
          {4, i64}]}
;

struct_info('LoginResult') ->
  {struct, [{1, bool},
          {2, string},
          {3, {list, string}},
          {4, {list, string}}]}
;

struct_info('Result') ->
  {struct, [{1, {struct, {'adbt_types', 'ReadResult'}}},
          {2, {struct, {'adbt_types', 'WriteResult'}}}]}
;

struct_info('InvalidRequestException') ->
  {struct, [{1, i32},
          {2, string}]}
;

struct_info(_) -> erlang:error(function_clause).

struct_info_ext('Val') ->
  {struct, [{1, undefined, i64, 'bigint', undefined},
          {2, undefined, i32, 'integer', undefined},
          {3, undefined, i16, 'smallint', undefined},
          {4, undefined, double, 'real', undefined},
          {5, undefined, bool, 'bval', undefined},
          {6, undefined, string, 'text', undefined},
          {7, undefined, i32, 'isnull', undefined}]}
;

struct_info_ext('ReadResult') ->
  {struct, [{1, required, bool, 'success', undefined},
          {2, optional, string, 'error', undefined},
          {3, optional, bool, 'hasMore', undefined},
          {4, optional, {list, string}, 'columns', []},
          {5, optional, {list, {map, string, {struct, {'adbt_types', 'Val'}}}}, 'rows', []}]}
;

struct_info_ext('WriteResult') ->
  {struct, [{1, required, bool, 'success', undefined},
          {2, optional, string, 'error', undefined},
          {3, optional, i64, 'lastChangeRowid', undefined},
          {4, optional, i64, 'rowsChanged', undefined}]}
;

struct_info_ext('LoginResult') ->
  {struct, [{1, required, bool, 'success', undefined},
          {2, optional, string, 'error', undefined},
          {3, optional, {list, string}, 'readaccess', []},
          {4, optional, {list, string}, 'writeaccess', []}]}
;

struct_info_ext('Result') ->
  {struct, [{1, undefined, {struct, {'adbt_types', 'ReadResult'}}, 'read', #'ReadResult'{}},
          {2, undefined, {struct, {'adbt_types', 'WriteResult'}}, 'write', #'WriteResult'{}}]}
;

struct_info_ext('InvalidRequestException') ->
  {struct, [{1, required, i32, 'code', undefined},
          {2, required, string, 'info', undefined}]}
;

struct_info_ext(_) -> erlang:error(function_clause).

