-module(adbt).
-export([start/1]).
% thrift callbacks
-export([handle_error/2, handle_function/2]).
-include_lib("adbt_types.hrl").
-include_lib("adbt_constants.hrl").
-compile([{parse_transform, lager_transform}]).

%% API
start(Port) ->
	lager:info("Starting thrift on ~p",[Port]),
    thrift_server:start_link(Port,actordb_thrift,?MODULE).


handle_error(_Func,_Reason) ->
	ok.

% Process dictionary:
% {pb,State} - backpressure state
handle_function(login,{_U,_P}) ->
	State = actordb_backpressure:start_caller(),
	put(bp,State),
	Types = [atom_to_binary(A,latin1) || A <- actordb:types()],
	{reply,#'LoginResult'{success = true, readaccess = Types, writeaccess = Types}};
handle_function(exec_single,{Actor,Type,Sql,Flags}) ->
	Bp = backpressure(),
	exec_res(Sql,(catch actordb:exec_bp(Bp,Actor,Type,flags(Flags),Sql)));
handle_function(exec_multi,{Actors,Type,Sql,Flags}) ->
	Bp = backpressure(),
	exec_res(Sql,(catch actordb:exec_bp(Bp,Actors,Type,flags(Flags),Sql)));
handle_function(exec_all,{Type,Sql,Flags}) ->
	Bp = backpressure(),
	exec_res(Sql,(catch actordb:exec_bp(Bp,$*,Type,flags(Flags),Sql)));
handle_function(exec_sql,{Sql}) ->
	Bp = backpressure(),
	exec_res(Sql,(catch actordb:exec_bp(Bp,Sql)));
handle_function(protocol_version,[]) ->
	{reply,?ADBT_VERSION}.

flags([H|T]) ->
	actordb_sqlparse:check_flags(H,[])++flags(T);
flags([]) ->
	[].

val(V) when is_binary(V); is_list(V) ->
	#'Val'{text = V};
val(V) when is_integer(V) ->
	#'Val'{bigint = V};
val(V) when is_float(V) ->
	#'Val'{real = V};
val(undefined) ->
	#'Val'{isnull = true};
val(V) when V == true; V == false ->
	#'Val'{bval = V}.


exec_res(_Sql,{_WhatNow,{ok,[{columns,Cols1},{rows,Rows1}]}}) ->
	Cols = tuple_to_list(Cols1),
	Rows = [maps:from_list(lists:zip(Cols,[val(Val) || Val <- tuple_to_list(R)])) || R <- Rows1],
	{reply,#'Result'{read = #'ReadResult'{hasMore = false,columns = Cols, rows = Rows}}};
exec_res(_Sql,{_WhatNow,{ok,{changes,LastId,NChanged}}}) ->
	{reply,#'Result'{write = #'WriteResult'{lastChangeRowid = LastId, rowsChanged = NChanged}}};
exec_res(Sql,{'EXIT',Exc}) ->
	lager:error("Query exception: ~p for sql: ~p",[Exc,Sql]),
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_ERROR, info = ""});
exec_res(_Sql,{error,empty_actor_name}) ->
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_EMPTYACTORNAME, info = ""});
exec_res(_Sql,{unknown_actor_type,Type}) ->
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_INVALIDTYPE, info = [Type," is not a valid type."]});
exec_res(_Sql,{error,invalid_actor_name}) ->
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_INVALIDACTORNAME, info = ""});
exec_res(_Sql,{error,Err}) ->
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_ERROR, info = atom_to_list(Err)});
exec_res(_Sql,{ok,{error,E}}) ->
	exec_res(_Sql,{error,E}).

backpressure() ->
	Bp = get(bp),
	case Bp of
		undefined ->
			throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_NOTLOGGEDIN, info = ""});
		_ ->
			ok
	end,
	case actordb:check_bp() of
		sleep ->
			actordb:sleep_bp(Bp),
			backpressure();
		ok ->
			Bp
	end.

