-module(adbt).
-export([start/1]).
% thrift callbacks
-export([handle_error/2, handle_function/2]).
-include_lib("adbt/src/adbt_types.hrl").
-include_lib("adbt/src/adbt_constants.hrl").
-export ([prepare/1]).
%% API
start(Port) ->
	thrift_server:start_link(Port,actordb_thrift,?MODULE).


handle_error(_Func,_Reason) ->
	ok.

% Process dictionary:
% {pb,State} - backpressure state
handle_function(login,{U,P}) ->
	put(adbt,true),
	case catch actordb_backpressure:start_caller(U,P) of
		State when element(1,State) == caller ->
			put(bp,State),
			case actordb:types() of
				schema_not_loaded ->
					{reply,#'LoginResult'{success = true, readaccess = undefined, writeaccess = undefined}};
				_ ->
					Types = [atom_to_binary(A,latin1) || A <- actordb:types()],
					{reply,#'LoginResult'{success = true, readaccess = Types, writeaccess = Types}}
			end;
		_ ->
			exec_res(ok,{error,invalid_login})
	end;
handle_function(exec_config,{Sql}) ->
	T = actordb:types(),
	put(adbt,true),
	case get(bp) of
		undefined when T == schema_not_loaded ->
			ok;
		undefined ->
			throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_NOTLOGGEDIN, info = ""});
		_ ->
			ok
	end,
	exec_res(Sql,{ok,(catch actordb_config:exec(get(bp),Sql))});
handle_function(exec_schema,{Sql}) ->
	put(adbt,true),
	Bp = backpressure(),
	actordb_backpressure:save(Bp,canempty,true),
	exec_res(Sql,{ok,(catch actordb_config:exec_schema(Bp,Sql))});
handle_function(exec_single,{Actor,Type,Sql,Flags}) ->
	Bp = backpressure(),
	exec_res(Sql,(catch actordb:exec_bp(Bp,Actor,Type,flags(Flags),Sql)));
handle_function(exec_single_prepare,{Actor,Type,Sql,Flags,BindingVals0}) ->
	Bp = backpressure(),
	BindingVals = prepare(BindingVals0),
	exec_res(Sql,(catch actordb:exec_bp(Bp,Actor,Type,flags(Flags),Sql,BindingVals)));
handle_function(exec_multi,{Actors,Type,Sql,Flags}) ->
	Bp = backpressure(),
	exec_res(Sql,(catch actordb:exec_bp(Bp,Actors,Type,flags(Flags),Sql)));
handle_function(exec_multi_prepare,{Actors,Type,Sql,Flags,BindingVals0}) ->
	Bp = backpressure(),
	BindingVals = prepare(BindingVals0),
	exec_res(Sql,(catch actordb:exec_bp(Bp,Actors,Type,flags(Flags),Sql,BindingVals)));
handle_function(exec_all,{Type,Sql,Flags}) ->
	Bp = backpressure(),
	exec_res(Sql,(catch actordb:exec_bp(Bp,$*,Type,flags(Flags),Sql)));
handle_function(exec_all_prepare,{Type,Sql,Flags,BindingVals0}) ->
	Bp = backpressure(),
	BindingVals = prepare(BindingVals0),
	exec_res(Sql,(catch actordb:exec_bp(Bp,$*,Type,flags(Flags),Sql,BindingVals)));
handle_function(exec_sql,{Sql}) ->
	Bp = backpressure(),
	R = (catch actordb:exec_bp(Bp,Sql)),
	exec_res(Sql,R);
handle_function(exec_sql_prepare,{Sql, BindingVals0}) ->
	Bp = backpressure(),
	BindingVals = prepare(BindingVals0),
	exec_res(Sql,(catch actordb:exec_bp(Bp,Sql,BindingVals)));
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

% exec_res(_Sql,{_WhatNow,ok}) ->
% 	Cols = [],
% 	Rows = [#{}],
% 	{reply,#'Result'{rdRes = #'ReadResult'{hasMore = false,columns = Cols, rows = Rows}}};
exec_res(_Sql,{_WhatNow,{ok,[{columns,[]},{rows,[]}]}}) ->
	Cols = [],
	Rows = [#{}],
	{reply,#'Result'{rdRes = #'ReadResult'{hasMore = false,columns = Cols, rows = Rows}}};
exec_res(_Sql,{_WhatNow,{ok,[{columns,Cols1},{rows,Rows1}]}}) ->
	Cols = tuple_to_list(Cols1),
	Rows = [maps:from_list(lists:zip(Cols,[val(Val) || Val <- tuple_to_list(R)])) || R <- Rows1],
	{reply,#'Result'{rdRes = #'ReadResult'{hasMore = false,columns = Cols, rows = Rows}}};
exec_res(_Sql,{_WhatNow,{ok,{changes,LastId,NChanged}}}) ->
	{reply,#'Result'{wrRes = #'WriteResult'{lastChangeRowid = LastId, rowsChanged = NChanged}}};
exec_res(_Sql,{'EXIT',_Exc}) ->
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_ERROR, info = ""});
exec_res(_Sql,{error,empty_actor_name}) ->
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_EMPTYACTORNAME, info = ""});
exec_res(_Sql,{unknown_actor_type,Type}) ->
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_INVALIDTYPE, info = [Type," is not a valid type."]});
exec_res(_Sql,{error,invalid_actor_name}) ->
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_INVALIDACTORNAME, info = ""});
exec_res(_Sql,{error,consensus_timeout}) ->
	I = "Cluster can not reach consensus at this time. Query was not executed.",
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_CONSENSUSTIMEOUT, info = I});

exec_res(_Sql,{error,no_permission}) ->
	I = "User lacks permission for this query.",
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_NOTPERMITTED, info = I});
exec_res(_Sql,{error,invalid_login}) ->
	I = "Username and/or password incorrect.",
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_LOGINFAILED, info = I});
exec_res(_Sql,{error,local_node_missing}) ->
	I = "This node is not a part of supplied node list.",
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_LOCALNODEMISSING, info = I});
exec_res(_Sql,{error,missing_group_insert}) ->
	I = "No valid groups for initialization.",
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_MISSINGGROUPINSERT, info = I});
exec_res(_Sql,{error,missing_nodes_insert}) ->
	I = "No valid nodes for initalization.",
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_MISSINGNODESINSERT, info = I});
exec_res(_Sql,{error,missing_root_user}) ->	
	I = "No valid root user for initialization",
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_MISSINGROOTUSER, info = I});

exec_res(_Sql,{error,Err}) when is_tuple(Err) ->
	I = iolist_to_binary([butil:tolist(E)++" "||E<-tuple_to_list(Err)]),
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_SQLERROR, info = I});
exec_res(_Sql,{error,Err}) ->
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_ERROR, info = butil:tolist(Err)});
exec_res(_Sql,{ok,{sql_error,E,_Description}}) ->
	exec_res(_Sql,{error,E});
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

prepare(Prepare)->
	prepare(Prepare,[]).
prepare([H|T],Acc)->
	prepare(T,[list_to_tuple([actordb_client:resp(Val)||Val<-[ table| H ] ])|Acc]);
prepare([],Acc)->
 	[lists:reverse(Acc)].
