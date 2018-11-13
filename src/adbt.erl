-module(adbt).
-export([start/1]).
% thrift callbacks
-export([handle_error/2, handle_function/2]).
-include_lib("adbt/include/adbt_types.hrl").
-include_lib("adbt/include/adbt_constants.hrl").
-export ([prepare/1]).
%% API
start(Port) ->
	case application:get_env(thrift, network_interface) of
		{ok, Value} ->
			case inet:parse_address(Value) of
				{ok, IPAddress} ->
					ok;
				_ ->
					{ok, {hostent, _, [], inet, _, [IPAddress]}} = inet:gethostbyname(Value)
			end;
		_ ->
			IPAddress = false
	end,
	{ok, Addresses} = inet:getif(),
	case lists:keyfind(IPAddress, 1, Addresses) of
		false ->
			IPAddr = [];
		_ ->
			IPAddr = [{ip,IPAddress}]
	end,
	case application:get_env(actordb_core,client_inactivity_timeout) of
		{ok,RTimeout} when RTimeout > 0 ->
			ok;
		_ ->
			RTimeout = infinity
	end,
	case application:get_env(actordb_core, thrift_framed) of
		{ok,FramedVal} when FramedVal == true; FramedVal == false ->
			Framed = [{framed, FramedVal}];
		_ ->
			Framed = []
	end,
	thrift_socket_server:start([{handler, ?MODULE},
		{service, actordb_thrift},
		{socket_opts,[{recv_timeout, RTimeout}]},
		{port, Port}] ++ IPAddr ++ Framed).

handle_error(_,closed) ->
	ok;
handle_error(_,timeout) ->
	ok;
handle_error(Func,Reason) ->
	error_logger:format("Thrift error: func=~p, reason=~p~n",[Func, Reason]),
	ok.

% Process dictionary:
% {pb,State} - backpressure state
handle_function(login,{U,P}) ->
	put(adbt,true),
	case get(salt) of
		undefined ->
			put(salt,<<>>);
		_ ->
			ok
	end,
	case catch actordb_backpressure:start_caller(U,P,get(salt)) of
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
handle_function(salt,_) ->
	put(salt,crypto:strong_rand_bytes(20)),
	{reply,get(salt)};
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
	case actordb:types() of
		schema_not_loaded ->
			exec_res(Sql,{error,not_initialized});
		_ ->
			Bp = backpressure(),
			actordb_backpressure:save(Bp,canempty,true),
			exec_res(Sql,{ok,(catch actordb_config:exec_schema(Bp,Sql))})
	end;
handle_function(exec_single,{Actor,Type,Sql,Flags}) ->
	Bp = backpressure(),
	R = (catch actordb:exec_bp(Bp,Actor,Type,flags(Flags),Sql)),
	exec_res(Sql,R);
handle_function(exec_single_param,{Actor,Type,Sql,Flags,BindingVals0}) ->
	Bp = backpressure(),
	BindingVals = prepare(BindingVals0),
	R = (catch actordb:exec_bp(Bp,Actor,Type,flags(Flags),Sql,BindingVals)),
	exec_res(Sql,R);
handle_function(exec_single_batch_param,{Actor,Type,Sql,Flags,BindingVals0}) ->
	Bp = backpressure(),
	BindingVals = prepare(BindingVals0),
	R = (catch actordb:exec_bp(Bp,Actor,Type,flags(Flags),Sql,BindingVals)),
	exec_res_batch(Sql,R);
handle_function(exec_multi,{Actors,Type,Sql,Flags}) ->
	Bp = backpressure(),
	exec_res(Sql,(catch actordb:exec_bp(Bp,Actors,Type,flags(Flags),Sql)));
% handle_function(exec_multi_prepare,{Actors,Type,Sql,Flags,BindingVals0}) ->
% 	Bp = backpressure(),
% 	BindingVals = prepare(BindingVals0),
% 	exec_res(Sql,(catch actordb:exec_bp(Bp,Actors,Type,flags(Flags),Sql,BindingVals)));
handle_function(exec_all,{Type,Sql,Flags}) ->
	Bp = backpressure(),
	exec_res(Sql,(catch actordb:exec_bp(Bp,$*,Type,flags(Flags),Sql)));
% handle_function(exec_all_prepare,{Type,Sql,Flags,BindingVals0}) ->
% 	Bp = backpressure(),
% 	BindingVals = prepare(BindingVals0),
% 	exec_res(Sql,(catch actordb:exec_bp(Bp,$*,Type,flags(Flags),Sql,BindingVals)));
handle_function(exec_sql,{Sql}) ->
	Bp = backpressure(),
	R = (catch actordb:exec_bp(Bp,Sql)),
	exec_res(Sql,R);
handle_function(exec_sql_param,{Sql, BindingVals0}) ->
	Bp = backpressure(),
	BindingVals = prepare(BindingVals0),
	exec_res(Sql,(catch actordb:exec_bp(Bp,Sql,BindingVals)));
handle_function(actor_types,_) ->
	backpressure(),
	case actordb:types() of
		schema_not_loaded ->
			{reply,[]};
		L ->
			{reply,[atom_to_binary(A,utf8) || A <- L]}
	end;
handle_function(actor_tables,{Type}) ->
	backpressure(),
	case catch actordb:tables(binary_to_existing_atom(Type,utf8)) of
		schema_not_loaded ->
			{reply,[]};
		L when is_list(L) ->
			{reply,L};
		_ ->
			{reply,[]}
	end;
handle_function(actor_columns,{Type,Table}) ->
	backpressure(),
	case catch actordb:columns(Type,Table) of
		schema_not_loaded ->
			{reply,#{}};
		L when is_list(L) ->
			{reply,maps:from_list(L)};
		_ ->
			{reply,#{}}
	end;
handle_function(uniqid,_) ->
	backpressure(),
	case catch actordb_idgen:getid() of
		{ok,N} when is_integer(N) ->
			{reply,N};
		_E ->
			{reply,0}
	end;
handle_function(protocolVersion,_) ->
	{reply,?ADBT_VERSION}.

flags([H|T]) ->
	actordb_sqlparse:check_flags(H,[])++flags(T);
flags([]) ->
	[].

val({blob,V}) ->
	#'Val'{blob = V};
val(undefined) ->
	#'Val'{isnull = true};
val(true) ->
	#'Val'{bval = true};
val(false) ->
	#'Val'{bval = false};
val(V) when is_float(V) ->
	#'Val'{real = V};
val(V) when is_binary(V); is_list(V) ->
	#'Val'{text = V};
% val(V) when V >= -32768, V =< 32767 ->
% 	#'Val'{smallint = V};
% val(V) when V >= -2147483648, V =< 2147483647 ->
% 	#'Val'{integer = V};
val(V) when is_integer(V) ->
	#'Val'{bigint = V}.

% Create reply for batch SQL queries
parse_query_res({reply, Result}) when Result#'Result'.rdRes /= undefined ->
	#'QueryResult'{rdRes = Result#'Result'.rdRes};
parse_query_res({reply, Result}) when Result#'Result'.wrRes /= undefined ->
	#'QueryResult'{wrRes = Result#'Result'.wrRes}.
% For each ResultSet, evaluate exec_res to parse the ResultSet and wrap it in a QueryResult
parse_query_res(_Sql, {_WhatNow, {ok, Rset}}) ->
	ExecResReply = exec_res(_Sql, {_WhatNow, {ok, Rset}}),
	parse_query_res(ExecResReply).
% In case the batch request had only one query, the response object is going to have only 1 ResultSet.
exec_res_batch(_Sql, {_WhatNow, {ok, [{columns, Cols}, {rows, Rows}]}}) ->
	QueryResults = [parse_query_res(_Sql, {_WhatNow, {ok, [{columns, Cols}, {rows, Rows}]}})],
	{reply, #'Result'{batchRes = QueryResults}};
% In case the batch request had only more than 1 query, the response object is going to have multiple ResultSets.
% Create a reply for each ResultSet
exec_res_batch(_Sql, {_WhatNow, {ok, ResultSets}}) ->
	QueryResults = [parse_query_res(_Sql, {_WhatNow, {ok, Rset}}) || Rset <- lists:reverse(ResultSets)],
	{reply, #'Result'{batchRes = QueryResults}};
% In case there is an error, fall back to exec_res to handle the error
exec_res_batch(_Sql, Unknown) ->
	exec_res(_Sql, Unknown).

% exec_res(_Sql,{_WhatNow,ok}) ->
% 	Cols = [],
% 	Rows = [#{}],
% 	{reply,#'Result'{rdRes = #'ReadResult'{hasMore = false,columns = Cols, rows = Rows}}};
exec_res(_Sql,{_WhatNow,{ok,[{columns,[]},{rows,[]}]}}) ->
	Cols = [],
	Rows = [#{}],
	{reply,#'Result'{rdRes = #'ReadResult'{hasMore = false,columns = Cols, rows = Rows}}};
exec_res(_Sql,{_WhatNow,{ok,[[_,_] = R|_]}}) ->
	exec_res(_Sql,{_WhatNow,{ok,R}});
exec_res(_Sql,{_WhatNow,{ok,[{columns,Cols1},{rows,Rows1}]}}) ->
	Cols = tuple_to_list(Cols1),
	Rows = [maps:from_list(lists:zip(Cols,[val(Val) || Val <- tuple_to_list(R)])) || R <- lists:reverse(Rows1)],
	{reply,#'Result'{rdRes = #'ReadResult'{hasMore = false,columns = Cols, rows = Rows}}};
exec_res(_Sql,{_WhatNow,{ok,{changes,LastId,NChanged}}}) ->
	{reply,#'Result'{wrRes = #'WriteResult'{lastChangeRowid = LastId, rowsChanged = NChanged}}};
exec_res(_Sql,{_WhatNow,{ok,[{changes,_,_} = H|_]}}) ->
	exec_res(_Sql,{_WhatNow,{ok,H}});
exec_res(_Sql,{'EXIT',_Exc}) ->
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_ERROR, info = ""});
exec_res(_Sql,{error,empty_actor_name}) ->
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_EMPTYACTORNAME, info = ""});
exec_res(_Sql,{unknown_actor_type,_Type} = E) ->
	case actordb:types() of
		schema_not_loaded ->
			exec_res(_Sql,{error,not_initialized});
		_ ->
			throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_INVALIDTYPE, info = actordb_err_desc:desc(E)})
	end;
exec_res(_Sql,{error,invalid_actor_name}) ->
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_INVALIDACTORNAME, info = ""});
exec_res(_Sql,{error,consensus_timeout} = E) ->
	I = actordb_err_desc:desc(E),
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_CONSENSUSTIMEOUT, info = I});
exec_res(_Sql,{error,consensus_impossible_atm} = E) ->
	I = actordb_err_desc:desc(E),
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_CONSENSUSIMPOSSIBLEATM, info = I});

exec_res(_Sql,{error,no_permission} = E) ->
	I = actordb_err_desc:desc(E),
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_NOTPERMITTED, info = I});
exec_res(_Sql,{error,invalid_login} = E) ->
	I = actordb_err_desc:desc(E),
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_LOGINFAILED, info = I});
exec_res(_Sql,{error,local_node_missing} = E) ->
	I = actordb_err_desc:desc(E),
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_LOCALNODEMISSING, info = I});
exec_res(_Sql,{error,missing_group_insert} = E) ->
	I = actordb_err_desc:desc(E),
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_MISSINGGROUPINSERT, info = I});
exec_res(_Sql,{error,missing_nodes_insert} = E) ->
	I = actordb_err_desc:desc(E),
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_MISSINGNODESINSERT, info = I});
exec_res(_Sql,{error,missing_root_user} = E) ->
	I = actordb_err_desc:desc(E),
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_MISSINGROOTUSER, info = I});
exec_res(_Sql,{error,not_initialized} = E) ->
	I = actordb_err_desc:desc(E),
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_NOTINITIALIZED, info = I});
exec_res(_Sql,{error,nocreate} = E) ->
	I = actordb_err_desc:desc(E),
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_NOCREATE, info = I});

exec_res(_Sql,{sql_error,"not_iolist"} = E) ->
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_SQLERROR, 
		info = actordb_err_desc:desc(E)});
exec_res(_Sql,{sql_error,E}) when is_list(E); is_binary(E) ->
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_SQLERROR, info = E});
exec_res(_Sql,{sql_error,E}) when is_tuple(E) ->
		exec_res(_Sql,{error,E});
exec_res(_Sql,{error,Err}) when is_tuple(Err) ->
	I = actordb_err_desc:desc(Err),
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_SQLERROR, info = I});
exec_res(_Sql,{error,Err}) ->
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_ERROR, info = actordb_err_desc:desc(Err)});
exec_res(_Sql,{ok,{sql_error,E}}) ->
	exec_res(_Sql,{sql_error,E});
exec_res(_Sql,{ok,{error,E}}) ->
	exec_res(_Sql,{error,E});
exec_res(_,Err) ->
	error_logger:format("Execute exception: ~p~n",[Err]),
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_ERROR, info = "Execute exception."}).

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
prepare(L) ->
	[prepare1(M) || M <- L].
prepare1([L|LT]) ->
	[prepare_line(L)|prepare1(LT)];
prepare1([]) ->
	[].
prepare_line(L) ->
	[prepval(V) || V <- L].
prepval(#'Val'{bigint = V}) when is_integer(V) ->
	V;
prepval(#'Val'{integer = V}) when is_integer(V) ->
	V;
prepval(#'Val'{smallint = V}) when is_integer(V) ->
	V;
prepval(#'Val'{real = V}) when is_float(V) ->
	V;
prepval(#'Val'{bval = V}) when V == true; V == false ->
	V;
prepval(#'Val'{text = V}) when is_binary(V); is_list(V) ->
	V;
prepval(#'Val'{isnull = true}) ->
	undefined;
prepval(#'Val'{blob = V})  when is_binary(V); is_list(V) ->
	{blob,V}.
