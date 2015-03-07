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
handle_function(login,[_U,_P]) ->
	State = actordb_backpressure:start_caller(),
	put(bp,State),
	ok;
handle_function(exec_single,[Actor,Type,Sql,Flags]) ->
	Bp = backpressure(),
	case catch actordb:exec_bp(Bp,Actor,Type,Flags,Sql) of
		X ->
			exec_res(Sql,X)
	end;
handle_function(exec_multi,[Actors,Type,Sql,Flags]) ->
	Bp = backpressure(),
	case catch actordb:exec_bp(Bp,Actors,Type,Flags,Sql) of
		X ->
			exec_res(Sql,X)
	end;
handle_function(exec_all,[Type,Sql,Flags]) ->
	Bp = backpressure(),
	case catch actordb:exec_bp(Bp,$*,Type,Flags,Sql) of
		X ->
			exec_res(Sql,X)
	end;
handle_function(exec_sql,[Sql]) ->
	Bp = backpressure(),
	case catch actordb:exec_bp(Bp,Sql) of
		X ->
			exec_res(Sql,X)
	end;
handle_function(protocol_version,[]) ->
	?ADBT_VERSION.


exec_res(_Sql,{ok,[{columns,Cols1},{rows,Rows1}]}) ->
	Cols = tuple_to_list(Cols1),
	Rows = [lists:zip(Cols,tuple_to_list(R)) || R <- Rows1],
	#'ReadResult'{success = true, columns = Cols, rows = Rows};
exec_res(_Sql,{ok,{changes,LastId,NChanged}}) ->
	#'WriteResult'{success = true, lastChangeRowid = LastId, rowsChanged = NChanged};
exec_res(Sql,{'EXIT',Exc}) ->
	lager:error("Query exception: ~p for sql: ~p",[Exc,Sql]),
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_ERROR, info = ""});
exec_res(_Sql,{error,empty_actor_name}) ->
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_EMPTYACTORNAME, info = ""});
exec_res(_Sql,{error,invalid_actor_name}) ->
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_INVALIDACTORNAME, info = ""});
exec_res(_Sql,{error,Err}) ->
	throw(#'InvalidRequestException'{code = ?ADBT_ERRORCODE_ERROR, info = atom_to_list(Err)}).

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

