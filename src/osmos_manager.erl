%% @doc
%% Server to manage multiple table owners each with multiple references,
%% and table name to pid mapping. Analogous to dets_server.
%% @end

-module (osmos_manager).

-export ([ start_link/0,
	   open/2,
	   close/1,
	   get_pid/1 ]).

-behaviour (gen_server).
-export ([ init/1,
           handle_call/3,
           handle_cast/2,
           handle_info/2,
           terminate/2,
           code_change/3]).

%
% public
%

%% @spec start_link () -> { ok, pid() } | { error, Reason }
%% @end

start_link () ->
  gen_server:start_link ({ local, ?MODULE }, ?MODULE, [], []).

%% @spec open (any(), [Option]) -> { ok, Table } | { error, Reason }
%% @end

open (Table, Options) ->
  gen_server:call (?MODULE, { Table, { open, Options } }, infinity).

%% @spec close (any()) -> ok | { error, Reason }
%% @end

close (Table) ->
  gen_server:call (?MODULE, { Table, close }, infinity).

%% @spec get_pid (any()) -> pid()
%% @end

get_pid (Table) ->
  gen_server:call (?MODULE, { Table, get_pid }).

%
% gen_server callbacks
%

%% @hidden
init ([]) ->
  process_flag (trap_exit, true),
  { ok, [] }.

%% @hidden
handle_call ({ Table, Request }, From, State) ->
  case request (Table, Request, From) of
    pending -> { noreply, State };
    Reply   -> { reply, Reply, State }
  end.

%% @hidden
handle_cast (_Request, State) ->
  { noreply, State }.

%% @hidden
handle_info ({ opened, Table, Opts, Result }, State) ->
  handle_opened (Table, Opts, Result),
  { noreply, State };

handle_info ({ closed, Table, Result }, State) ->
  handle_closed (Table, Result),
  { noreply, State };

handle_info ({ 'EXIT', Pid, Reason }, State) ->
  handle_process_exit (Pid, Reason),
  { noreply, State };

handle_info (Msg, State) ->
  error_logger:info_report ([ ?MODULE, handle_info, Msg ]),
  { noreply, State }.

%% @hidden
terminate (_Reason, _State) ->
  close_all_tables (),
  ok.

%% @hidden
code_change (_OldVsn, State, _Extra) ->
  { ok, State }.

%
% private
%

request (Table, Request, From) ->
  TableState = get_table_state (Table),
  { NewState, Result } = request (Table, TableState, Request, From),
  set_table_state (Table, NewState),
  Result.

%
% table state is one of:
%     undefined
%   | { open, Opts, Pid, NRefs, Links }
%   | { blocked, WorkerPid, From, queue<{Request, From}> }
%

request (Table, undefined, { open, Opts } = Request, From) ->
  Parent = self (),
  DoOpen = fun () ->
	     Result = try
			supervisor:start_child (osmos_table_supervisor, [Opts])
		      catch
			_:Error -> { error, Error }
		      end,
	     Parent ! { opened, Table, Opts, Result }
	   end,
  WorkerPid = spawn_worker (Table, DoOpen),
  Pending = queue:snoc (queue:new (), { Request, From }),
  { { blocked, WorkerPid, From, Pending }, pending };

request (_Table, undefined, _Request, _From) ->
  { undefined, { error, not_owner } };

request (Table,
	 { open, Opts, Pid, NRefs, Links },
	 { open, Opts },
	 { FromPid, _Tag }) ->
  link_client (FromPid),
  NewLinks = case gb_trees:lookup (FromPid, Links) of
	       { value, N } -> gb_trees:update (FromPid, N + 1, Links);
	       none         -> gb_trees:insert (FromPid, 1, Links)
	     end,
  { { open, Opts, Pid, NRefs + 1, NewLinks }, { ok, Table } };

request (_Table,
	 { open, Opts, _, _, _ } = State,
	 { open, OtherOpts },
	 _From)
    when OtherOpts =/= Opts ->
  { State, { error, options_mismatch } };

request (Table,
	 { open, Opts, Pid, NRefs, Links } = State,
	 close,
	 { FromPid, _Tag } = From) ->
  case gb_trees:lookup (FromPid, Links) of
    { value, N } when N > 1 ->
      unlink_client (FromPid),
      NewLinks = gb_trees:update (FromPid, N - 1, Links),
      { { open, Opts, Pid, NRefs - 1, NewLinks }, ok };
    { value, 1 } ->
      case NRefs of
	1 ->
	  WorkerPid = spawn_close (Table, Pid),
	  Pending = queue:snoc (queue:new (), { close, From }),
	  { { blocked, WorkerPid, From, Pending }, pending };
	M when M > 1 ->
	  unlink_client (FromPid),
	  NewLinks = gb_trees:delete (FromPid, Links),
	  { { open, Opts, Pid, NRefs - 1, NewLinks }, ok }
      end;
    none ->
      { State, { error, not_owner } }
  end;

request (_Table, { open, _, Pid, _, _ } = State, get_pid, _From) ->
  { State, { ok, Pid } };

request (_Table, { blocked, WorkerPid, BlockedFrom, Pending }, Request, From) ->
  NewPending = queue:snoc (Pending, { Request, From }),
  { { blocked, WorkerPid, BlockedFrom, NewPending }, pending }.

handle_opened (Table, Opts, Result) ->
  { blocked, _WorkerPid, From, Pending } = get_table_state (Table),
  case Result of
    { ok, TablePid } ->
      link_table (Table, TablePid),
      State = { open, Opts, TablePid, 0, gb_trees:empty () },
      do_pending (Table, State, Pending);
    Error ->
      case queue:is_empty (Pending) of
	true ->
	  set_table_state (Table, undefined);
	false ->
	  case queue:head (Pending) of
	    { { open, Opts }, From } ->
	      gen_server:reply (From, { error, Error }),
	      do_pending (Table, undefined, queue:tail (Pending));
	    _ ->
	      do_pending (Table, undefined, Pending)
	  end
      end
  end.

handle_closed (Table, Result) ->
  { blocked, _WorkerPid, From, Pending } = get_table_state (Table),
  case queue:is_empty (Pending) of
    true ->
      set_table_state (Table, undefined);
    false ->
      case queue:head (Pending) of
	{ close, { FromPid, _Tag } = From } ->
	  unlink_client (FromPid),
	  Reply = case Result of
		    ok    -> ok;
		    Error -> { error, Error }
		  end,
	  gen_server:reply (From, Reply),
	  do_pending (Table, undefined, queue:tail (Pending));
	_ ->
	  do_pending (Table, undefined, Pending)
      end
  end.

do_pending (Table, State, Pending) ->
  case queue:is_empty (Pending) of
    true ->
      set_table_state (Table, State);
    false ->
      { Request, From } = queue:head (Pending),
      { NewState, Reply } = request (Table, State, Request, From),
      case Reply of
	pending ->
	  set_table_state (Table, NewState);
	_ ->
	  gen_server:reply (From, Reply),
	  do_pending (Table, NewState, queue:tail (Pending))
      end
  end.

handle_process_exit (Pid, Reason) ->
  Type = get_link_type (Pid),
  erase_link_type (Pid),
  case Type of
    { table, Table }  -> handle_table_exit (Table, Pid, Reason);
    { worker, Table } -> handle_worker_exit (Table, Pid, Reason);
    { client, _N }    -> handle_client_exit (Pid)
  end.

handle_table_exit (Table, TablePid, _Reason) ->
  % We are linked to the table process only while its state is open, but the
  % exit message may have been sent just before we decided to close the table.
  % In that case, however, we can ignore the message and move on.
  case get_table_state (Table) of
    { open, _Opts, TablePid, _NRefs, Links } ->
% XXX: wouldn't it be nice if the client's next operation returned an error?
      lists:foreach (fun ({ ClientPid, N }) ->
		       case get_link_type (ClientPid) of
			 { client, N } ->
			   unlink_process (ClientPid);
			 { client, M } when M > N ->
			   set_link_type (ClientPid, { client, M - N })
		       end
		     end,
		     gb_trees:to_list (Links)),
      set_table_state (Table, undefined);
    _ ->
      ok
  end.

handle_worker_exit (_Table, _WorkerPid, normal) ->
  % XXX: someone could hang us by doing exit (WorkerPid, normal) before
  % the worker sends us any message....oh well?
  ok;
handle_worker_exit (Table, WorkerPid, Reason) when Reason =/= normal ->
  case get_table_state (Table) of
    { blocked, WorkerPid, From, Pending } ->
      case queue:is_empty (Pending) of
	true ->
	  set_table_state (Table, undefined);
	false ->
	  case queue:head (Pending) of
	    { _Request, { FromPid, _Tag } = From } ->
	      unlink_client (FromPid),
	      gen_server:reply (From, { error, Reason }),
	      do_pending (Table, undefined, queue:tail (Pending));
	    _ ->
	      do_pending (Table, undefined, Pending)
	  end
      end;
    _ ->
      ok
  end.

handle_client_exit (Pid) ->
  List = [ { Table, State } || { { table, Table }, State } <- get () ],
  remove_client (Pid, List).

remove_client (_ClientPid, []) ->
  ok;

remove_client (ClientPid,
	       [ { Table, { open, Opts, TablePid, NRefs, Links } } | List ]) ->
  % remove any references held by this client
  case gb_trees:lookup (ClientPid, Links) of
    none ->
      ok;
    { value, N } ->
      case NRefs > N of
	true ->
	  NewLinks = gb_trees:delete (ClientPid, Links),
	  NewState = { open, Opts, TablePid, NRefs - N, NewLinks },
	  set_table_state (Table, NewState);
	false ->
	  WorkerPid = spawn_close (Table, TablePid),
	  set_table_state (Table, { blocked, WorkerPid, nobody, queue:new () })
      end
  end,
  remove_client (ClientPid, List);

remove_client (ClientPid,
	       [ { Table, { blocked, WorkerPid, From, Pending } } | List ]) ->
  % remove any pending requests from this client
  NewPending =
    queue:from_list ([ E || { _, { Pid, _ } } = E <- queue:to_list (Pending),
			    Pid =/= ClientPid ]),
  set_table_state (Table, { blocked, WorkerPid, From, NewPending }),
  remove_client (ClientPid, List).

spawn_close (Table, Pid) ->
  unlink_process (Pid),
  Parent = self (),
  DoClose = fun () ->
	      Result = try
			 gen_server:call (Pid, close)
		       catch
			 _:Error -> { error, Error }
		       end,
	      Parent ! { closed, Table, Result }
	    end,
  spawn_worker (Table, DoClose).

get_table_state (Table) ->
  get ({ table, Table }).

set_table_state (Table, undefined) ->
  erase ({ table, Table });
set_table_state (Table, State) when State =/= undefined ->
  put ({ table, Table }, State).

close_all_tables () ->
  lists:foreach (fun (Pid) ->
		   gen_server:call (Pid, close)
		 end,
		 [ Pid || { { table, _ }, { open, _, Pid, _, _ } } <- get () ]).

spawn_worker (Table, F) ->
  Pid = spawn_link (F),
  set_link_type (Pid, { worker, Table }),
  Pid.

link_table (Table, Pid) ->
  link (Pid),
  set_link_type (Pid, { table, Table }).

link_client (Pid) ->
  case get_link_type (Pid) of
    { client, N } ->
      set_link_type (Pid, { client, N + 1 });
    undefined ->
      link (Pid),
      set_link_type (Pid, { client, 1 })
  end.

unlink_client (Pid) ->
  case get_link_type (Pid) of
    { client, N } when N > 1 ->
      set_link_type (Pid, { client, N - 1 });
    { client, 1 } ->
      unlink_process (Pid)
  end.

unlink_process (Pid) ->
  unlink (Pid),
  erase_link_type (Pid).

get_link_type (Pid) ->
  get ({ link_type, Pid }).

set_link_type (Pid, Type) ->
  put ({ link_type, Pid }, Type).

erase_link_type (Pid) ->
  erase ({ link_type, Pid }).
