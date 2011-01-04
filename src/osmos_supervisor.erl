-module (osmos_supervisor).

-export ([ start_link/0 ]).

-behaviour (supervisor).
-export ([ init/1 ]).

-define (MAX_RESTARTS, 3).
-define (MAX_RESTART_INTERVAL, 10).
-define (SERVER_STOP_TIMEOUT, 30000).

start_link () ->
  supervisor:start_link ({ local, ?MODULE }, ?MODULE, []).

init ([]) ->
  Strategy = { one_for_one, ?MAX_RESTARTS, ?MAX_RESTART_INTERVAL },
  ChildSpecs = [ { osmos_manager,
		   { osmos_manager, start_link, [] },
		   permanent,
		   ?SERVER_STOP_TIMEOUT,
		   worker,
		   [ osmos_manager ] },
		 { osmos_table_supervisor,
		   { osmos_table_supervisor, start_link, [] },
		   permanent,
		   infinity,
		   supervisor,
		   [ osmos_table_supervisor ] } ],
  { ok, { Strategy, ChildSpecs } }.
