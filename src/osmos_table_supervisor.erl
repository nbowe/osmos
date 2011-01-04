-module (osmos_table_supervisor).

-export ([ start_link/0 ]).

-behaviour (supervisor).
-export ([ init/1 ]).

-define (STOP_TIMEOUT, 30000).

start_link () ->
  supervisor:start_link ({ local, ?MODULE }, ?MODULE, []).

init ([]) ->
  % temporary children are never restarted
  Strategy = { simple_one_for_one, 0, 1 },
  ChildSpecs = [ { osmos_table,
		   { osmos_table, start_link, [] },
		   temporary,
		   ?STOP_TIMEOUT,
		   worker,
		   [ osmos_table ] } ],
  { ok, { Strategy, ChildSpecs } }.
