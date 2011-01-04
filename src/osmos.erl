-module (osmos).

-export ([ start/0,
	   stop/0,
	   open/2,
	   close/1,
	   read/2,
	   write/3,
	   select_range/5,
	   select_continue/3,
	   info/1,
	   info/2 ]).

-behaviour (application).
-export ([ start/2,
	   stop/1 ]).

%
% public
%

%% @spec start () -> ok
%% @doc Start the osmos application.
%% @end

start () ->
  application:start (osmos).

%% @spec stop () -> ok
%% @doc Stop the osmos application.
%% @end

stop () ->
  application:stop (osmos).

%% @spec open (Table::any(), [Option]) -> { ok, Table } | { error, Reason }
%%       Option = { directory, string() }
%%              | { format, #osmos_table_format{} }
%%              | { max_memory_entries, integer() }
%%              | { max_merge_jobs, integer() }
%%              | { max_selects, integer() }
%%              | { select_timeout_secs, number() }
%% @doc Open or reopen a osmos table. Note that more than one process may
%% open a given table; it is not actually closed until every process has
%% closed it. If another process has already opened the same table with
%% incompatible options, the call fails with {error, options_mismatch}.
%%
%% If a table has been opened previously in the given directory, it is
%% reopened. Otherwise, a new, empty table is created.
%% @end

open (Table, Options) when is_list (Options) ->
  case osmos_table_format:valid (proplists:get_value (format, Options)) of
    true  -> osmos_manager:open (Table, Options);
    false -> erlang:error ({ badarg, Options })
  end.

%% @spec close (any()) -> ok | { error, Reason }
%% @doc Close the given Table, which must have been opened by the calling
%% process. The table is not actually closed until all processes that
%% opened it have closed it.
%% @end

close (Table) ->
  osmos_manager:close (Table).

%% @spec read (any(), any()) -> { ok, Value::any() } | not_found
%% @doc Read the value for Key in the given Table. Return { ok, Value } if
%% found, or not_found if not.
%% @end

read (Table, Key) ->
  { ok, Pid } = osmos_manager:get_pid (Table),
  osmos_table:read (Pid, Key).

%% @spec write (any(), any(), any()) -> ok | { error, Reason }
%% @doc Write a new Value for Key in the given Table. The exact semantics
%% of writing to the table depend on the table's format; among other
%% possibilities, writing may add a new value, update an existing value,
%% or delete a value, depending on the format's merge, short-circuit,
%% and delete methods.
%% @end

write (Table, Key, Value) ->
  { ok, Pid } = osmos_manager:get_pid (Table),
  osmos_table:write (Pid, Key, Value).

%% @spec select_range (any(), Less, Less, Select, integer())
%%         -> { ok, [ { Key::any(), Value::any() } ], continuation() }
%%          | { error, Reason }
%%       Less = (Key::any()) -> bool()
%%       Select = (Key::any(), Value::any()) -> bool()
%% @doc Begin iterating over a range of keys and values in the table
%% specified by lower and upper bounds, and a predicate that can select
%% within this range.
%%
%% The lower bound LessLo (Key) should return true iff Key is less than
%% every element in the range. The upper bound LessHi (Key) should return
%% true iff Key is not greater than every element in the range.
%%
%% Both bounds must be consistent with the table format's ordering, so
%% the following implications hold:
%% <ol>
%%   <li>(Less (K2) and KeyLess (K1, K2)) implies Less (K1)</li>
%%   <li>(not Less (K1) and KeyLess (K1, K2)) implies not Less (K2)</li>
%% </ol>
%% where KeyLess is the table format's key_less function.
%%
%% The selection predicate Select (Key, Value) should return true to
%% include {Key, Value} in the results. It will only be called for keys
%% in the interval defined by LessLo and LessHi.
%%
%% At most N key-value pairs are returned if successful, along with a
%% continuation that can be used to retrieve additional results with
%% select_continue. If fewer than N results are returned from select_range
%% or select_continue, the end of the table has been reached; further calls
%% to select_continue will return an empty list of results.
%%
%% The results from subsequent calls to select_continue always reflect
%% the state of the table at the time of the original call to select_range.
%% That is, any intervening writes to the table will not affect the
%% results from the select.
%%
%% Note that, depending on the select_timeout_secs and max_selects options
%% given when opening the table, the continuation may go stale (if more
%% results are not requested within select_timeout_secs, or if max_selects
%% other selects are started in the meantime).
%% @end

select_range (Table, LessLo, LessHi, Select, N) when is_function (LessLo),
						     is_function (LessHi),
						     is_function (Select),
						     is_integer (N) ->
  { ok, Pid } = osmos_manager:get_pid (Table),
  osmos_table:select_range (Pid, LessLo, LessHi, Select, N).

%% @spec select_continue (any(), continuation(), integer())
%%         -> { ok, [ { Key::any(), Value::any() } ], continuation() }
%%          | { error, Reason }
%% @doc Continue selecting results using a continuation returned by
%% select_range or select_continue.
%% @end

select_continue (Table, Cont, N) when is_integer (N) ->
  { ok, Pid } = osmos_manager:get_pid (Table),
  osmos_table:select_continue (Pid, Cont, N).

%% @spec info (any(), What) -> Value | undefined
%%       What = file_entries | files | tree_entries
%% @doc Get table information.
%% @end

info (Table, What) ->
  { ok, Pid } = osmos_manager:get_pid (Table),
  osmos_table:info (Pid, What).

%% @spec info (any()) -> [ { What, Value } ]
%%       What = file_entries | files | tree_entries
%% @doc Get table information.
%% @end

info (Table) ->
  { ok, Pid } = osmos_manager:get_pid (Table),
  osmos_table:info (Pid).

%
% application callbacks
%

%% @hidden
start (_Type, _Args) ->
  osmos_supervisor:start_link ().

%% @hidden
stop (_State) ->
  ok.
