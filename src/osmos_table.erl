-module (osmos_table).

-export ([ start_link/1,
	   close/1,
	   read/2,
	   write/3,
	   select_range/5,
	   select_continue/3,
	   info/1,
	   info/2 ]).

-behaviour (gen_server).
-export ([ init/1,
	   handle_call/3,
	   handle_cast/2,
	   handle_info/2,
	   terminate/2,
	   code_change/3 ]).

% protected
-export ([ merge_process/6 ]).

-ifdef (HAVE_EUNIT).
-include_lib ("eunit/include/eunit.hrl").
-endif.

-include ("osmos.hrl").
-include ("osmos_vli.hrl").

-record (table, { directory,
		  format,
		  max_memory_entries,
		  max_merge_jobs,
		  max_selects,
		  select_timeout_secs,
		  tree,
		  journal_path,
		  journal_io,
		  n_file_entries,	% sum of file entry counts
		  n_files,
		  files,
		  merge_jobs,		% [ { pid(), PathA, PathB, PathOut } ]
		  path_refs,		% file reference counts
		  path_files,
		  prev_select_handle,
		  selects,		% id -> select_cont
		  select_times,		% id -> time last used
		  iters
		}).

-define (JOURNAL_MAGIC_BEGIN,       <<240, 204,  15, 255, 240, 204, 244, 206>>).
-define (JOURNAL_MAGIC_ENTRY_BEGIN, << 16,  28, 167,  88,  16,  28, 167,  82>>).
-define (JOURNAL_MAGIC_ENTRY_END,   <<206,  11, 172, 202,  48,  12,  17,  51>>).
-define (JOURNAL_MAGIC_BYTES,       8).
-define (JOURNAL_ENTRY_SIZE,        32/big-unsigned-integer).
-define (JOURNAL_ENTRY_SIZE_BYTES,  4).

-define (STATE_MAGIC_BEGIN, <<33, 85, 78, 88, 88, 83,  6, 33>>).
-define (STATE_MAGIC_END,   <<33,  1, 77, 75, 85,  1, 90, 33>>).
-define (STATE_MAGIC_BYTES, 8).

-define (MAX_MERGE_RATIO, 1.2).

-define (DEFAULT_OPTIONS, [ { max_memory_entries, 16384 },
			    { max_merge_jobs, 4 },
			    { max_selects, 16 },
			    { select_timeout_secs, 3600 } ]).

-define (is_positive_integer (N), (is_integer (N) andalso N > 0)).
-define (is_positive_or_infinity (N),
	 ((is_number (N) andalso N > 0) orelse N =:= infinity)).

%
% public
%

start_link (Options) ->
  gen_server:start_link (?MODULE, Options, []).

close (Pid) ->
  gen_server:call (Pid, close).

read (Pid, Key) ->
  gen_server:call (Pid, { read, Key }).

write (Pid, Key, Value) ->
  gen_server:call (Pid, { write, Key, Value }).

select_range (Pid, LessLo, LessHi, Select, N) ->
  gen_server:call (Pid, { select_range, LessLo, LessHi, Select, N }).

select_continue (Pid, Handle, N) ->
  gen_server:call (Pid, { select_continue, Handle, N }).

info (Pid) ->
  gen_server:call (Pid, table_info).

info (Pid, What) ->
  gen_server:call (Pid, { table_info, What }).

%
% gen_server callbacks
%

init (Options) ->
  process_flag (trap_exit, true),
  [ Dir, Format, MaxMemoryEntries, MaxMergeJobs,
    MaxSelects, SelectTimeoutSecs ]
    = lists:map (fun (K) ->
		   case lists:keysearch (K, 1, Options) of
		     { value, { K, V } } ->
		       V;
		     false ->
		       case lists:keysearch (K, 1, ?DEFAULT_OPTIONS) of
			 { value, { K, V } } ->
			   V;
			 false ->
			   exit ({ badarg, Options })
		       end
		   end
		 end,
		 [ directory,
		   format,
		   max_memory_entries,
		   max_merge_jobs,
		   max_selects,
		   select_timeout_secs ]),
  Table = open (Dir, Format, MaxMemoryEntries, MaxMergeJobs,
		MaxSelects, SelectTimeoutSecs),
  { ok, Table }.

handle_call ({ write, Key, Value }, _From, Table) ->
  NewTable = handle_write (Table, Key, Value),
  { reply, ok, NewTable };
handle_call ({ read, Key }, _From, Table) ->
  Result = handle_read (Table, Key),
  { reply, Result, Table };
handle_call ({ select_range, LessLo, LessHi, Select, N }, _From, Table) ->
  { NewTable, Result } = handle_select_range (Table, LessLo, LessHi, Select, N),
  { reply, Result, NewTable };
handle_call ({ select_continue, Handle, N }, _From, Table) ->
  { NewTable, Result } = handle_select_continue (Table, Handle, N),
  { reply, Result, NewTable };
handle_call (close, _From, Table) ->
  { stop, normal, ok, Table };
handle_call (table_info, _From, Table) ->
  Result = table_info (Table),
  { reply, Result, Table };
handle_call ({ table_info, What }, _From, Table) ->
  Result = table_info (Table, What),
  { reply, Result, Table };
handle_call (_Request, _From, Table) ->
  { noreply, Table }.

handle_cast (_Request, Table) ->
  { noreply, Table }.

handle_info ({ 'EXIT', Pid, normal }, Table) ->
  NewTable = merge_done (Table, Pid),
  { noreply, NewTable };
handle_info ({ 'EXIT', Pid, Reason }, Table = #table { merge_jobs = Jobs })
	when Reason =/= normal ->
  case lists:keysearch (Pid, 1, Jobs) of
    { value, Job } ->
      erlang:error ({ merge_error, Pid, Job, Reason, Table });
    _ ->
      { noreply, Table }
  end;
handle_info (_Msg, Table) ->
  { noreply, Table }.

terminate (Reason, Table) when Reason =:= normal ; Reason =:= shutdown ->
  handle_terminate (Table),
  ok;
terminate (_Reason, _Table) ->
  % just crash if there was an error
  ok.

code_change (_OldVsn, Table, _Extra) ->
  { ok, Table }.

%
% private
%

open (Dir, Format, MaxMemoryEntries, MaxMergeJobs,
      MaxSelects, SelectTimeoutSecs)
    when is_record (Format, osmos_table_format),
	 Format#osmos_table_format.block_size >= ?OSMOS_MIN_BLOCK_SIZE,
	 ?is_positive_integer (MaxMemoryEntries),
	 ?is_positive_integer (MaxMergeJobs),
	 ?is_positive_integer (MaxSelects),
	 ?is_positive_or_infinity (SelectTimeoutSecs) ->
  Less = Format#osmos_table_format.key_less,
  case file:make_dir (Dir) of
    ok ->
      JournalPath = new_journal_path (),
      JournalIo = journal_open (Dir, JournalPath),
      Table = #table { directory = Dir,
		       format = Format,
		       max_memory_entries = MaxMemoryEntries,
		       max_merge_jobs = MaxMergeJobs,
		       max_selects = MaxSelects,
		       select_timeout_secs = SelectTimeoutSecs,
		       tree = osmos_tree:new (Less),
		       journal_path = JournalPath,
		       journal_io = JournalIo,
		       n_file_entries = 0,
		       n_files = 0,
		       files = [],
		       merge_jobs = [],
		       path_refs = gb_trees:empty (),
		       path_files = gb_trees:empty (),
		       prev_select_handle = 0,
		       selects = gb_trees:empty (),
		       select_times = gb_trees:empty (),
		       iters = gb_trees:empty () },
      table_state_write (Table),
      Table;

    { error, eexist } ->
      BlockSize = Format#osmos_table_format.block_size,
      { BlockSize, JournalPath, NFiles, Paths } = table_state_read (Dir),
      Files = [ { Path, osmos_file:open (Format, dir_path (Dir, Path)) }
		|| Path <- Paths ],
      PathRefs = lists:foldl (fun (P, T) ->
				gb_trees:enter (P, 1, T)
			      end,
			      gb_trees:empty (),
			      Paths),
      PathFiles = lists:foldl (fun ({ P, F }, T) ->
				 gb_trees:enter (P, F, T)
			       end,
			       gb_trees:empty (),
			       Files),
      NEntries = lists:foldl (fun ({ _, F }, N) ->
				N + osmos_file:total_entries (F)
			      end,
			      0,
			      Files),
      Tree = journal_read (#table { format = Format },
			   dir_path (Dir, JournalPath)),
      { ok, JournalIo } = file:open (dir_path (Dir, JournalPath),
				     [ append, raw, binary ]),
      #table { directory = Dir,
	       format = Format,
	       max_memory_entries = MaxMemoryEntries,
	       max_merge_jobs = MaxMergeJobs,
	       max_selects = MaxSelects,
	       select_timeout_secs = SelectTimeoutSecs,
	       tree = Tree,
	       journal_path = JournalPath,
	       journal_io = JournalIo,
	       n_file_entries = NEntries,
	       n_files = NFiles,
	       files = Files,
	       merge_jobs = [],
	       path_refs = PathRefs,
	       path_files = PathFiles,
	       prev_select_handle = 0,
	       selects = gb_trees:empty (),
	       select_times = gb_trees:empty (),
	       iters = gb_trees:empty () };

    { error, Err } ->
      erlang:error ({ make_dir_error, Dir, Err })
  end.

handle_terminate (Table) ->
  ok = journal_close (Table),
  lists:foreach (fun ({ _Path, File }) ->
		   ok = osmos_file:close (File)
		 end,
		 gb_trees:to_list (Table#table.path_files)),
  ok.

%
% file handle management
%

fix_path_refs (Table = #table { directory = Dir,
				files = Files,
				path_files = OldPathFiles,
				selects = Selects,
				select_times = SelectTimes,
				iters = Iters }) ->
  { NewSelects, NewSelectTimes } = expire_selects (Table, Selects, SelectTimes),
  NewPathRefs0 = lists:foldl (fun ({ Path, _File }, T) ->
				gb_trees:insert (Path, 1, T)
			      end,
			      gb_trees:empty (),
			      Files),
  NewPathRefs1 = lists:foldl (fun ({ _, C }, T) ->
				iter_path_refs (select_cont_iter (C), T)
			      end,
			      NewPathRefs0,
			      gb_trees:to_list (NewSelects)),
  NewPathRefs = lists:foldl (fun ({ _, It }, T) ->
			       iter_path_refs (It, T)
			     end,
			     NewPathRefs1,
			     gb_trees:to_list (Iters)),
  NewPathFiles =
    lists:foldl (fun ({ Path, F }, T) ->
		   case gb_trees:lookup (Path, NewPathRefs) of
		     { value, _ } ->
		       gb_trees:insert (Path, F, T);
		     none ->
		       ok = osmos_file:close (F),
		       delete_file (Dir, Path),
		       T
		   end
		 end,
		 gb_trees:empty (),
		 gb_trees:to_list (OldPathFiles)),
  Table#table { path_refs = NewPathRefs,
		path_files = NewPathFiles,
		selects = NewSelects,
		select_times = NewSelectTimes }.

expire_selects (#table { max_selects = Max, select_timeout_secs = Timeout },
		Selects, SelectTimes) when ?is_positive_or_infinity (Timeout) ->
  [ Selects1, SelectTimes1 ] =
    expire_selects (now (),
		    Timeout,
		    [ Selects, SelectTimes ],
		    gb_trees:to_list (SelectTimes)),
  N = gb_trees:size (Selects1),
  case N > Max of
    true ->
      [ Selects2, SelectTimes2 ] =
	delete_selects (N - Max,
			[ Selects1, SelectTimes1 ],
			lists:keysort (2, gb_trees:to_list (SelectTimes1))),
      { Selects2, SelectTimes2 };
    false ->
      { Selects1, SelectTimes1 }
  end.

expire_selects (_Now, infinity, Trees, _List) ->
  Trees;
expire_selects (_Now, _Timeout, Trees, []) ->
  Trees;
expire_selects (Now, Timeout, Trees, [ { Id, Time } | Rest ])
	when is_number (Timeout) ->
  NewTrees = case timer:now_diff (Now, Time) > 1000000 * Timeout of
	       true  -> [ gb_trees:delete (Id, T) || T <- Trees ];
	       false -> Trees
	     end,
  expire_selects (Now, Timeout, NewTrees, Rest).

delete_selects (0, Trees, _List) ->
  Trees;
delete_selects (N, Trees, [ { Id, _Time } | Rest ]) ->
  delete_selects (N - 1, [ gb_trees:delete (Id, T) || T <- Trees ], Rest).

%
% single key read
%

handle_read (Table, Key) ->
  case osmos_tree:search (Table#table.tree, Key) of
    { ok, Value } ->
      case short_circuit (Table, Key, Value) of
	true ->
	  case delete (Table, Key, Value) of
	    true  -> not_found;
	    false -> { ok, Value }
	  end;
	false ->
	  handle_read (Table, Key, Table#table.files, true, Value)
	end;
    not_found ->
      handle_read (Table, Key, Table#table.files, false, [])
  end.

handle_read (Table, Key, [ { _Path, File } | Files ], HaveNewer, Newer) ->
  case osmos_file:search (File, Key) of
    not_found ->
      handle_read (Table, Key, Files, HaveNewer, Newer);
    { ok, Value } ->
      NewValue = case HaveNewer of
		   true  -> merge (Table, Key, Value, Newer);
		   false -> Value
		 end,
      case short_circuit (Table, Key, NewValue) of
	true ->
	  case delete (Table, Key, NewValue) of
	    true  -> not_found;
	    false -> { ok, NewValue }
	  end;
	false ->
	  handle_read (Table, Key, Files, true, NewValue)
      end
  end;
handle_read (Table, Key, [], true, Value) ->
  case delete (Table, Key, Value) of
    true  -> not_found;
    false -> { ok, Value }
  end;
handle_read (_Table, _Key, [], false, _) ->
  not_found.

%
% select on a range
%

-record (select_cont, { it, it_end, select, format }).

select_cont_iter (#select_cont { it = It }) ->
  % Note: no need to worry about it_end's file handle going stale,
  % since it is used only for comparison.
  It.

% LessLo (Key) -> true if Key less than any element in range | false
% LessHi (Key) -> true if Key less than upper bound of range | false
handle_select_range (Table, LessLo, LessHi, Select, N) ->
  C0 = #select_cont { it = iter_lower_bound (Table, LessLo),
		      it_end = iter_lower_bound (Table, LessHi),
		      select = Select,
		      format = Table#table.format },
  { Entries, C } = select_loop (C0, N, []),
  case C of
    ?OSMOS_END ->
      { Table, { ok, Entries, ?OSMOS_END } };
    _ ->
      NewSelectHandle = Table#table.prev_select_handle + 1,
      NewSelects = gb_trees:insert (NewSelectHandle, C, Table#table.selects),
      NewSelectTimes = gb_trees:insert (NewSelectHandle,
					now (),
					Table#table.select_times),
      NewTable = Table#table { prev_select_handle = NewSelectHandle,
			       selects = NewSelects,
			       select_times = NewSelectTimes },
      { fix_path_refs (NewTable), { ok, Entries, NewSelectHandle } }
  end.

handle_select_continue (Table, ?OSMOS_END, _N) ->
  { Table, { ok, [], ?OSMOS_END } };
handle_select_continue (Table, Handle, N) ->
  case gb_trees:lookup (Handle, Table#table.selects) of
    none ->
      { Table, { error, { bad_select_handle, Handle } } };
    { value, C0 } ->
      { Entries, C } = select_loop (C0, N, []),
      case C of
	?OSMOS_END ->
	  NewSelects = gb_trees:delete (Handle, Table#table.selects),
	  NewSelectTimes = gb_trees:delete (Handle, Table#table.select_times),
	  NewTable0 = Table#table { selects = NewSelects,
				    select_times = NewSelectTimes },
	  NewTable = fix_path_refs (NewTable0),
	  { NewTable, { ok, Entries, ?OSMOS_END } };
	_ ->
	  NewSelects = gb_trees:update (Handle, C, Table#table.selects),
	  NewSelectTimes = gb_trees:update (Handle,
					    now (),
					    Table#table.select_times),
	  NewTable0 = Table#table { selects = NewSelects,
				    select_times = NewSelectTimes },
	  NewTable = fix_path_refs (NewTable0),
	  { NewTable, { ok, Entries, Handle } }
      end
  end.

select_loop (C, 0, Acc) ->
  { lists:reverse (Acc), C };
select_loop (C = #select_cont { it = It, it_end = End, select = Select },
	     N, Acc) ->
  case iter_less (It, End) of
    false ->
      { lists:reverse (Acc), ?OSMOS_END };
    true ->
      K = iter_key (It),
      V = iter_value (It),
      NewC = C#select_cont { it = iter_next (It) },
      case Select (K, V) of
	true  -> select_loop (NewC, N - 1, [ { K, V } | Acc ]);
	false -> select_loop (NewC, N,     Acc)
      end
  end.

%
% iterators
%

-record (iter, { format,
		 key,
		 value,
		 tree_it,
		 file_its }).

iter_key (#iter { key = K }) ->
  K.
iter_value (#iter { value = V }) ->
  V.

iter_less (?OSMOS_END, ?OSMOS_END) ->
  false;
iter_less (?OSMOS_END, B) when is_record (B, iter) ->
  false;
iter_less (A, ?OSMOS_END) when is_record (A, iter) ->
  true;
iter_less (#iter { format = Fmt, key = A },
	   #iter { format = Fmt, key = B }) ->
  key_less (Fmt, A, B).

iter_lower_bound (#table { format = Fmt, tree = Tree, files = Files }, Less)
	when is_function (Less) ->
  case osmos_tree:iter_lower_bound (Tree, Less) of
    ?OSMOS_END ->
      It = #iter { format = Fmt, tree_it = ?OSMOS_END },
      iter_lower_bound_no_least (It, Less, Files, []);
    TreeIt ->
      It = #iter { format = Fmt,
		   key = osmos_tree:iter_key (TreeIt),
		   value = osmos_tree:iter_value (TreeIt),
		   tree_it = TreeIt },
      iter_lower_bound_least (It, Less, Files, [])
  end.

% none of the iterators has returned a key yet
iter_lower_bound_no_least (_It, _Less, [], _Acc) ->
  ?OSMOS_END;
iter_lower_bound_no_least (It, Less, [ { _Path, File } | Files ], Acc) ->
  case osmos_file:iter_lower_bound (File, Less) of
    ?OSMOS_END ->
      iter_lower_bound_no_least (It, Less, Files, [ ?OSMOS_END | Acc ]);
    FileIt ->
      NewIt = It#iter { key = osmos_file:iter_key (FileIt),
			value = osmos_file:iter_value (FileIt) },
      iter_lower_bound_least (NewIt, Less, Files, [ FileIt | Acc ])
  end.

iter_lower_bound_least (It, _Less, [], Acc) ->
  It#iter { file_its = lists:reverse (Acc) };
iter_lower_bound_least (It, Less, [ { _Path, File } | Files ], Acc) ->
  case osmos_file:iter_lower_bound (File, Less) of
    ?OSMOS_END ->
      iter_lower_bound_least (It, Less, Files, [ ?OSMOS_END | Acc ]);
    FileIt ->
      FileKey = osmos_file:iter_key (FileIt),
      case key_less (It#iter.format, FileKey, It#iter.key) of
	true ->
	  NewIt = It#iter { key = FileKey,
			    value = osmos_file:iter_value (FileIt) },
	  iter_lower_bound_least (NewIt, Less, Files, [ FileIt | Acc ]);
	false ->
	  iter_lower_bound_least (It, Less, Files, [ FileIt | Acc ])
      end
  end.

% precondition: none of the tree or file iterators' keys K are less than
% the table iterator Key, i.e., not (Key < K) implies (K == Key)

iter_next (It = #iter { tree_it = ?OSMOS_END, file_its = FileIts }) ->
  iter_next_no_least (It, FileIts, []);

iter_next (It = #iter { format = Fmt,
			key = OldKey,
			tree_it = TreeIt,
			file_its = FileIts }) ->
  TreeKey = osmos_tree:iter_key (TreeIt),
  case key_less (Fmt, OldKey, TreeKey) of
    true ->
      NewIt = It#iter { key = TreeKey, value = osmos_tree:iter_value (TreeIt) },
      iter_next_maybe_sc (NewIt, OldKey, FileIts, []);
    false ->
      % TreeKey == OldKey; must advance tree
      case osmos_tree:iter_next (TreeIt) of
	?OSMOS_END ->
	  iter_next_no_least (It#iter { tree_it = ?OSMOS_END }, FileIts, []);
	NewTreeIt ->
	  NewIt = It#iter { key = osmos_tree:iter_key (NewTreeIt),
			    value = osmos_tree:iter_value (NewTreeIt),
			    tree_it = NewTreeIt },
	  iter_next_maybe_sc (NewIt, OldKey, FileIts, [])
      end
  end.

iter_next_maybe_sc (It = #iter { format = Fmt, key = Key, value = Value },
		    OldKey, FileIts, Acc) ->
  case short_circuit (Fmt, Key, Value) of
    true  -> iter_next_least_sc (It, OldKey, FileIts, Acc);
    false -> iter_next_least (It, OldKey, FileIts, Acc)
  end.

% helper functions: advance any file iterators with the least key;
% find the new least key, merging as necessary to find new value

% advance iterators while all at end (no new least key)
iter_next_no_least (_It, [], _Acc) ->
  ?OSMOS_END;
iter_next_no_least (It, [ ?OSMOS_END | FileIts ], Acc) ->
  iter_next_no_least (It, FileIts, [ ?OSMOS_END | Acc ]);
iter_next_no_least (It = #iter { format = Fmt, key = OldKey },
		    [ FileIt | FileIts ],
		    Acc) ->
  NewFileIt = case key_less (Fmt, OldKey, osmos_file:iter_key (FileIt)) of
		true  -> FileIt;
		false -> osmos_file:iter_next (FileIt)
	      end,
  case NewFileIt of
    ?OSMOS_END ->
      iter_next_no_least (It, FileIts, [ ?OSMOS_END | Acc ]);
    _ ->
      NewIt = It#iter { key = osmos_file:iter_key (NewFileIt),
			value = osmos_file:iter_value (NewFileIt) },
      iter_next_maybe_sc (NewIt, OldKey, FileIts, [ NewFileIt | Acc ])
  end.

% after a new least key has been found, short_circuit has not returned true
iter_next_least (It, _OldKey, [], Acc) ->
  iter_next_maybe_delete (It#iter { file_its = lists:reverse (Acc) });
iter_next_least (It, OldKey, [ ?OSMOS_END | FileIts ], Acc) ->
  iter_next_least (It, OldKey, FileIts, [ ?OSMOS_END | Acc ]);
iter_next_least (It, OldKey, [ FileIt | FileIts ], Acc) ->
  Fmt = It#iter.format,
  NewFileIt = case key_less (Fmt, OldKey, osmos_file:iter_key (FileIt)) of
		true  -> FileIt;
		false -> osmos_file:iter_next (FileIt)
	      end,
  NewAcc = [ NewFileIt | Acc ],
  case NewFileIt of
    ?OSMOS_END ->
      iter_next_least (It, OldKey, FileIts, NewAcc);
    _ ->
      NewFileKey = osmos_file:iter_key (NewFileIt),
      LeastKey = It#iter.key,
      case key_less (Fmt, NewFileKey, LeastKey) of
	true ->
	  NewIt = It#iter { key = NewFileKey,
			    value = osmos_file:iter_value (NewFileIt) },
	  iter_next_maybe_sc (NewIt, OldKey, FileIts, NewAcc);
	false ->
	  case key_less (Fmt, LeastKey, NewFileKey) of
	    true ->
	      iter_next_least (It, OldKey, FileIts, [ NewFileIt | Acc ]);
	    false ->
	      NewFileValue = osmos_file:iter_key (NewFileIt),
	      LeastValue = merge (Fmt, LeastKey, NewFileValue, It#iter.value),
	      NewIt = It#iter { value = LeastValue },
	      iter_next_maybe_sc (NewIt, OldKey, FileIts, NewAcc)
	  end
      end
  end.

% after a new least key has been found and short_circuit returned true
iter_next_least_sc (It, _OldKey, [], Acc) ->
  iter_next_maybe_delete (It#iter { file_its = lists:reverse (Acc) });
iter_next_least_sc (It, OldKey, [ ?OSMOS_END | FileIts ], Acc) ->
  iter_next_least_sc (It, OldKey, FileIts, [ ?OSMOS_END | Acc ]);
iter_next_least_sc (It, OldKey, [ FileIt | FileIts ], Acc) ->
  Fmt = It#iter.format,
  NewFileIt = case key_less (Fmt, OldKey, osmos_file:iter_key (FileIt)) of
		true  -> FileIt;
		false -> osmos_file:iter_next (FileIt)
	      end,
  NewAcc = [ NewFileIt | Acc ],
  case NewFileIt of
    ?OSMOS_END ->
      iter_next_least_sc (It, OldKey, FileIts, NewAcc);
    _ ->
      NewFileKey = osmos_file:iter_key (NewFileIt),
      case key_less (Fmt, NewFileKey, It#iter.key) of
	true ->
	  NewIt = It#iter { key = NewFileKey,
			    value = osmos_file:iter_value (NewFileIt) },
	  iter_next_maybe_sc (NewIt, OldKey, FileIts, NewAcc);
	false ->
	  % NewFileKey is greater than or equal to the current least key, so
	  % we are either ignoring (if greater) or short-circuiting (if equal)
	  iter_next_least_sc (It, OldKey, FileIts, NewAcc)
      end
  end.

iter_next_maybe_delete (It = #iter { format = Fmt, key = K, value = V }) ->
  case delete (Fmt, K, V) of
    true  -> iter_next (It);
    false -> It
  end.

% add reference counts to tree for each path with a valid iterator
iter_path_refs (?OSMOS_END, Tree) ->
  Tree;
iter_path_refs (#iter { file_its = FileIts }, Tree) ->
  lists:foldl (fun (FileIt, T) ->
		 case FileIt of
		   ?OSMOS_END ->
		     T;
		   _ ->
		     Path = osmos_file:path (osmos_file:iter_file (FileIt)),
		     case gb_trees:lookup (Path, T) of
		       { value, N } ->
			 gb_trees:update (Path, N + 1, T);
		       none ->
			 gb_trees:insert (Path, 1, T)
		     end
		 end
	       end,
	       Tree,
	       FileIts).

%
% writing
%

handle_write (Table = #table { format = Fmt, tree = OldTree }, Key, Value) ->
  KeyBin = ?osmos_table_format_key_to_binary (Fmt, Key),
  ValueBin = ?osmos_table_format_value_to_binary (Fmt, Value),
  journal_write (Table, KeyBin, ValueBin),
  case osmos_tree:search (OldTree, Key) of
    { ok, OldValue } ->
      NewValue = merge (Table, Key, OldValue, Value),
      NewTree = osmos_tree:update (OldTree, Key, NewValue),
      Table#table { tree = NewTree };
    not_found ->
      NewTree = osmos_tree:insert (OldTree, Key, Value),
      NewTable = Table#table { tree = NewTree },
      case osmos_tree:total_entries (NewTree) < Table#table.max_memory_entries
      of
	true  -> NewTable;
	false -> flush_tree (NewTable)
      end
  end.

flush_tree (Table = #table { directory = Dir,
			     format = Fmt,
			     n_file_entries = NEntries,
			     n_files = NFiles,
			     files = Files }) ->
  ok = journal_close (Table),
  Path = new_file_path (),
  FullPath = dir_path (Dir, Path),
  { ok, _ } = osmos_file:build (FullPath,
				Fmt,
				fun (It) ->
				  case It of
				    ?OSMOS_END ->
				      { eof, ?OSMOS_END };
				    _ ->
				      K = osmos_tree:iter_key (It),
				      V = osmos_tree:iter_value (It),
				      NewIt = osmos_tree:iter_next (It),
				      { ok, K, V, NewIt }
				  end
				end,
				osmos_tree:iter_begin (Table#table.tree)),
  Less = Fmt#osmos_table_format.key_less,
  File = osmos_file:open (Fmt, FullPath),
  OldJournalPath = Table#table.journal_path,
  NewJournalPath = new_journal_path (),
  NewJournalIo = journal_open (Dir, NewJournalPath),
  NewNEntries = NEntries + osmos_file:total_entries (File),
  NewPathRefs = gb_trees:insert (Path, 1, Table#table.path_refs),
  NewPathFiles = gb_trees:insert (Path, File, Table#table.path_files),
  NewTable = Table#table { tree = osmos_tree:new (Less),
			   journal_path = NewJournalPath,
			   journal_io = NewJournalIo,
			   n_file_entries = NewNEntries,
			   n_files = NFiles + 1,
			   files = [ { Path, File } | Files ],
			   path_refs = NewPathRefs,
			   path_files = NewPathFiles },
  table_state_write (NewTable),
  delete_file (Dir, OldJournalPath),
  try_merge (NewTable).

%
% merge job management
%

try_merge (Table = #table { n_files = N }) when N < 2 ->
  Table;
try_merge (Table = #table { max_merge_jobs = MaxJobs,
			    n_files = N,
			    merge_jobs = Jobs })
	when N > 1, length (Jobs) < MaxJobs ->
  try_merge (Table, Table#table.files, ?MAX_MERGE_RATIO, none, none);
try_merge (Table = #table { max_merge_jobs = MaxJobs,
			    n_files = N,
			    merge_jobs = Jobs })
	when N > 1, length (Jobs) =:= MaxJobs ->
  MaxFiles = max_files (Table),
  case N > MaxFiles of
    true ->
      error_logger:warning_msg ("osmos_table ~s overloaded: ~b entries, "
				"~b files, target ~b files~n",
				[ Table#table.directory,
				  Table#table.n_file_entries,
				  N,
				  MaxFiles ]);
    false ->
      ok
  end,
  Table.

try_merge (Table,
	   [ { PNewer, FNewer }, { POldest, FOldest } ],
	   BestRatio,
	   BestOlder,
	   BestNewer) ->
  { NewBestRatio, NewBestOlder, NewBestNewer, ApplyDelete } =
    case is_merging (Table, PNewer) orelse is_merging (Table, POldest) of
      true ->
	{ BestRatio, BestOlder, BestNewer, false };
      false ->
	Ratio = osmos_file:total_entries (FOldest)
	      / osmos_file:total_entries (FNewer),
	case Ratio < BestRatio of
	  true ->
	    % apply the delete function only when merging the earliest file
	    { Ratio, POldest, PNewer, true };
	  false ->
	    { BestRatio, BestOlder, BestNewer, false }
	end
    end,
  case NewBestOlder =/= none andalso NewBestNewer =/= none of
    true ->
      start_merge_job (Table,
		       NewBestRatio,
		       NewBestOlder,
		       NewBestNewer,
		       ApplyDelete);
    false ->
      Table
  end;
try_merge (Table,
	   [ { PNewer, FNewer } | Files = [ { POlder, FOlder } | _ ] ],
	   BestRatio,
	   BestOlder,
	   BestNewer) ->
  case is_merging (Table, PNewer) orelse is_merging (Table, POlder) of
    true ->
      try_merge (Table, Files, BestRatio, BestOlder, BestNewer);
    false ->
      Ratio = osmos_file:total_entries (FOlder)
	    / osmos_file:total_entries (FNewer),
      case Ratio < BestRatio of
	true  -> try_merge (Table, Files, Ratio, POlder, PNewer);
	false -> try_merge (Table, Files, BestRatio, BestOlder, BestNewer)
      end
  end.

is_merging (#table { merge_jobs = Jobs }, Path) ->
  lists:keymember (Path, 2, Jobs) orelse lists:keymember (Path, 3, Jobs).

start_merge_job (Table = #table { merge_jobs = Jobs, n_files = NFiles },
		 Ratio, PathA, PathB, ApplyDelete) ->
  TooManyFiles = NFiles > max_files (Table),
  case Ratio < ?MAX_MERGE_RATIO orelse TooManyFiles of
    false ->
      Table;
    true ->
      PathOut = new_file_path (),
      Fmt = Table#table.format,
      Delete = case ApplyDelete of
		 true  -> Fmt#osmos_table_format.delete;
		 false -> undefined
	       end,
      Pid = proc_lib:spawn_link (?MODULE,
				 merge_process,
				 [ Table#table.directory,
				   PathA,
				   PathB,
				   PathOut,
				   Fmt,
				   Delete ]),
      Table#table { merge_jobs = [ { Pid, PathA, PathB, PathOut } | Jobs ] }
  end.

merge_process (Dir, PathA, PathB, PathOut, Fmt, Delete) ->
  FileA = osmos_file:open (Fmt, dir_path (Dir, PathA)),
  FileB = osmos_file:open (Fmt, dir_path (Dir, PathB)),
  FullPathOut = dir_path (Dir, PathOut),
  Merge = Fmt#osmos_table_format.merge,
  case is_function (Delete) of
    false ->
      ok = osmos_file:merge (FileA, FileB, FullPathOut, Fmt, Merge);
    true ->
      ok = osmos_file:merge_delete (FileA, FileB, FullPathOut,
				    Fmt, Merge, Delete)
  end,
  ok = osmos_file:close (FileB),
  ok = osmos_file:close (FileA).

merge_done (Table = #table { directory = Dir,
			     format = Fmt,
			     n_file_entries = NEntries,
			     n_files = NFiles,
			     files = Files,
			     merge_jobs = Jobs },
	    Pid) ->
  case lists:keysearch (Pid, 1, Jobs) of
    { value, { Pid, PathA, PathB, NewPath } } ->
      NewFile = osmos_file:open (Fmt, dir_path (Dir, NewPath)),
      { NewFiles, FileA, FileB } =
	replace_merged (PathA, PathB, { NewPath, NewFile }, Files, []),
      NA = osmos_file:total_entries (FileA),
      NB = osmos_file:total_entries (FileB),
      NNew = osmos_file:total_entries (NewFile),
      NewPathFiles = gb_trees:insert (NewPath, NewFile, Table#table.path_files),
      NewJobs = lists:keydelete (Pid, 1, Jobs),
      NewTable = Table#table { n_file_entries = NEntries - NA - NB + NNew,
			       n_files = NFiles - 1,
			       files = NewFiles,
			       path_files = NewPathFiles,
			       merge_jobs = NewJobs },
      table_state_write (NewTable),
      try_merge (fix_path_refs (NewTable));
    _ ->
      Table
  end.

replace_merged (PathA, PathB, NewFile,
		[ { PathB, FileB }, { PathA, FileA } | Files ], Acc) ->
  { lists:reverse (Acc) ++ [ NewFile | Files ], FileA, FileB };
replace_merged (PathA, PathB, NewFile, [ File | Files ], Acc) ->
  replace_merged (PathA, PathB, NewFile, Files, [ File | Acc ]).

%
% table_info
%

table_info (Table) ->
  [ { What, table_info (Table, What) }
    || What <- [ file_entries, files, tree_entries ] ].

table_info (#table { n_file_entries = N }, file_entries) ->
  N;
table_info (#table { n_files = N }, files) ->
  N;
table_info (#table { tree = T }, tree_entries) ->
  osmos_tree:total_entries (T);
table_info (_, _) ->
  undefined.

%
% misc
%

max_files (#table { max_memory_entries = MinFileSize,
		    n_file_entries = NEntries }) ->
  % TODO: this is the upper bound (and twice the average)
  % for the ideal powers-of-two case with unique keys; how
  % to account for collisions and deletions?
  1 + round (math:log (NEntries / MinFileSize) / math:log (2.0)).

journal_open (Dir, JournalPath) ->
  { ok, JournalIo } = file:open (dir_path (Dir, JournalPath),
				 [ write, raw, binary ]),
  ok = file:write (JournalIo, ?JOURNAL_MAGIC_BEGIN),
  ok = file:sync (JournalIo),
  JournalIo.

journal_close (Table) ->
  ok = file:close (Table#table.journal_io).

journal_write (Table, Key, Value) ->
  KeySize = size (Key),
  ValueSize = size (Value),
  ok = file:write (Table#table.journal_io,
		   [ ?JOURNAL_MAGIC_ENTRY_BEGIN,
		     << KeySize:?JOURNAL_ENTRY_SIZE >>,
		     << ValueSize:?JOURNAL_ENTRY_SIZE >>,
		     Key,
		     Value,
		     ?JOURNAL_MAGIC_ENTRY_END ]).

journal_read (Table = #table { format = Fmt }, Path) ->
  { ok, Io } = file:open (Path, [ read, raw, binary ]),
  { ok, ?JOURNAL_MAGIC_BEGIN } = file:read (Io, ?JOURNAL_MAGIC_BYTES),
  Less = Fmt#osmos_table_format.key_less,
  journal_read (Table, Io, osmos_tree:new (Less)).

journal_read (Table, Io, Tree) ->
  case file:read (Io, ?JOURNAL_MAGIC_BYTES + 2 * ?JOURNAL_ENTRY_SIZE_BYTES) of
    eof ->
      ok = file:close (Io),
      Tree;
    { ok, Header } ->
      MagicBegin = ?JOURNAL_MAGIC_ENTRY_BEGIN,
      case Header of
	<< MagicBegin:?JOURNAL_MAGIC_BYTES/binary,
	   KeySize:?JOURNAL_ENTRY_SIZE,
	   ValueSize:?JOURNAL_ENTRY_SIZE >> ->
	  journal_read_key_value (Table, Io, Tree, KeySize, ValueSize);
	_ ->
	  ok = file:close (Io),
	  Tree
      end
  end.

journal_read_key_value (Table, Io, Tree, KeySize, ValueSize) ->
  case file:read (Io, KeySize + ValueSize + ?JOURNAL_MAGIC_BYTES) of
    eof ->
      ok = file:close (Io),
      Tree;
    { ok, Data } ->
      MagicEnd = ?JOURNAL_MAGIC_ENTRY_END,
      case Data of
	<< KeyBin:KeySize/binary,
	   ValueBin:ValueSize/binary,
	   MagicEnd:?JOURNAL_MAGIC_BYTES/binary >> ->
	  Fmt = Table#table.format,
	  Key = ?osmos_table_format_key_from_binary (Fmt, KeyBin),
	  Value = ?osmos_table_format_value_from_binary (Fmt, ValueBin),
	  NewTree =
	    case osmos_tree:search (Tree, Key) of
	      { ok, OldValue } ->
		NewValue = merge (Table, Key, OldValue, Value),
		osmos_tree:update (Tree, Key, NewValue);
	      not_found ->
		osmos_tree:insert (Tree, Key, Value)
	    end,
	  journal_read (Table, Io, NewTree);
	_ ->
	  ok = file:close (Io),
	  Tree
      end
  end.

table_state_write (#table { directory = Dir,
			    format = Fmt,
			    journal_path = JournalPath,
			    n_files = NFiles,
			    files = Files }) ->
  BlockSize = Fmt#osmos_table_format.block_size,
  JournalPathBin = list_to_binary (JournalPath),
  FilePaths = [ list_to_binary (Path) || { Path, _ } <- Files ],
  State = iolist_to_binary ([ ?STATE_MAGIC_BEGIN,
			      vli32_write (BlockSize),
			      vli32_write (size (JournalPathBin)),
			      JournalPathBin,
			      vli32_write (NFiles),
			      [ [ vli32_write (size (P)), P ]
				|| P <- FilePaths ],
			      ?STATE_MAGIC_END ]),
  TmpPath = new_path (tmp),
  ok = file:write_file (dir_path (Dir, TmpPath), State),
  ok = file:rename (dir_path (Dir, TmpPath), state_path (Dir)).

table_state_read (Dir) ->
  { ok, B0 } = file:read_file (state_path (Dir)),
  MagicBegin = ?STATE_MAGIC_BEGIN,
  << MagicBegin:?STATE_MAGIC_BYTES/binary, B1/binary >> = B0,
  { BlockSize, B2 } = vli32_read (B1),
  { JournalPathSize, B3 } = vli32_read (B2),
  << JournalPathBin:JournalPathSize/binary, B4/binary >> = B3,
  { NFiles, B5 } = vli32_read (B4),
  Files = table_state_read_files (B5, []),
  { BlockSize, binary_to_list (JournalPathBin), NFiles, Files }.

table_state_read_files (B, Acc)
      when is_binary (B), size (B) > ?STATE_MAGIC_BYTES ->
  { Size, B1 } = vli32_read (B),
  << PathBin:Size/binary, B2/binary >> = B1,
  table_state_read_files (B2, [ binary_to_list (PathBin) | Acc ]);

table_state_read_files (?STATE_MAGIC_END, Acc) ->
  lists:reverse (Acc).

new_file_path () ->
  new_path (data).

new_journal_path () ->
  new_path (journal).

new_path (Type) ->
  { A, B, C } = now (),
  lists:flatten (io_lib:format ("~s-~b-~6.10.0b-~6.10.0b-~s",
				[ Type, A, B, C, os:getpid () ])).
state_path (Dir) ->
  Dir ++ "/state".

dir_path (Dir, Filename) ->
  Dir ++ [ $/ | Filename ].

delete_file (Dir, Filename) ->
  ok = file:delete (dir_path (Dir, Filename)).

key_less (#table { format = Fmt }, A, B) ->
  (Fmt#osmos_table_format.key_less) (A, B);
key_less (#osmos_table_format { key_less = Less }, A, B) ->
  Less (A, B).

merge (#table { format = Fmt }, Key, OlderValue, NewerValue) ->
  (Fmt#osmos_table_format.merge) (Key, OlderValue, NewerValue);
merge (#osmos_table_format { merge = M }, Key, OlderValue, NewerValue) ->
  M (Key, OlderValue, NewerValue).

short_circuit (#table { format = Fmt }, Key, Value) ->
  (Fmt#osmos_table_format.short_circuit) (Key, Value);
short_circuit (#osmos_table_format { short_circuit = S }, Key, Value) ->
  S (Key, Value).

delete (#table { format = Fmt }, Key, Value) ->
  (Fmt#osmos_table_format.delete) (Key, Value);
delete (#osmos_table_format { delete = Delete }, Key, Value) ->
  Delete (Key, Value).

%
% tests
%

-ifdef (EUNIT).

make_setup () ->
  fun () ->
    { ok, [ App ] } = file:consult ("../src/osmos.app"),
    ok = application:load (App),
    ok = osmos:start ()
  end.

make_teardown (Dir) ->
  fun (_) ->
    osmos:stop (),
    application:unload (osmos),
    os:cmd ("rm -rf '" ++ Dir ++ "'")
  end.

test_tmp_path (Line) ->
  lists:flatten (io_lib:format ("tmp-test-~s-~s-~b",
				[ ?MODULE, os:getpid (), Line ])).

basic_test_ () ->
  Dir = test_tmp_path (?LINE),
  { setup,
    make_setup (),
    make_teardown (Dir),
    fun () ->
      Format = osmos_table_format:new (binary, binary_replace, 64),
      Options = [ { directory, Dir },
		  { format, Format },
		  { max_memory_entries, 4 } ],
      Table = foo,

      { ok, Table } = osmos:open (Table, Options),
      List1 = [ <<"foo1">>, <<"bar1">>, <<"baz1">>, <<"quux1">>, <<"blorf1">> ],
      ok = assert_write_md5 (?LINE, Table, List1),
      ok = assert_read_md5 (?LINE, Table, List1),
      ok = osmos:close (Table),

      { ok, Table } = osmos:open (Table, Options),
      ok = assert_read_md5 (?LINE, Table, List1),
      List2 = [ <<"foo2">>, <<"bar2">>, <<"baz2">>, <<"quux2">>, <<"blorf2">> ],
      ok = assert_write_md5 (?LINE, Table, List2),
      ok = assert_read_md5 (?LINE, Table, List2),
      ok = assert_read_md5 (?LINE, Table, List1),
      ok = osmos:close (Table),

      { ok, Table } = osmos:open (Table, Options),
      ok = assert_read_md5 (?LINE, Table, List1),
      ok = assert_read_md5 (?LINE, Table, List2),
      List3 = [ <<"foo3">>, <<"bar3">>, <<"baz3">>, <<"quux3">>, <<"blorf3">> ],
      ok = assert_write_md5 (?LINE, Table, List3),
      ok = assert_read_md5 (?LINE, Table, List1),
      ok = assert_read_md5 (?LINE, Table, List2),
      ok = assert_read_md5 (?LINE, Table, List3),
      timer:sleep (1000),	% wait for merge to complete
      ok = assert_read_md5 (?LINE, Table, List1),
      ok = assert_read_md5 (?LINE, Table, List2),
      ok = assert_read_md5 (?LINE, Table, List3),
      ok = osmos:close (Table),

      ok
    end
  }.

assert_write_md5 (Line, Table, [ Key | Keys ]) ->
  case osmos:write (Table, Key, erlang:md5 (Key)) of
    ok -> ok;
    Bad -> throw ({ write_error, Line, Key, Bad })
  end,
  assert_write_md5 (Line, Table, Keys);
assert_write_md5 (_Line, _Table, []) ->
  ok.

assert_read_md5 (Line, Table, [ Key | Keys ]) ->
  Expected = erlang:md5 (Key),
  case osmos:read (Table, Key) of
    { ok, Expected } -> ok;
    Bad -> throw ({ read_error, Line, Key, Bad })
  end,
  assert_read_md5 (Line, Table, Keys);
assert_read_md5 (_Line, _Table, []) ->
  ok.

medium_test_ () ->
  Dir = test_tmp_path (?LINE),
  { setup,
    make_setup (),
    make_teardown (Dir),
    { timeout,
      60,
      fun () ->
	Format = osmos_table_format:new (binary, binary_replace, 4096),
	Options = [ { directory, Dir },
		    { format, Format },
		    { max_memory_entries, 1024 } ],
	Table = foo,

	{ ok, Table } = osmos:open (Table, Options),
	ok = medium_test_read_write (Table, 1, 0, 10000),
	ok = osmos:close (Table),

	{ ok, Table } = osmos:open (Table, Options),
	ok = medium_test_read_write (Table, 1/2, 10000, 20000),
	ok = medium_test_read_write (Table, 1/3, 20000, 30000),
	ok = medium_test_read_write (Table, 1/4, 30000, 40000),
	ok = medium_test_read_write (Table, 0, 40000, 50000),
	ok = osmos:close (Table),

	ok
      end
    }
  }.

medium_test_read_write (Table, PRead, Start, Max) ->
  T0 = now (),
  ok = medium_test_loop (Table, PRead, Start, Max),
  D = timer:now_diff (now (), T0),
  error_logger:info_msg ("medium_test ~p r-w/sec with P(read)=~p (~b)~n",
			[ 1000000 * (Max - Start) / D, PRead, (Max - Start) ]),
  ok.

medium_test_loop (_Table, _PRead, Max, Max) ->
  ok;
medium_test_loop (Table, PRead, N, Max) ->
  WriteKey = medium_test_key (N),
  WriteValue = medium_test_value (WriteKey),
  case osmos:write (Table, WriteKey, WriteValue) of
    ok -> ok;
    BadWrite -> throw ({ write_error, ?LINE, WriteKey, BadWrite })
  end,
  case random:uniform () < PRead of
    false ->
      ok;
    true ->
      ReadN = random:uniform (N+1) - 1,
      ReadKey = medium_test_key (ReadN),
      ReadValue = medium_test_value (ReadKey),
      case osmos:read (Table, ReadKey) of
	{ ok, ReadValue } -> ok;
	BadRead -> throw ({ read_error, ?LINE, ReadKey, ReadValue, BadRead })
      end
  end,
  medium_test_loop (Table, PRead, N+1, Max).

medium_test_key (N) ->
  << N:32/big-unsigned-integer >>.

medium_test_value (Key) ->
  N = size (Key) - 1,
  << _:N/binary, _:4, Len:4 >> = Key,
  Suffix = list_to_binary (lists:duplicate (Len, 0)),
  << Key/binary, Suffix/binary >>.

select_test_ () ->
  Dir = test_tmp_path (?LINE),
  { setup,
    make_setup (),
    make_teardown (Dir),
    { timeout,
      60,
      fun () ->
	Format = osmos_table_format:new (binary, binary_replace, 4096),
	Options = [ { directory, Dir },
		    { format, Format },
		    { max_memory_entries, 1024 } ],
	Table = foo,
	{ ok, Table } = osmos:open (Table, Options),
	ok = medium_test_loop (Table, 0.0, 1, 100000),
	ok = select_test_select (Table, 1, 100000,     0,    137, 100),
	ok = select_test_select (Table, 1, 100000, 13427,  15118, 100),
	ok = select_test_select (Table, 1, 100000, 99997, 100002, 100),
	ok = osmos:close (Table),
	ok
      end
    }
  }.

select_test_select (Table, Min, Max, Lo, Hi, N) ->
  LoKey = medium_test_key (Lo),
  LessLo = fun (K) -> K < LoKey end,
  HiKey = medium_test_key (Hi),
  LessHi = fun (K) -> K < HiKey end,
  Select = fun (_K, _V) -> true end,
  SelectLo = case Lo < Min of
	       true -> Min;
	       false -> Lo
	     end,
  SelectHi = case Hi > Max of
	       true -> Max;
	       false -> Hi
	     end,
  LoPlusN = SelectLo + N,
  ChunkHi = case LoPlusN > SelectHi of
	      true -> SelectHi;
	      false -> LoPlusN
	    end,
  Expected = case ChunkHi > SelectLo of
	       true ->
		 lists:map (fun (X) ->
			      K = medium_test_key (X),
			      { K, medium_test_value (K) }
			    end,
			    lists:seq (SelectLo, ChunkHi - 1));
	       false ->
		 []
	     end,
  N0 = N div 2,
  N1 = N - N0,
  SplitLen = case N0 > length (Expected) of
	       true  -> length (Expected);
	       false -> N0
	     end,
  { Expected0, Expected1 } = lists:split (SplitLen, Expected),
  case osmos:select_range (Table, LessLo, LessHi, Select, N0) of
    { ok, Entries0, Cont } ->
      case Entries0 =:= Expected0 of
	false ->
	  throw ({ select_wrong, ?LINE, Expected0, Entries0 });
	true ->
	  case osmos:select_continue (Table, Cont, N1) of
	    { ok, Entries1, _C } ->
	      case Entries1 =:= Expected1 of
		false ->
		  throw ({ select_wrong, ?LINE, Expected1, Entries1 });
		true ->
		  ok
	      end;
	    Bad1 ->
	      throw ({ select_error, ?LINE, Bad1 })
	  end
      end;
    Bad0 ->
      throw ({ select_error, ?LINE, Bad0 })
  end.

sum_test_ () ->
  Dir = test_tmp_path (?LINE),
  { setup,
    make_setup (),
    make_teardown (Dir),
    { timeout,
      300,
      fun () ->
	Format = osmos_table_format:new (term, uint64_sum_delete, 4096),
	Options = [ { directory, Dir },
		    { format, Format },
		    { max_memory_entries, 8192 } ],
	Table = foo,
	{ ok, Table } = osmos:open (Table, Options),
	ok = sum_test_write_loop (Table, 10000),
	ok = sum_test_read_loop (Table, 10000),
	ok = osmos:close (Table),
	ok
      end
    }
  }.

sum_test_write_loop (Table, MaxN) ->
  sum_test_write_loop (Table, 1, 0, MaxN).
sum_test_write_loop (_Table, Pass, _N, _MaxN) when Pass > 64 ->
  ok;
sum_test_write_loop (Table, Pass, MaxN, MaxN) ->
  sum_test_write_loop (Table, Pass + 1, 0, MaxN);
sum_test_write_loop (Table, Pass, N, MaxN) ->
  WriteKey = sum_test_key (N),
  WriteValue = sum_test_write_value (N, Pass),
  case osmos:write (Table, WriteKey, WriteValue) of
    ok -> ok;
    BadWrite -> throw ({ write_error, ?LINE, WriteKey, BadWrite })
  end,
  sum_test_write_loop (Table, Pass, N + 1, MaxN).

sum_test_read_loop (Table, MaxN) ->
  sum_test_read_loop (Table, 0, MaxN).
sum_test_read_loop (_Table, MaxN, MaxN) ->
  ok;
sum_test_read_loop (Table, N, MaxN) ->
  ReadKey = sum_test_key (N),
  Got = osmos:read (Table, ReadKey),
  case sum_test_read_value (N) of
    delete ->
      case Got of
	not_found -> ok;
	_ -> throw ({ read_error, ?LINE, ReadKey, Got, delete })
      end;
    Expected ->
      case Got of
	{ ok, Expected } -> ok;
	_ -> throw ({ read_error, ?LINE, ReadKey, Got, Expected })
      end
  end,
  sum_test_read_loop (Table, N + 1, MaxN).

sum_test_key (N) ->
  Md5 = << X:4, _:4, _/binary >> = erlang:md5 (term_to_binary (N)),
  << Y:X/binary, _/binary >> = Md5,
  [ N | binary_to_list (Y) ].

sum_test_read_value (N) when is_integer (N) ->
  List = binary_split_2 (erlang:md5 (term_to_binary (N))),
  sum_test_read_value (N, delete, List).
sum_test_read_value (_N, X, []) ->
  X;
sum_test_read_value (N, _, [ 0 | Rest ]) ->
  sum_test_read_value (N, delete, Rest);
sum_test_read_value (N, delete, [ B | Rest ]) ->
  sum_test_read_value (N, B * N, Rest);
sum_test_read_value (N, A, [ B | Rest ]) ->
  sum_test_read_value (N, A + B * N, Rest).

sum_test_write_value (N, Pass) when Pass >= 1, Pass =< 64 ->
  List = binary_split_2 (erlang:md5 (term_to_binary (N))),
  case lists:nth (Pass, List) of
    0 -> delete;
    B when is_integer (B) -> B * N
  end.

binary_split_2 (B) ->
  binary_split_2 (B, []).
binary_split_2 (<<>>, Acc) ->
  lists:reverse (Acc);
binary_split_2 (<<A:2, B:2, C:2, D:2, Rest/binary>>, Acc) ->
  binary_split_2 (Rest, [ D, C, B, A | Acc ]).

vector_test_ () ->
  Dir = test_tmp_path (?LINE),
  { setup,
    make_setup (),
    make_teardown (Dir),
    { timeout,
      600,
      fun () ->
	Format =
	  osmos_table_format:new (binary, uint64_vector_sum_delete, 4096),
	Options = [ { directory, Dir },
		    { format, Format },
		    { max_memory_entries, 1024 } ],
	Table = foo,
	{ ok, Table } = osmos:open (Table, Options),
	T0 = now (),
	NWrites = 1000000,
	Tree = vector_test_write_loop (Table, NWrites),
	DT = 1.0e-6 * timer:now_diff (now (), T0),
	error_logger:info_msg ("~b writes in ~p seconds, ~p writes/second~n",
			       [ NWrites, DT, NWrites / DT ]),
	ok = vector_test_verify (Table, Tree),
	ok = osmos:close (Table),
	ok
      end
    }
  }.

vector_test_write_loop (Table, N) ->
  vector_test_write_loop (Table, N, gb_trees:empty ()).

vector_test_write_loop (Table, N, Tree) when N > 0 ->
  KeyN = round (10000 * poisson ()),
  Remember = (KeyN < 10000 andalso (KeyN rem 7 =:= 0)),
  Key = erlang:md5 (term_to_binary (KeyN)),
  Vector = list_to_tuple ([ random:uniform (2) - 1 || _ <- lists:seq (1, 3) ]),
  NewTree = case Remember of
	      false ->
		Tree;
	      true ->
		case gb_trees:lookup (Key, Tree) of
		  { value, OldVector } ->
		    NewVector = sum_tuples (Vector, OldVector),
		    gb_trees:update (Key, NewVector, Tree);
		  none ->
		    gb_trees:insert (Key, Vector, Tree)
		end
	    end,
  ok = osmos:write (Table, Key, Vector),
  vector_test_write_loop (Table, N - 1, NewTree);
vector_test_write_loop (_Table, 0, Tree) ->
  Tree.

vector_test_verify (Table, Tree) ->
  vector_test_verify_loop (Table, gb_trees:iterator (Tree)).

vector_test_verify_loop (Table, It) ->
  case gb_trees:next (It) of
    { K, V, NewIt } ->
      case osmos:read (Table, K) of
	{ ok, V } ->
	  vector_test_verify_loop (Table, NewIt);
	{ ok, Other } ->
	  erlang:error ({ mismatch, K, V, Other });
	not_found ->
	  erlang:error ({ not_found, K, V })
      end;
    none ->
      ok
  end.

sum_tuples (T1, T2) ->
  Z = lists:zip (tuple_to_list (T1), tuple_to_list (T2)),
  list_to_tuple ([ N1 + N2 || { N1, N2 } <- Z ]).

poisson () ->
  X = random:uniform (),
  case X == 0 of
    true ->
      poisson ();
    false ->
      -math:log (X)
  end.

% issue 1 reported by japerk
japerk1_test_ () ->
  Dir = test_tmp_path (?LINE),
  { setup,
    make_setup (),
    make_teardown (Dir),
    fun () ->
      Fmt = osmos_table_format:new (term, term_replace, 256),
      { ok, Tab } = osmos:open ("test", [ { directory, Dir },
					  { format, Fmt } ]),
      ok = osmos:write (Tab, { "foo", "doc1" }, 0.5),
      ok = osmos:write (Tab, { "bar", "doc1" }, 0.5),

      L1 = fun ({ Term, _DocId }) -> Term < "bar" end,
      H1 = fun ({ Term, _DocId }) -> Term =< "bar" end,
      S1 = fun ({ Term, _DocId }, _Value) -> Term =:= "bar" end,
      { ok, Results1, _Cont1 } = osmos:select_range (Tab, L1, H1, S1, 10),
      ?assertMatch ([ { { "bar", "doc1" }, 0.5 } ], Results1),

      L2 = fun ({ Term, _DocId }) -> Term < "foo" end,
      H2 = fun ({ Term, _DocId }) -> Term =< "foo" end,
      S2 = fun ({ Term, _DocId }, _Value) -> Term =:= "foo" end,
      { ok, Results2, _Cont2 } = osmos:select_range (Tab, L2, H2, S2, 10),
      ?assertMatch ([ { { "foo", "doc1" }, 0.5 } ], Results2),
      ok
    end
  }.

-endif.
