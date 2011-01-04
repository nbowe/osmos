-module (osmos_benchmark_1).

-export ([ run/3 ]).

run (Seconds, WriterSleepMs, ReaderSleepMs) ->
  ok = osmos:start (),
  Dir = "tmp." ++ os:getpid (),
  Fmt = osmos_table_format:new (term, uint64_sum_delete, 1024),
  { ok, table } = osmos:open (table, [ { directory, Dir }, { format, Fmt } ]),
  Parent = self (),
  Start = now (),
  proc_lib:spawn (fun () ->
		    loop (Parent,
			  writer,
			  fun (K) ->
			    ok = osmos:write (table, K, 1)
			  end,
			  WriterSleepMs,
			  Start,
			  1000000 * Seconds,
			  [])
		  end),
  proc_lib:spawn (fun () ->
		    loop (Parent,
			  reader,
			  fun (K) ->
			    case osmos:read (table, K) of
			      not_found -> ok;
			      { ok, _ } -> ok
			    end
			  end,
			  ReaderSleepMs,
			  Start,
			  1000000 * Seconds,
			  [])
		  end),
  WStats = receive { writer, W } -> W end,
  RStats = receive { reader, R } -> R end,
  Info = osmos:info (table),
  io:format ("table info: ~p~n"
	     "write stats: ~p~n"
	     "read stats: ~p~n",
	     [Info, WStats, RStats]),
  ok = osmos:close (table),
  ok = osmos:stop (),
  os:cmd ("rm -rf " ++ Dir),
  ok.

loop (Parent, What, F, SleepMs, Start, Max, Acc) ->
  case timer:now_diff (now (), Start) > Max of
    false ->
      K = random_key (),
      T0 = now (),
      F (K),
      DT = timer:now_diff (now (), T0),
      timer:sleep (SleepMs),
      loop (Parent, What, F, SleepMs, Start, Max, [DT | Acc]);
    true ->
      Parent ! { What, stats (Acc) }
  end.

% want a spectrum of keys from frequent to unique
random_key () ->
  random_key (1/16, []).

random_key (P, Acc) ->
  case random:uniform () < P of
    true  -> list_to_binary (Acc);
    false -> random_key (P, [random_byte () | Acc])
  end.

random_byte () ->
  lists:sum ([ random:uniform (64) - 1 || _ <- lists:seq (1, 4) ]).

stats (Xs) ->
  N = length (Xs),
  Mean = lists:sum (Xs) / N,
  Sorted = lists:sort (Xs),
  Deciles = [ { 10 * Q, lists:nth (round (N * Q / 10), Sorted) }
	      || Q <- lists:seq (1, 9) ],
  [ { count, N }, { mean, Mean }, { deciles, Deciles } ].
