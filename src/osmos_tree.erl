-module (osmos_tree).

-export ([ new/1,
	   search/2,
	   insert/3,
	   update/3,
	   total_entries/1,
	   iter_begin/1,
	   iter_end/1,
	   iter_lower_bound/2,
	   iter_next/1,
	   iter_key/1,
	   iter_value/1 ]).

-ifdef (HAVE_EUNIT).
-include_lib ("eunit/include/eunit.hrl").
-endif.

-include ("osmos.hrl").

-record (tree, { less, tree }).
-record (iter, { key, value, iter }).

new (Less) ->
  #tree { less = Less, tree = osmos_gb_trees:empty () }.

search (#tree { less = Less, tree = Tree }, Key) ->
  case osmos_gb_trees:lookup (Less, Key, Tree) of
    { value, V } -> { ok, V };
    none         -> not_found
  end.

insert (T = #tree { less = Less, tree = Tree }, Key, Value) ->
  NewTree = osmos_gb_trees:insert (Less, Key, Value, Tree),
  T#tree { tree = NewTree }.

update (T = #tree { less = Less, tree = Tree }, Key, Value) ->
  NewTree = osmos_gb_trees:update (Less, Key, Value, Tree),
  T#tree { tree = NewTree }.

total_entries (#tree { tree = Tree }) ->
  osmos_gb_trees:size (Tree).

iter_begin (#tree { tree = Tree }) ->
  case osmos_gb_trees:next (osmos_gb_trees:iterator (Tree)) of
    { K, V, It } -> #iter { key = K, value = V, iter = It };
    none         -> ?OSMOS_END
  end.

iter_end (_Tree) ->
  ?OSMOS_END.

iter_lower_bound (#tree { tree = Tree }, Less) when is_function (Less) ->
  case osmos_gb_trees:next (osmos_gb_trees:iterator_lower_bound (Tree, Less)) of
    { K, V, It } -> #iter { key = K, value = V, iter = It };
    none         -> ?OSMOS_END
  end.

iter_next (#iter { iter = It }) ->
  case osmos_gb_trees:next (It) of
    { K, V, NewIt } -> #iter { key = K, value = V, iter = NewIt };
    none            -> ?OSMOS_END
  end.

iter_key (#iter { key = K }) ->
  K.
iter_value (#iter { value = V }) ->
  V.

%
% tests
%

-ifdef (EUNIT).

% issue 1 reported by japerk
japerk1_test () ->
  T0 = ?MODULE:new (fun osmos_table_format:erlang_less/2),
  T1 = ?MODULE:insert (T0, { "foo", "doc1" }, 0.5),
  T2 = ?MODULE:insert (T1, { "bar", "doc1" }, 0.5),
  Lbar = fun ({ Term, _DocId }) -> Term < "bar" end,
  Ibar = ?MODULE:iter_lower_bound (T2, Lbar),
  ?assertMatch ({ "bar", "doc1" }, ?MODULE:iter_key (Ibar)),
  Lfoo = fun ({ Term, _DocId }) -> Term < "foo" end,
  Ifoo = ?MODULE:iter_lower_bound (T2, Lfoo),
  ?assertMatch ({ "foo", "doc1" }, ?MODULE:iter_key (Ifoo)),
  ok.

lower_bound_test () ->
  lists:foreach (fun (N) ->
		   random_lower_bound_check (N)
		 end,
		 lists:seq (1, 100)).

random_lower_bound_check (N) ->
  L = lists:usort ([ random:uniform (1000000) || _ <- lists:seq (1, N) ]),
  T = lists:foldl (fun (K, T0) ->
		     ?MODULE:insert (T0, K, K)
		   end,
		   ?MODULE:new (fun osmos_table_format:erlang_less/2),
		   shuffle (L)),
  lists:foreach (fun (K) ->
		   Less = fun (X) -> X < K end,
		   It = ?MODULE:iter_lower_bound (T, Less),
		   Expected = [ { X, X } || X <- lists:dropwhile (Less, L) ],
		   Got = collect (It),
		   case Got =:= Expected of
		     true  -> ok;
		     false -> erlang:error ({ Expected, Got })
		   end
		 end,
		 [ hd (L) - 1, lists:last (L) + 1 | L ]).

shuffle (List) ->
  [ E || { _, E } <- lists:sort ([ { random:uniform (), E } || E <- List ]) ].

collect (?OSMOS_END) ->
  [];
collect (It) ->
  [ { ?MODULE:iter_key (It), ?MODULE:iter_value (It) }
    | collect (?MODULE:iter_next (It)) ].

-endif.
