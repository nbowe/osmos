%% Based on gb_trees.erl from OTP stdlib distribution, with modifications
%% to allow user-defined comparison and the iterator_lower_bound function.
%% Original copyright notice follows:

%% ``The contents of this file are subject to the Erlang Public License,
%% Version 1.1, (the "License"); you may not use this file except in
%% compliance with the License. You should have received a copy of the
%% Erlang Public License along with this software. If not, it can be
%% retrieved via the world wide web at http://www.erlang.org/.
%% 
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and limitations
%% under the License.
%% 
%% The Initial Developer of the Original Code is Sven-Olof Nyström/Richard Carlsson.
%% Portions created by Ericsson are Copyright 1999, Ericsson Utvecklings
%% AB. All Rights Reserved.''
%% 
%%     $Id $
%% =====================================================================
%% General Balanced Trees - highly efficient dictionaries.
%%
%% Copyright (C) 1999-2001 Sven-Olof Nyström, Richard Carlsson
%%
%% An efficient implementation of Prof. Arne Andersson's General
%% Balanced Trees. These have no storage overhead compared to plain
%% unbalanced binary trees, and their performance is in general better
%% than AVL trees.
%% ---------------------------------------------------------------------
%% Operations:
%%
%% - empty(): returns empty tree.
%%
%% - size(T): returns the number of nodes in the tree as an integer.
%%   Returns 0 (zero) if the tree is empty.
%%
%% - lookup(Less, X, T): looks up key X in tree T; returns {value, V}, or
%%   `none' if the key is not present.
%%
%% - insert(Less, X, V, T): inserts key X with value V into tree T; returns
%%   the new tree. Assumes that the key is *not* present in the tree.
%%
%% - update(Less, X, V, T): updates key X to value V in tree T; returns the
%%   new tree. Assumes that the key is present in the tree.
%%
%% - iterator(T): returns an iterator that can be used for traversing
%%   the entries of tree T; see `next'. The implementation of this is
%%   very efficient; traversing the whole tree using `next' is only
%%   slightly slower than getting the list of all elements using
%%   `to_list' and traversing that. The main advantage of the iterator
%%   approach is that it does not require the complete list of all
%%   elements to be built in memory at one time.
%%
%% - iterator_lower_bound(T, L): returns an iterator pointing to the least
%%   entry for which the predicate L returns false; L must be consistent
%%   with the Less function used to build the tree.
%%
%% - next(S): returns {X, V, S1} where X is the smallest key referred to
%%   by the iterator S, and S1 is the new iterator to be used for
%%   traversing the remaining entries, or the atom `none' if no entries
%%   remain.

-module(osmos_gb_trees).

-export ([ empty/0,
	   lookup/3,
	   insert/4,
	   update/4,
	   size/1,
	   iterator/1,
	   iterator_lower_bound/2,
	   next/1 ]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Data structure:
%% - {Size, Tree}, where `Tree' is composed of nodes of the form:
%%   - {Key, Value, Smaller, Bigger}, and the "empty tree" node:
%%   - nil.
%%
%% I make no attempt to balance trees after deletions. Since deletions
%% don't increase the height of a tree, I figure this is OK.
%%
%% Original balance condition h(T) <= ceil(c * log(|T|)) has been
%% changed to the similar (but not quite equivalent) condition 2 ^ h(T)
%% <= |T| ^ c. I figure this should also be OK.
%%
%% Performance is comparable to the AVL trees in the Erlang book (and
%% faster in general due to less overhead); the difference is that
%% deletion works for my trees, but not for the book's trees. Behaviour
%% is logaritmic (as it should be).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Some macros. 

-define(p, 2). % It seems that p = 2 is optimal for sorted keys

-define(pow(A, _), A * A). % correct with exponent as defined above.

-define(div2(X), X bsr 1). 

-define(mul2(X), X bsl 1).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

empty() ->
    {0, nil}.

size({Size, _}) when is_integer(Size), Size >= 0 ->
    Size.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

lookup(Less, Key, {_, T}) ->
    lookup_1(Less, Key, T).

%% The term order is an arithmetic total order, so we should not
%% test exact equality for the keys. (If we do, then it becomes
%% possible that neither `>', `<', nor `=:=' matches.) Testing '<'
%% and '>' first is statistically better than testing for
%% equality, and also allows us to skip the test completely in the
%% remaining case.

%% [MR: same comments apply to the Less function used for comparison here.]

lookup_1(Less, Key, {Key1, Value, Smaller, Bigger}) ->
    case Less (Key, Key1) of
      true ->
	  lookup_1(Less, Key, Smaller);
      false ->
	  case Less (Key1, Key) of
	      true  -> lookup_1(Less, Key, Bigger);
	      false -> {value, Value}
	  end
    end;
lookup_1(_Less, _Key, nil) ->
    none.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

update(Less, Key, Val, {S, T}) ->
    T1 = update_1(Less, Key, Val, T),
    {S, T1}.

%% See `lookup' for notes on the term comparison order.

update_1(Less, Key, Value, {Key1, V, Smaller, Bigger}) ->
    case Less (Key, Key1) of
	true ->
	    {Key1, V, update_1(Less, Key, Value, Smaller), Bigger};
	false ->
	    case Less (Key1, Key) of
		true  -> {Key1, V, Smaller, update_1(Less, Key, Value, Bigger)};
		false -> {Key, Value, Smaller, Bigger}
	    end
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

insert(Less, Key, Val, {S, T}) when is_integer(S) ->
    S1 = S+1,
    {S1, insert_1(Less, Key, Val, T, ?pow(S1, ?p))}.

insert_1(Less, Key, Value, {Key1, V, Smaller, Bigger}, S) ->
  case Less (Key, Key1) of
    true ->
      case insert_1(Less, Key, Value, Smaller, ?div2(S)) of
	{T1, H1, S1} ->
	  T = {Key1, V, T1, Bigger},
	  {H2, S2} = count(Bigger),
	  H = ?mul2(max(H1, H2)),
	  SS = S1 + S2 + 1,
	  P = ?pow(SS, ?p),
	  if
	    H > P -> balance(T, SS);
	    true  -> {T, H, SS}
	  end;
	T1 ->
	  {Key1, V, T1, Bigger}
      end;
    false ->
      case Less (Key1, Key) of
	true ->
	  case insert_1(Less, Key, Value, Bigger, ?div2(S)) of
	    {T1, H1, S1} ->
	      T = {Key1, V, Smaller, T1},
	      {H2, S2} = count(Smaller),
	      H = ?mul2(max(H1, H2)),
	      SS = S1 + S2 + 1,
	      P = ?pow(SS, ?p),
	      if
		H > P -> balance(T, SS);
		true  -> {T, H, SS}
	      end;
	    T1 ->
	      {Key1, V, Smaller, T1}
	  end;
	false ->
	  erlang:error({key_exists, Key})
      end
  end;
insert_1(_Less, Key, Value, nil, S) when S =:= 0 ->
  {{Key, Value, nil, nil}, 1, 1};
insert_1(_Less, Key, Value, nil, _S) ->
  {Key, Value, nil, nil}.

max(X, Y) when X < Y ->
    Y;
max(X, _Y) ->
    X.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

count({_, _, nil, nil}) ->
    {1, 1};
count({_, _, Sm, Bi}) ->
    {H1, S1} = count(Sm),
    {H2, S2} = count(Bi),
    {?mul2(max(H1, H2)), S1 + S2 + 1};
count(nil) ->
    {1, 0}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

balance(T, S) ->
    balance_list(to_list_1(T), S).

balance_list(L, S) ->
    {T, []} = balance_list_1(L, S),
    T.

balance_list_1(L, S) when S > 1 ->
    Sm = S - 1,
    S2 = Sm div 2,
    S1 = Sm - S2,
    {T1, [{K, V} | L1]} = balance_list_1(L, S1),
    {T2, L2} = balance_list_1(L1, S2),
    T = {K, V, T1, T2},
    {T, L2};
balance_list_1([{Key, Val} | L], 1) ->
    {{Key, Val, nil, nil}, L};
balance_list_1(L, 0) ->
    {nil, L}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

to_list_1(T) -> to_list(T, []).

to_list({Key, Value, Small, Big}, L) ->
    to_list(Small, [{Key, Value} | to_list(Big, L)]);
to_list(nil, L) -> L.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

iterator({_, T}) ->
    iterator_1(T).

iterator_1(T) ->
    iterator(T, []).

%% The iterator structure is really just a list corresponding to
%% the call stack of an in-order traversal. This is quite fast.

iterator({_, _, nil, _} = T, As) ->
    [T | As];
iterator({_, _, L, _} = T, As) ->
    iterator(L, [T | As]);
iterator(nil, As) ->
    As.

iterator_lower_bound({_, T}, L) ->
    iterator_lower_bound_1(L, T, []).

iterator_lower_bound_1(L, {K, _V, Small, Big} = T, Stack) ->
    case L(K) of
	true  -> iterator_lower_bound_1(L, Big, Stack);
	false -> iterator_lower_bound_1(L, Small, [T | Stack])
    end;
iterator_lower_bound_1(_L, nil, Stack) ->
    Stack.

next([{X, V, _, T} | As]) ->
    {X, V, iterator(T, As)};
next([]) ->
    none.
