%% @doc A method of encoding and decoding some set of Erlang
%% terms to and from binaries.
%%
%% An #osmos_format{} record has two fields:
%% <ul>
%% <li>to_binary (<tt>(any()) -> binary()</tt>): convert a term to binary.
%%     This is not required to accept all Erlang terms.</li>
%% <li>from_binary (<tt>(binary()) -> any()</tt>): convert a binary returned
%%     by the to_binary function back to the original term.</li>
%% </ul>
%%
%% The convenience functions in this module return #osmos_format{} records
%% for several common term encodings.
%% @end

-module (osmos_format).

% formats
-export ([ binary/0,
	   term/0,
	   string_vector/0,
	   uint64_delete/0,
	   uint64_vector_delete/0 ]).

% converters
-export ([ identity/1,
	   uint64_delete_to_binary/1,
	   uint64_delete_from_binary/1,
	   uint64_vector_delete_to_binary/1,
	   uint64_vector_delete_from_binary/1,
	   string_vector_to_binary/1,
	   string_vector_from_binary/1 ]).

-include ("osmos.hrl").

%% @spec () -> #osmos_format{}
%% @doc A binary. The binary format is the identical binary.
%% @end

binary () ->
  ToBinary = fun ?MODULE:identity/1,
  FromBinary = fun ?MODULE:identity/1,
  #osmos_format { to_binary = ToBinary, from_binary = FromBinary }.

%% @spec () -> #osmos_format{}
%% @doc An erlang term.
%% The binary format is the erlang external term format.
%% @end

term () ->
  ToBinary = fun erlang:term_to_binary/1,
  FromBinary = fun erlang:binary_to_term/1,
  #osmos_format { to_binary = ToBinary, from_binary = FromBinary }.

%% @spec () -> #osmos_format{}
%% @doc An integer in the range [0,2^64), or the atom 'delete'.
%% The binary format is an unsigned 64-bit big-endian integer, or
%% an empty binary to indicate deletion.
%% @end

uint64_delete () ->
  ToBinary = fun ?MODULE:uint64_delete_to_binary/1,
  FromBinary = fun ?MODULE:uint64_delete_from_binary/1,
  #osmos_format { to_binary = ToBinary, from_binary = FromBinary }.

%% @spec () -> #osmos_format{}
%% @doc A tuple of one or more integers, each in the range [0,2^64),
%% or the atom 'delete'. The binary format is a concatenated sequence
%% of big-endian, unsigned 64-bit integers, or an empty binary to
%% indicate deletion.
%% @end

uint64_vector_delete () ->
  ToBinary = fun ?MODULE:uint64_vector_delete_to_binary/1,
  FromBinary = fun ?MODULE:uint64_vector_delete_from_binary/1,
  #osmos_format { to_binary = ToBinary, from_binary = FromBinary }.

%% @spec () -> #osmos_format{}
%% @doc A tuple of strings as binaries, which may not contain ASCII NUL.
%% The binary format is the strings NUL-terminated and concatenated.
%% @end

string_vector () ->
  ToBinary = fun ?MODULE:string_vector_to_binary/1,
  FromBinary = fun ?MODULE:string_vector_from_binary/1,
  #osmos_format { to_binary = ToBinary, from_binary = FromBinary }.

%
% private
%

%% @hidden
identity (X) ->
  X.

%% @hidden
uint64_delete_to_binary (N) when is_integer (N) ->
  << N:64/big-unsigned-integer >>;
uint64_delete_to_binary (delete) ->
  <<>>.

%% @hidden
uint64_delete_from_binary (<< N:64/big-unsigned-integer >>) ->
  N;
uint64_delete_from_binary (<<>>) ->
  delete.

%% @hidden
uint64_vector_delete_to_binary (Tuple) when is_tuple (Tuple) ->
  iolist_to_binary ([ << C:64/big-unsigned-integer >>
		      || C <- tuple_to_list (Tuple) ]);
uint64_vector_delete_to_binary (delete) ->
  <<>>.

%% @hidden
uint64_vector_delete_from_binary (<<>>) ->
  delete;
uint64_vector_delete_from_binary (B) when is_binary (B), B =/= <<>> ->
  uint64_vector_from_binary (B, []).

uint64_vector_from_binary (<< C:64/big-unsigned-integer, B/binary >>, Acc) ->
  uint64_vector_from_binary (B, [ C | Acc ]);
uint64_vector_from_binary (<<>>, Acc) ->
  list_to_tuple (lists:reverse (Acc)).

%% @hidden
string_vector_to_binary (Tuple) when is_tuple (Tuple) ->
  iolist_to_binary ([ [ S, $\0 ] || S <- tuple_to_list (Tuple) ]).

%% @hidden
string_vector_from_binary (B) when is_binary (B) ->
  string_vector_from_binary (B, [], []).
string_vector_from_binary (<< Byte:8, Rest/binary >>, S, Acc) when Byte =/= 0 ->
  string_vector_from_binary (Rest, [ Byte | S ], Acc);
string_vector_from_binary (<< 0:8, Rest/binary >>, S, Acc) ->
  Str = list_to_binary (lists:reverse (S)),
  string_vector_from_binary (Rest, [], [ Str | Acc ]);
string_vector_from_binary (<< >>, [], Acc) ->
  list_to_tuple (lists:reverse (Acc)).
