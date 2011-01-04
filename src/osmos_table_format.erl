%% @doc A table format controls the on-disk format of keys and values in
%% a table, and the rules for updating, querying, and deleting records
%% from the table.
%%
%% An #osmos_table_format{} record has the following fields:
%% <ul>
%% <li>block_size (<tt>integer()</tt>): block size of the table in bytes,
%%     controlling the size of disk reads (which are always whole blocks),
%%     and the fanout of the on-disk search trees. The block size should
%%     be at least several times larger than the average record in the
%%     table. If it is too small, there will be more wasted space, and
%%     more seeks and reads will be required to search the table. If it is
%%     too large, the table will consume more i/o bandwidth and CPU than
%%     necessary.</li>
%% <li>key_format (<tt>#osmos_format{}</tt>): on-disk format for keys.</li>
%% <li>key_less (<tt>(KeyA, KeyB) -> bool()</tt>): comparison function
%%     defining the order of keys in the table. Takes two native-format
%%     keys, and returns true if the first argument is less than the
%%     second argument.</li>
%% <li>value_format (<tt>#osmos_format{}</tt>): on-disk format for
%%     values.</li>
%% <li>merge (<tt>(Key, EarlierValue, LaterValue) -> MergedValue</tt>):
%%     function providing a rule for merging records with identical
%%     keys. Takes the key and the two values in the order they were
%%     written to the table, and returns a new value for the merged
%%     record.
%%
%%     The merge function must be associative, i.e.,
%%     <pre>
%%     Merge (K, Merge (K, V1, V2), V3) =:= Merge (K, V1, Merge (K, V2, V3))
%%     </pre>
%%     for any sequence of values V1, V2, V3 for a particular key K.
%%     That is, the final result of merging a sequence of values
%%     by temporally adjacent pairs must be the same no matter in which
%%     order the pairs are chosen.</li>
%% <li>short_circuit (<tt>(Key, Value) -> bool()</tt>): function
%%     which allows searches of the table to be terminated early
%%     (short-circuited) if it can be determined from a record that
%%     any earlier records with the same key are irrelevant. For
%%     example, if the merge function always returns the later
%%     value, so that a later value simply replaces an earlier
%%     value, then short_circuit should always return true. Or if
%%     there is a special value that indicates deletion of any
%%     previous record, then short_circuit should return true for that
%%     value.</li>
%% <li>delete (<tt>(Key, Value) -> bool()</tt>): function
%%     controlling when records are deleted from the table. If,
%%     after all merging is done for a particular key, this
%%     function returns true for the key's final record, the record
%%     will be deleted from the table, and reads will return
%%     not_found.</li>
%% </ul>
%% @end

-module (osmos_table_format).

-export ([ new/3,
	   valid/1 ]).

% helpers
-export ([ erlang_less/2,
	   always/2,
	   never/2,
	   merge_return_later/3,
	   merge_uint64_sum_delete/3,
	   value_is_delete/2,
	   merge_uint64_vector_sum_delete/3 ]).

-ifdef (HAVE_EUNIT).
-include_lib ("eunit/include/eunit.hrl").
-endif.

-include ("osmos.hrl").

%
% public
%

%% @spec (KeyType, ValueType, integer()) -> #osmos_table_format{}
%%       KeyType = binary | term | string_vector
%%       ValueType = term_replace | binary_replace | uint64_sum_delete
%%                 | uint64_vector_sum_delete
%% @doc Return a table format with the given key and value types, and
%% the given block size in bytes.
%%
%% The key type controls the ordering of keys and their on-disk format.
%% The recognized key types are:
%% <ul>
%% <li>term: keys are any Erlang term, compared using the term order;
%%     on-disk format is the external term format</li>
%% <li>binary: keys are binaries, compared lexically;
%%     on-disk format is the identical binary</li>
%% <li>string_vector: keys are tuples of strings represented as binaries,
%%     which may not contain ASCII NUL, and are compared lexically
%%     element by element; on-disk format is the strings NUL-terminated
%%     and concatenated.</li>
%% </ul>
%%
%% The value type controls the on-disk format of values and the semantics
%% of writing to the table. The recognized value types are:
%% <ul>
%% <li>term_replace: values are any Erlang term, and the on-disk format is
%%     the external term format; a write replaces any previous value for
%%     the key, and deletions are not supported.</li>
%% <li>binary_replace: values are binaries, and the on-disk format is the
%%     identical binary; a write replaces any previous value for the key,
%%     and deletions are not supported.</li>
%% <li>uint64_sum_delete: values are integers in [0,2^64), and the on-disk
%%     format is 64-bit big-endian; writing an integer adds to the existing
%%     value if any, while writing the atom 'delete' deletes any existing
%%     value.</li>
%% <li>uint64_vector_sum_delete: values are tuples of integers in [0,2^64),
%%     and the on-disk format is 64-bit big-endian concatenated; writing
%%     a vector adds to the existing value if any, while writing the atom
%%     'delete' deletes any existing value.</li>
%% </ul>
%%
%% The block size controls the size of reads from disk. It should be at
%% least several times larger than the average record in the table. (If it
%% is too small, there will be more wasted space, and more seeks and reads
%% will be required to search the table. If it is too large, the table will
%% consume more i/o bandwidth and CPU than necessary.)
%%
%% Note that you are free to build your own #osmos_table_format{} record
%% if these canned options don't meet your needs.
%% @end

new (KeyType, ValueType, BlockSize) when is_integer (BlockSize),
					 BlockSize >= ?OSMOS_MIN_BLOCK_SIZE ->
  F0 = #osmos_table_format { block_size = BlockSize },
  F1 = add_key_type (KeyType, F0),
  F2 = add_value_type (ValueType, F1),
  true = valid (F2),
  F2.

%% @spec (Format::#osmos_table_format{}) -> bool()
%% @doc Returns true if the argument appears to be a valid table format,
%% false otherwise.
%% @end

valid (#osmos_table_format { block_size = BlockSize,
			     key_format = KeyFormat = #osmos_format{},
			     key_less = KeyLess,
			     value_format = ValueFormat = #osmos_format{},
			     merge = Merge,
			     short_circuit = ShortCircuit,
			     delete = Delete })
	when is_integer (BlockSize), BlockSize >= ?OSMOS_MIN_BLOCK_SIZE,
	     is_function (KeyFormat#osmos_format.to_binary, 1),
	     is_function (KeyFormat#osmos_format.from_binary, 1),
	     is_function (KeyLess, 2),
	     is_function (ValueFormat#osmos_format.to_binary, 1),
	     is_function (ValueFormat#osmos_format.from_binary, 1),
	     is_function (Merge, 3),
	     is_function (ShortCircuit, 2),
	     is_function (Delete, 2) ->
  true;
valid (_) ->
  false.

%
% private
%

add_key_type (term, F) ->
  TermFormat = osmos_format:term (),
  ErlangLess = fun ?MODULE:erlang_less/2,
  F#osmos_table_format { key_format = TermFormat, key_less = ErlangLess };
add_key_type (binary, F) ->
  BinaryFormat = osmos_format:binary (),
  ErlangLess = fun ?MODULE:erlang_less/2,
  F#osmos_table_format { key_format = BinaryFormat, key_less = ErlangLess };
add_key_type (string_vector, F) ->
  StringVectorFormat = osmos_format:string_vector (),
  ErlangLess = fun ?MODULE:erlang_less/2,
  F#osmos_table_format { key_format = StringVectorFormat,
			 key_less = ErlangLess };
add_key_type (Type, _F) ->
  erlang:error ({ badarg, Type }).

add_value_type (term_replace, F) ->
  TermFormat = osmos_format:term (),
  MergeReturnLater = fun ?MODULE:merge_return_later/3,
  ShortCircuitAlways = fun ?MODULE:always/2,
  DeleteNever = fun ?MODULE:never/2,
  F#osmos_table_format { value_format = TermFormat,
			 merge = MergeReturnLater,
			 short_circuit = ShortCircuitAlways,
			 delete = DeleteNever };
add_value_type (binary_replace, F) ->
  BinaryFormat = osmos_format:binary (),
  MergeReturnLater = fun ?MODULE:merge_return_later/3,
  ShortCircuitAlways = fun ?MODULE:always/2,
  DeleteNever = fun ?MODULE:never/2,
  F#osmos_table_format { value_format = BinaryFormat,
			 merge = MergeReturnLater,
			 short_circuit = ShortCircuitAlways,
			 delete = DeleteNever };
add_value_type (uint64_sum_delete, F) ->
  BinaryFormat = osmos_format:uint64_delete (),
  Merge = fun ?MODULE:merge_uint64_sum_delete/3,
  ShortCircuit = fun ?MODULE:value_is_delete/2,
  Delete = fun ?MODULE:value_is_delete/2,
  F#osmos_table_format { value_format = BinaryFormat,
			 merge = Merge,
			 short_circuit = ShortCircuit,
			 delete = Delete };
add_value_type (uint64_vector_sum_delete, F) ->
  BinaryFormat = osmos_format:uint64_vector_delete (),
  Merge = fun ?MODULE:merge_uint64_vector_sum_delete/3,
  ShortCircuit = fun ?MODULE:value_is_delete/2,
  Delete = fun ?MODULE:value_is_delete/2,
  F#osmos_table_format { value_format = BinaryFormat,
			 merge = Merge,
			 short_circuit = ShortCircuit,
			 delete = Delete };
add_value_type (Type, _F) ->
  erlang:error ({ badarg, Type }).

%
% exported helpers
%

%% @hidden
erlang_less (A, B) ->
  A < B.

%% @hidden
always (_, _) ->
  true.

%% @hidden
never (_, _) ->
  false.

%% @hidden
merge_return_later (_Key, _EarlierValue, LaterValue) ->
  LaterValue.

%% @hidden
merge_uint64_sum_delete (_Key, Earlier, Later) when is_integer (Earlier),
						    is_integer (Later) ->
  Earlier + Later;
merge_uint64_sum_delete (_Key, _Earlier, Later) ->
  Later.

%% @hidden
value_is_delete (_Key, Value) ->
  Value =:= delete.

%% @hidden
merge_uint64_vector_sum_delete (_Key, Earlier, Later)
	when is_tuple (Earlier), is_tuple (Later) ->
  list_to_tuple (sum_ints (tuple_to_list (Earlier), tuple_to_list (Later), []));
merge_uint64_vector_sum_delete (_Key, _Earlier, Later) ->
  Later.

sum_ints ([ C1 | C1s ], [ C2 | C2s ], Acc) ->
  sum_ints (C1s, C2s, [ C1 + C2 | Acc ]);
sum_ints (C1s = [_|_], [], Acc) ->
  lists:reverse (lists:reverse (C1s) ++ Acc);
sum_ints ([], C2s = [_|_], Acc) ->
  lists:reverse (lists:reverse (C2s) ++ Acc);
sum_ints ([], [], Acc) ->
  lists:reverse (Acc).

%
% tests
%

-ifdef (EUNIT).

basic_test () ->
  ok.

-endif.
