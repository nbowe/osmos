%% @doc A sorted file of key-value records, supporting searching,
%% iteration, and merging.
%%
%% An osmos file is built by calling build/4 with a table format and
%% a sorted stream of records. The records are converted to binary by
%% calling the to_binary functions from the table format's key and
%% value formats, and then written to disk in order. Finally, an
%% index is constructed to allow efficient searching.
%%
%% Because the keys are written in order, they are prefix-compressed
%% to save space and i/o bandwidth. Many common key formats benefit from
%% prefix-compression, such as lexically sorted NUL-terminated strings,
%% sorted big-endian integers, and even most Erlang terms sorted by term
%% order and in external term format (integers over 32 bits are one
%% notable exception).
%%
%% After the file has been built, it can be opened for reading using
%% open/2. Because the physical file is opened in raw mode, each process
%% reading a given file must open its own handle.
%% @end

-module (osmos_file).

-export ([ build/4,
	   open/2,
	   close/1,
	   path/1,
	   total_entries/1,
	   search/2,
	   iter_begin/1,
	   iter_end/1,
	   iter_lower_bound/2,
	   iter_key/1,
	   iter_value/1,
	   iter_file/1,
	   iter_next/1,
	   iter_less/2,
	   iter_stream_next/1,
	   merge/5,
	   merge_delete/6 ]).

% FILE FORMAT
%
% leaf:
%	byte[4] leaf magic
%	vli32 n_values
%	vli32 lv0 = length value 0 [key is stored in level above!]
%	byte[lv0] value0
%	vli32 pk1 = prefix length 1
%	vli32 lk1 = key length 1
% 	byte[lk1] key1
%	vli32 lv1 = length value 1
%	byte[lv1] value1
%	...
%
% large leaf:
%	byte[4] large leaf magic
%	vli32 lv = length value [key is stored in level above!]
%	byte[lv] value
%
% large leaf continued:
%	byte[4] large leaf continued magic
%	byte[] value
%
% index:
%	byte[4] index magic
%	vli32 n_keys
% 	uint32 key 0 block number [key is stored in level above!]
%	vli32 pk1 = prefix length 1
%	vli32 lk1 = key length 1
% 	byte[lk1] key 1
%	uint32 key 1 block number
%	...
%
% index root:
%	byte[4] index root magic
%	vli32 n_keys
%	vli32 lk1 = key length 0
% 	byte[lk1] key 0
% 	uint32 key 0 block number
%	vli32 pk1 = prefix length 1
%	vli32 lk1 = key length 1
% 	byte[lk1] key 1
%	uint32 key 1 block number
%	...

-ifdef (HAVE_EUNIT).
-include_lib ("eunit/include/eunit.hrl").
-endif.

-include_lib ("kernel/include/file.hrl").
-include ("osmos.hrl").
-define (OSMOS_VLI_LENGTH, 1).
-include ("osmos_vli.hrl").

-define (ASSERT (X), (X)).

-define (MAGIC_BYTES, 4).
-define (MAGIC_HEADER,			<<69,42,69,3>>).
-define (MAGIC_LEAF,			<<69,69,69,3>>).
-define (MAGIC_LEAF_LARGE_START,	<<69,69,45,3>>).
-define (MAGIC_LEAF_LARGE_CONTINUE,	<<69,69,45,3>>).
-define (MAGIC_INDEX,			<<45,45,45,3>>).
-define (MAGIC_INDEX_ROOT,		<<45,69,45,3>>).

-define (LARGE_SIZE, 32/big-unsigned-integer).
-define (LARGE_SIZE_BYTES, 4).
-define (LARGE_SIZE_MAX, 16#ffffffff).
-define (BLOCK_NUMBER, 32/big-unsigned-integer).
-define (BLOCK_NUMBER_BYTES, 4).
-define (FILE_HEADER_SIZE, 64).

%
% building
%

%% @spec build (string(), #osmos_table_format{}, Stream, any())
%%         -> { ok, FinalStreamState }
%%       Stream = (State) -> { ok, Key::binary(), Value::binary(), NewState }
%%                         | { eof, FinalState }
%% @doc Build a searchable file at Path with the given Format and
%% BlockSize bytes per block, from the given sorted stream of records.
%% Return the final stream state (for any necessary cleanup etc.).
%% @end

build (Path, Format = #osmos_table_format{}, StreamNext, StreamState)
	when is_function (StreamNext) ->
  { ok, Io } = file:open (Path, [ write, raw, binary ]),
  % placeholder header; will seek back and rewrite the real header later
  ok = file:write (Io, pad (?FILE_HEADER_SIZE)),
  % Note: no delayed_write since we always write block-sized iodata
  { ok, IoTmp } = file:open (tmp_path (Path), [ write, raw, binary ]),
  write_leaves (Path, Format, StreamNext, StreamState, Io, IoTmp,
		new_leaf_block (0), 0).

write_leaves (Path, Fmt, StreamNext, StreamState,
	      Io, IoTmp, LeafBlock, TotalRecs) ->
  BlockSize = Fmt#osmos_table_format.block_size,
  case StreamNext (StreamState) of
    { ok, Key, Value, NewState } ->
      KeyBin = ?osmos_table_format_key_to_binary (Fmt, Key),
      ValueBin = ?osmos_table_format_value_to_binary (Fmt, Value),
      KeySize = size (KeyBin),
      MaxSize = max_key_bytes (BlockSize),
      case KeySize > MaxSize of
	true  -> erlang:error ({ key_too_large, KeySize, MaxSize });
	false -> ok
      end,
      NewLeafBlock = add_leaf_record (BlockSize, Io, IoTmp, KeyBin, ValueBin,
				      LeafBlock),
      write_leaves (Path, Fmt, StreamNext, NewState,
		    Io, IoTmp, NewLeafBlock, TotalRecs + 1);
    { eof, NewState } ->
      { BlockNo, _, NRecs, _ } = LeafBlock,
      FirstIndexBlockNo =
	case NRecs of
	  0 -> BlockNo;
	  _ -> write_leaf_block (BlockSize, Io, IoTmp, LeafBlock),
	       BlockNo + 1
	end,
      ok = file:close (IoTmp),
      ok = file:rename (tmp_path (Path), tmp_key_path (Path)),
      { ok, IoKeys } =
	file:open (tmp_key_path (Path), [ read, raw, binary, read_ahead ]),
      { ok, NewIoTmp } = file:open (tmp_path (Path), [ write, raw, binary ]),
      write_index (Path, BlockSize, NewState, Io, IoKeys, NewIoTmp,
		   FirstIndexBlockNo, TotalRecs,
		   new_index_block (FirstIndexBlockNo));
    Other ->
      erlang:error ({ bad_return_value, Other })
  end.

write_index (Path, BlockSize, StreamState, Io, IoKeys, IoTmp,
	     Start, TotalRecs, IndexBlock) ->
  case file:read (IoKeys, ?BLOCK_NUMBER_BYTES + ?LARGE_SIZE_BYTES) of
    { ok, << KeyBlockNo:?BLOCK_NUMBER, KeySize:?LARGE_SIZE >> } ->
      { ok, << Key:KeySize/binary >> } = file:read (IoKeys, KeySize),
      NewIndexBlock =
	add_index_record (BlockSize, Io, IoTmp, KeyBlockNo, Key, IndexBlock),
      write_index (Path, BlockSize, StreamState, Io, IoKeys, IoTmp,
		   Start, TotalRecs, NewIndexBlock);
    eof ->
      ok = file:close (IoKeys),
      { IndexBlockNo, _, NKeys, _ } = IndexBlock,
      case IndexBlockNo of
	Start ->
	  % we collected only one index block; this is the root
	  RootSize = write_index_root_block (Io, IndexBlock),
	  TotalBlocks = IndexBlockNo + 1,
	  Header = << ?MAGIC_HEADER/binary,
		      BlockSize:?LARGE_SIZE,
		      TotalBlocks:?LARGE_SIZE,
		      RootSize:?LARGE_SIZE,
		      TotalRecs:?LARGE_SIZE >>,
	  ok = file:pwrite (Io, 0, Header),
	  ok = file:close (Io),
	  ok = file:close (IoTmp),
	  ok = file:delete (tmp_path (Path)),
	  ok = file:delete (tmp_key_path (Path)),
	  { ok, StreamState };
	_ ->
	  % prepare for next index pass
	  NextIndexBlockNo = case NKeys of
	    0 -> IndexBlockNo;
	    _ -> write_index_block (BlockSize, Io, IoTmp, IndexBlock),
		 IndexBlockNo + 1
	  end,
	  ok = file:close (IoTmp),
	  ok = file:rename (tmp_path (Path), tmp_key_path (Path)),
	  { ok, NewIoKeys } =
	    file:open (tmp_key_path (Path), [ read, raw, binary, read_ahead ]),
	  { ok, NewIoTmp } =
	    file:open (tmp_path (Path), [ write, raw, binary ]),
	  write_index (Path, BlockSize, StreamState, Io, NewIoKeys, NewIoTmp,
		       NextIndexBlockNo, TotalRecs,
		       new_index_block (NextIndexBlockNo))
      end
  end.

% ensures that at least two keys fit in each index block
max_key_bytes (BlockSize) ->
  (BlockSize - min_index_header_size ()) bsr 1.

% first key-value pair in block
add_leaf_record (BlockSize, Io, IoTmp, Key, Value, { BlockNo, _, 0, [ ] }) ->
  ValueSize = size (Value),
  Size = 5				% magic + n_values == 1
       + vli32_length (ValueSize)	% value length 0
       + ValueSize,			% value 0
  case Size > BlockSize of
    true ->
      NWritten = write_large_leaf (BlockSize, Io, IoTmp, Key, Value, BlockNo),
      new_leaf_block (BlockNo + NWritten);
    false ->
      KeyData = [ vli32_write (ValueSize), Value ],
      { BlockNo, Size, 1, [ { Key, KeyData } ] }
  end;

% second and subsequent key-value pairs in block
add_leaf_record (BlockSize, Io, IoTmp, Key, Value,
		 LeafBlock = { BlockNo, Size, NRecs, Recs }) ->
  % can we add record w/o overflowing the block?
  [ { LastKey, _ } | _ ] = Recs,
  PrefixSize = prefix_size (Key, LastKey),
  KeySize = size (Key) - PrefixSize,
  ValueSize = size (Value),
  NewSize = Size
	    + vli32_length (NRecs + 1) - vli32_length (NRecs)	% n_values
	    + vli32_length (PrefixSize)
	    + vli32_length (KeySize)
	    + KeySize
	    + vli32_length (ValueSize)
	    + ValueSize,
  case NewSize > BlockSize of
    true ->
      % record would overflow; write current block and try again
      write_leaf_block (BlockSize, Io, IoTmp, LeafBlock),
      add_leaf_record (BlockSize, Io, IoTmp, Key, Value,
		       new_leaf_block (BlockNo + 1));
    false ->
      << _:PrefixSize/binary, KeySuffix:KeySize/binary >> = Key,
      KeyData = [ vli32_write (PrefixSize),
		  vli32_write (KeySize),
		  KeySuffix,
		  vli32_write (ValueSize),
		  Value ],
      { BlockNo, NewSize, NRecs + 1, [ { Key, KeyData } | Recs ] }
  end.

new_leaf_block (BlockNo) ->
  { BlockNo, undefined, 0, [] }.

write_leaf_block (BlockSize, Io, IoTmp, { BlockNo, _Size, NRecs, RevRecs }) ->
  % Recs were accumulated in reverse order
  Recs = [ { FirstKey, _ } | _ ] = lists:reverse (RevRecs),
  write_block_first_key (IoTmp, FirstKey, BlockNo),
  Data = iolist_to_binary ([ ?MAGIC_LEAF,
			     vli32_write (NRecs),
			     [ KeyData || { _, KeyData } <- Recs ] ]),
  ok = file:write (Io, [ Data, pad (BlockSize - size (Data)) ]).

write_large_leaf (BlockSize, Io, IoTmp, Key, Value, BlockNo) ->
  write_block_first_key (IoTmp, Key, BlockNo),
  ValueSize = size (Value),
  Header = << ?MAGIC_LEAF_LARGE_START/binary,
	      (vli32_write (ValueSize))/binary >>,
  Remaining = BlockSize - size (Header),
  case ValueSize > Remaining of
    true ->
      << ValuePrefix:Remaining/binary, ValueSuffix/binary >> = Value,
      ok = file:write (Io, [ Header, ValuePrefix ]),
      continue_large_leaf (BlockSize, Io, ValueSuffix, 1);
    false ->
      ok = file:write (Io, [ Header, Value, pad (Remaining - ValueSize) ]),
      1
  end.

continue_large_leaf (BlockSize, Io, Value, NWritten) ->
  Header = ?MAGIC_LEAF_LARGE_CONTINUE,
  Remaining = BlockSize - size (Header),
  ValueSize = size (Value),
  case ValueSize > Remaining of
    true ->
      << ValuePrefix:Remaining/binary, ValueSuffix/binary >> = Value,
      ok = file:write (Io, [ Header, ValuePrefix ]),
      continue_large_leaf (BlockSize, Io, ValueSuffix, NWritten + 1);
    false ->
      ok = file:write (Io, [ Header, Value, pad (Remaining - ValueSize) ]),
      NWritten + 1
  end.

write_block_first_key (IoTmp, Key, BlockNo) ->
  KeySize = size (Key),
  ok = file:write (IoTmp, << BlockNo:?BLOCK_NUMBER,
			     KeySize:?LARGE_SIZE,
			     Key/binary >>).

add_index_record (_BlockSize, _Io, _IoTmp, KeyBlockNo, Key,
		  _IndexBlock = { IndexBlockNo, _, 0, [] }) ->
  % 9 = 4-byte magic, 1-byte n_keys == 1, 4-byte block no.
  KeyData = << KeyBlockNo:?BLOCK_NUMBER >>,
  { IndexBlockNo, 9, 1, [ { Key, KeyData } ] };

add_index_record (BlockSize, Io, IoTmp, KeyBlockNo, Key,
		  IndexBlock = { IndexBlockNo, Size, NKeys, Keys }) ->
  [ { PrevKey, _ } | _ ] = Keys,
  PrefixSize = prefix_size (Key, PrevKey),
  KeySize = size (Key) - PrefixSize,
  NewSize = Size
	    + vli32_length (NKeys + 1) - vli32_length (NKeys)	% n_keys
	    + vli32_length (PrefixSize)				% prefix len
	    + vli32_length (KeySize)				% key suffix len
	    + KeySize						% key
	    + 4,						% block number
  case NewSize > BlockSize of
    true ->
      % key would overflow
      write_index_block (BlockSize, Io, IoTmp, IndexBlock),
      add_index_record (BlockSize, Io, IoTmp, KeyBlockNo, Key,
			new_index_block (IndexBlockNo + 1));
    false ->
      % key fits
      << _:PrefixSize/binary, KeySuffix/binary >> = Key,
      KeyData = [ vli32_write (PrefixSize),
		  vli32_write (KeySize),
		  KeySuffix,
		  << KeyBlockNo:?BLOCK_NUMBER >> ],
      { IndexBlockNo, NewSize, NKeys + 1, [ { Key, KeyData } | Keys ] }
  end.

% NB: Keys were accumulated in reverse order
write_index_block (BlockSize, Io, IoTmp,
		   { IndexBlockNo, _Size, NKeys, RevKeys }) ->
  Keys = [ { FirstKey, _ } | _ ] = lists:reverse (RevKeys),
  write_block_first_key (IoTmp, FirstKey, IndexBlockNo),
  Data = iolist_to_binary ([ ?MAGIC_INDEX,
			     vli32_write (NKeys),
			     [ KeyData || { _, KeyData } <- Keys ] ]),
  ok = file:write (Io, [ Data, pad (BlockSize - size (Data)) ]).

write_index_root_block (Io, { _IndexBlockNo, _Size, NKeys, RevKeys }) ->
  % Note: we assumed the first key was not stored in the block, so we
  % may have exceeded the block size -- however, since the root block
  % is conveniently at the end of the file, we allow it to vary in size.
  KeyData = case lists:reverse (RevKeys) of
    [ { FirstKey, FirstKeyBlockNo } | Keys ] ->
      [ vli32_write (size (FirstKey)),
	FirstKey,
	FirstKeyBlockNo,
	[ D || { _, D } <- Keys ] ];
    [] ->
      []
  end,
  Data = iolist_to_binary ([ ?MAGIC_INDEX_ROOT,
			     vli32_write (NKeys),
			     KeyData ]),
  ok = file:write (Io, Data),
  size (Data).

new_index_block (BlockNo) ->
  { BlockNo, undefined, 0, [] }.

min_index_header_size () ->
  ?MAGIC_BYTES
  + 5				% vli32 n_keys + 2 (prefix length, key length)
  + 2 * ?BLOCK_NUMBER_BYTES.	% 2 (block number)

tmp_path (Path) when is_list (Path) ->
  Path ++ ".index.tmp".

tmp_key_path (Path) when is_list (Path) ->
  Path ++ ".key.tmp".

%
% searching
%

-record (file, { path,
		 format,
		 io,
		 block_size,
		 total_blocks,
		 root_size,
		 total_entries }).

%% @spec open (Format::#osmos_table_format{}, Path::string()) -> osmos_file()
%% @doc Open a file in the given Format at the given Path.
%% @end

open (Fmt = #osmos_table_format{}, Path) ->
  { ok, Io } = file:open (Path, [ read, raw, binary ]),
  { ok, Data } = file:read (Io, ?MAGIC_BYTES + 4 * ?LARGE_SIZE_BYTES),
  MagicHeader = ?MAGIC_HEADER,
  << MagicHeader:?MAGIC_BYTES/binary,
     BlockSize:?LARGE_SIZE,
     TotalBlocks:?LARGE_SIZE,
     RootSize:?LARGE_SIZE,
     Entries:?LARGE_SIZE >> = Data,
  #file { path = Path,
	  format = Fmt,
	  io = Io,
	  block_size = BlockSize,
	  total_blocks = TotalBlocks,
	  root_size = RootSize,
	  total_entries = Entries }.

%% @spec close (File::osmos_file()) -> ok
%% @doc Close the given File.
%% @end

close (#file { io = Io }) ->
  ok = file:close (Io).

%% @spec path (File::osmos_file()) -> string()
%% @doc Return the path of the given open File.
%% @end

path (#file { path = Path }) ->
  Path.

%% @spec total_entries (File::osmos_file()) -> int()
%% @doc Return the number of key-value records in the given open File.
%% @end

total_entries (#file { total_entries = E }) ->
  E.

%% @spec search (File::osmos_file(), Key::any()) -> { ok, Value } | not_found
%% @doc Search for the given Key in the given open File, and return the
%% corresponding Value, or not_found if there is no record with the
%% given key.
%% @end

search (F = #file { format = Fmt }, Key) ->
  { NKeys, AfterNKeys } = read_root_header (F),
  case NKeys of
    0 ->
      not_found;
    _ ->
      { FirstKeyBin, FirstKeyBlockNo, AfterFirstKeyBlockNo } =
	read_root_first_key (AfterNKeys),
      FirstKey = ?osmos_table_format_key_from_binary (Fmt, FirstKeyBin),
      case ?osmos_table_format_less (Fmt, Key, FirstKey) of
	true ->
	  not_found;
	false ->
	  search_index_data (F, Key, FirstKey, FirstKeyBin, FirstKeyBlockNo,
			     NKeys - 1, AfterFirstKeyBlockNo)
      end
  end.

% precondition: PrevKey <= Key
search_index_data (F, Key, PrevKey, PrevKeyBin, PrevKeyBlockNo, 0, _) ->
  search_block (F, Key, PrevKey, PrevKeyBin, PrevKeyBlockNo);

search_index_data (F = #file { format = Fmt }, Key,
		   PrevKey, PrevKeyBin, PrevKeyBlockNo, NKeys, Data) ->
  { NextKeyBin, NextKeyBlockNo, AfterNextKeyBlockNo } =
    read_index_key (PrevKeyBin, Data),
  NextKey = ?osmos_table_format_key_from_binary (Fmt, NextKeyBin),
  ?ASSERT (false = ?osmos_table_format_less (Fmt, NextKey, PrevKey)),
  case ?osmos_table_format_less (Fmt, Key, NextKey) of
    true ->
      % key is less than next key: descend to prev key block
      search_block (F, Key, PrevKey, PrevKeyBin, PrevKeyBlockNo);
    false ->
      search_index_data (F, Key, NextKey, NextKeyBin, NextKeyBlockNo,
			 NKeys - 1, AfterNextKeyBlockNo)
  end.

% precondition: FirstKey <= Key
search_block (F = #file { format = Fmt },
	      Key, FirstKey, FirstKeyBin, BlockNo) ->
  { Magic, AfterMagic } = read_block_magic (F, BlockNo),
  case Magic of
    ?MAGIC_INDEX ->
      { NKeys, FirstKeyBlockNo, AfterFirstKeyBlockNo } =
	read_index_first_key (AfterMagic),
      search_index_data (F, Key, FirstKey, FirstKeyBin, FirstKeyBlockNo,
			 NKeys - 1, AfterFirstKeyBlockNo);
    ?MAGIC_LEAF ->
      { NValues, FirstValueBin, AfterFirstValue } =
	read_leaf_first_key (AfterMagic),
      case ?osmos_table_format_less (Fmt, FirstKey, Key) of
	false ->
	  { ok, ?osmos_table_format_value_from_binary (Fmt, FirstValueBin) };
	true ->
	  search_leaf_data (F, Key, FirstKeyBin, NValues - 1, AfterFirstValue)
      end;

    ?MAGIC_LEAF_LARGE_START ->
      case ?osmos_table_format_less (Fmt, FirstKey, Key) of
	true ->
	  % only one record per large leaf block, so if this isn't it...
	  not_found;
	false ->
	  ValueBin = read_large_leaf_data (F, BlockNo, AfterMagic),
	  { ok, ?osmos_table_format_value_from_binary (Fmt, ValueBin) }
      end;

    _ ->
      erlang:error ({ bad_magic, Magic })
  end.

% precondition: PrevKey < Key 
search_leaf_data (_F, _Key, _PrevKeyBin, 0, _Data) ->
  not_found;
search_leaf_data (F = #file { format = Fmt }, Key, PrevKeyBin, NValues, Data) ->
  { NextKeyBin, ValueBin, AfterValue } = read_leaf_key (PrevKeyBin, Data),
  NextKey = ?osmos_table_format_key_from_binary (Fmt, NextKeyBin),
  case ?osmos_table_format_less (Fmt, Key, NextKey) of
    true ->
      not_found;
    false ->
      case ?osmos_table_format_less (Fmt, NextKey, Key) of
	true  -> search_leaf_data (F, Key, NextKeyBin, NValues - 1, AfterValue);
	false -> { ok, ?osmos_table_format_value_from_binary (Fmt, ValueBin) }
      end
  end.

%
% iterators
% 

-record (iter, { file,		% osmos_file()
		 stack,		% stack of parent index blocks:
				%   { NValues, Data, PrevKeyBin }
		 n_values,	% number of remaining records in leaf block
		 data,		% remaining data in leaf block
		 key_bin,	% current binary key
		 key,		% current native key
		 value }).	% current native value

%% @spec iter_file (Iter::iter()) -> osmos_file()
%% @doc Return the open file handle for the given iterator.
%% @end

iter_file (#iter { file = F }) ->
  F.

%% @spec iter_key (Iter::iter()) -> Key
%% @doc Return the key from the current record of the given iterator.
%% @end

iter_key (#iter { key = K }) ->
  K.

%% @spec iter_value (Iter::iter()) -> Value
%% @doc Return the value from the current record of the given iterator.
%% @end

iter_value (#iter { value = V }) ->
  V.

%% @spec iter_next (Iter::iter()) -> iter()
%% @doc Advance the given iterator to the next record.
%% @end

iter_next (Iter = #iter { file = F, stack = Stack, n_values = 0 }) ->
  case iter_index_next (F, Stack) of
    ?OSMOS_END ->
      ?OSMOS_END;
    { NewStack, FirstKeyBin, BlockNo } ->
      % Note: here we rely on all leaves being at the same depth
      { ?MAGIC_LEAF, AfterMagic } = read_block_magic (F, BlockNo),
      { NewNValues, FirstValueBin, AfterFirstValue } =
	read_leaf_first_key (AfterMagic),
      Fmt = F#file.format,
      FirstKey = ?osmos_table_format_key_from_binary (Fmt, FirstKeyBin),
      FirstValue = ?osmos_table_format_value_from_binary (Fmt, FirstValueBin),
      Iter#iter { stack = NewStack,
		  n_values = NewNValues - 1,
		  data = AfterFirstValue,
		  key_bin = FirstKeyBin,
		  key = FirstKey,
		  value = FirstValue }
  end;
iter_next (Iter = #iter { file = F,
			  n_values = NValues,
			  data = Data,
			  key_bin = PrevKeyBin })
      when NValues > 0 ->
  { NextKeyBin, NextValueBin, AfterValue } = read_leaf_key (PrevKeyBin, Data),
  Fmt = F#file.format,
  NextKey = ?osmos_table_format_key_from_binary (Fmt, NextKeyBin),
  NextValue = ?osmos_table_format_value_from_binary (Fmt, NextValueBin),
  Iter#iter { n_values = NValues - 1,
	      data = AfterValue,
	      key_bin = NextKeyBin,
	      key = NextKey,
	      value = NextValue }.

iter_index_next (_F, [ { 0, _Data, _PrevKeyBin } ]) ->
  ?OSMOS_END;
iter_index_next (F, [ { 0, _Data, _PrevKeyBin } | OldStack ]) ->
  case iter_index_next (F, OldStack) of
    ?OSMOS_END ->
      ?OSMOS_END;
    { NewParentStack, NewPrevKeyBin, BlockNo } ->
      % Note: here we rely on all leaves being at the same depth
      { ?MAGIC_INDEX, AfterMagic } = read_block_magic (F, BlockNo),
      { NKeys, FirstKeyBlockNo, AfterFirstKeyBlockNo } =
	read_index_first_key (AfterMagic),
      NewStack = [ { NKeys - 1, AfterFirstKeyBlockNo, NewPrevKeyBin }
		   | NewParentStack ],
      { NewStack, NewPrevKeyBin, FirstKeyBlockNo }
  end;
iter_index_next (_F, [ { NValues, Data, PrevKeyBin } | Stack ]) ->
  { NextKeyBin, NextKeyBlockNo, AfterNextKeyBlockNo } =
    read_index_key (PrevKeyBin, Data),
  NewStack = [ { NValues - 1, AfterNextKeyBlockNo, NextKeyBin } | Stack ],
  { NewStack, NextKeyBin, NextKeyBlockNo }.

%% @spec iter_begin (File::osmos_file()) -> iter()
%% @doc Return an iterator pointing to the first (least) key in the given
%% open file.
%% @end

iter_begin (F) ->
  { NKeys, AfterNKeys } = read_root_header (F),
  case NKeys of
    0 ->
      ?OSMOS_END;
    _ ->
      { FirstKeyBin, FirstKeyBlockNo, AfterFirstKeyBlockNo } =
	read_root_first_key (AfterNKeys),
      FirstKey = ?osmos_table_format_key_from_binary (F#file.format,
						      FirstKeyBin),
      iter_descend_left (F, FirstKey, FirstKeyBin, FirstKeyBlockNo,
			 [ { NKeys - 1, AfterFirstKeyBlockNo, FirstKeyBin } ])
  end.

%% @spec iter_end (File) -> iter()
%% @doc Return an iterator pointing to the end of the file (immediately
%% after the greatest key in the file). This iterator cannot be
%% dereferenced with iter_value.
%% @end

iter_end (_) ->
  ?OSMOS_END.

%% @spec iter_lower_bound (File::osmos_file(), Less) -> iter()
%%       Less = (Key::any()) -> bool()
%%            | any()
%% @doc Return an iterator pointing to the least element in the file
%% which is in the (possibly open) interval bounded below by the given
%% function Less, or iter_end() if every key is below the desired interval.
%%
%% Less should return true if the given Key is less than any key in the
%% desired interval, or false otherwise. It must also be consistent with
%% the file format's ordering, so the following implications hold:
%% <ol>
%%   <li>(Less (K2) and KeyLess (K1, K2)) implies Less (K1)</li>
%%   <li>(not Less (K1) and KeyLess (K1, K2)) implies not Less (K2)</li>
%% </ol>
%% where KeyLess is the format's key_less function.
%%
%% If Less is not a function, it is taken to be the least key in the
%% desired interval, i.e., the iterator will point to the first key in the
%% file not less than the given key, or iter_end() if every key is less
%% than the given key. This is equivalent to passing a function:
%% <pre>
%% fun (K) ->
%%   KeyLess (K, Key)
%% end
%% </pre>
%% @end

iter_lower_bound (F = #file { format = Fmt }, Key) when not is_function (Key) ->
  Less = fun (K) ->
	   ?osmos_table_format_less (Fmt, K, Key)
	 end,
  iter_lower_bound (F, Less);

iter_lower_bound (F = #file { format = Fmt }, Less) when is_function (Less) ->
  { NKeys, AfterNKeys } = read_root_header (F),
  case NKeys of
    0 ->
      ?OSMOS_END;
    _ ->
      { FirstKeyBin, FirstKeyBlockNo, AfterFirstKeyBlockNo } =
	read_root_first_key (AfterNKeys),
      FirstKey = ?osmos_table_format_key_from_binary (Fmt, FirstKeyBin),
      case Less (FirstKey) of
	false ->
	  % first key not less than key: descend immediately
	  Stack = [ { NKeys - 1, AfterFirstKeyBlockNo, FirstKeyBin } ],
	  iter_descend_left (F, FirstKey, FirstKeyBin, FirstKeyBlockNo, Stack);
	true ->
	  % first key less than key: continue scanning root
	  iter_lb_scan_index (F, Less, FirstKey, FirstKeyBin, FirstKeyBlockNo,
			      NKeys - 1, AfterFirstKeyBlockNo, [])
      end
  end.

% precondition: Less (PrevKey)
iter_lb_scan_index (F, Less, PrevKey, PrevKeyBin, PrevKeyBlockNo,
		    0, Data, Stack) ->
  iter_lb_descend (F, Less, PrevKey, PrevKeyBin, PrevKeyBlockNo,
		   [ { 0, Data, PrevKeyBin } | Stack ]);

iter_lb_scan_index (F, Less, PrevKey, PrevKeyBin, PrevKeyBlockNo,
		    NKeys, Data, Stack) ->
  { NextKeyBin, NextKeyBlockNo, AfterNextKeyBlockNo } =
    read_index_key (PrevKeyBin, Data),
  NextKey = ?osmos_table_format_key_from_binary (F#file.format, NextKeyBin),
  case Less (NextKey) of
    false ->
      % next key not less than key: lower bound is either next key,
      % or some key between prev key and next key
      case iter_lb_descend (F, Less, PrevKey, PrevKeyBin, PrevKeyBlockNo,
			    [ { NKeys, Data, PrevKeyBin } | Stack ])
      of
	?OSMOS_END ->
	  NewStack = [ { NKeys - 1, AfterNextKeyBlockNo, NextKeyBin } | Stack ],
	  iter_descend_left (F, NextKey, NextKeyBin, NextKeyBlockNo, NewStack);
        Iter ->
	  Iter
      end;
    true ->
      % next key less than key: continue scanning block
      iter_lb_scan_index (F, Less, NextKey, NextKeyBin, NextKeyBlockNo,
			  NKeys - 1, AfterNextKeyBlockNo, Stack)
  end.

% Descend to a new index or leaf block, and invoke the appropriate
% scanning function to find the first key in the desired interval,
% or iter_end() if none.
% precondition: Less (FirstKey)

iter_lb_descend (F, Less, FirstKey, FirstKeyBin, BlockNo, Stack) ->
  { Magic, AfterMagic } = read_block_magic (F, BlockNo),
  case Magic of
    ?MAGIC_INDEX ->
      { NKeys, FirstKeyBlockNo, AfterFirstKeyBlockNo } =
	read_index_first_key (AfterMagic),
      iter_lb_scan_index (F, Less, FirstKey, FirstKeyBin, FirstKeyBlockNo,
			  NKeys - 1, AfterFirstKeyBlockNo, Stack);
    ?MAGIC_LEAF ->
      { NValues, _FirstValue, AfterFirstValue } =
	read_leaf_first_key (AfterMagic),
      iter_lb_scan_leaf (F, Less, FirstKeyBin,
			 NValues - 1, AfterFirstValue, Stack);
    ?MAGIC_LEAF_LARGE_START ->
      % only one key in a large leaf
      ?OSMOS_END;
    _ ->
      erlang:error ({ bad_magic, Magic })
  end.

% Scan through a leaf block; return the first key in the
% desired interval, or iter_end() if none.
% precondition: Less (PrevKey)

iter_lb_scan_leaf (_F, _Less, _PrevKeyBin, 0, _Data, _Stack) ->
  ?OSMOS_END;
iter_lb_scan_leaf (F, Less, PrevKeyBin, NValues, Data, Stack) ->
  Fmt = F#file.format,
  { NextKeyBin, ValueBin, AfterValue } = read_leaf_key (PrevKeyBin, Data),
  NextKey = ?osmos_table_format_key_from_binary (Fmt, NextKeyBin),
  case Less (NextKey) of
    false ->
      % next key not below interval: this is the lower bound
      Value = ?osmos_table_format_value_from_binary (Fmt, ValueBin),
      #iter { file = F,
	      stack = Stack,
	      n_values = NValues - 1,
	      data = AfterValue,
	      key_bin = NextKeyBin,
	      key = NextKey,
	      value = Value };
    true ->
      % next key below interval: continue scanning block
      iter_lb_scan_leaf (F, Less, NextKeyBin, NValues - 1, AfterValue, Stack)
  end.

%% @spec iter_less (iter(), iter()) -> bool()
%% @doc Return true if the first iterator is positioned before the
%% second iterator.
%% @end

iter_less (?OSMOS_END, ?OSMOS_END) ->
  false;
iter_less (?OSMOS_END, #iter{}) ->
  false;
iter_less (#iter{}, ?OSMOS_END) ->
  true;
iter_less (#iter { file = FA, key = A }, #iter { key = B }) ->
  ?osmos_table_format_less (FA#file.format, A, B).

% Return an iterator pointing to the first key in the subtree rooted
% at BlockNo.

iter_descend_left (F, FirstKey, FirstKeyBin, BlockNo, Stack) ->
  { Magic, AfterMagic } = read_block_magic (F, BlockNo),
  case Magic of
    ?MAGIC_INDEX ->
      { NKeys, FirstKeyBlockNo, AfterFirstKeyBlockNo } =
	read_index_first_key (AfterMagic),
      NewStack = [ { NKeys - 1, AfterFirstKeyBlockNo, FirstKeyBin } | Stack ],
      iter_descend_left (F, FirstKey, FirstKeyBin, FirstKeyBlockNo, NewStack);

    ?MAGIC_LEAF ->
      { NValues, FirstValueBin, AfterFirstValue } =
	read_leaf_first_key (AfterMagic),
      FirstValue = ?osmos_table_format_value_from_binary (F#file.format,
							  FirstValueBin),
      #iter { file = F,
	      stack = Stack,
	      n_values = NValues - 1,
	      data = AfterFirstValue,
	      key_bin = FirstKeyBin,
	      key = FirstKey,
	      value = FirstValue };

    ?MAGIC_LEAF_LARGE_START ->
      FirstValueBin = read_large_leaf_data (F, BlockNo, AfterMagic),
      FirstValue = ?osmos_table_format_value_from_binary (F#file.format,
							  FirstValueBin),
      #iter { file = F,
	      stack = Stack,
	      n_values = 0,
	      data = <<>>,
	      key_bin = FirstKeyBin,
	      key = FirstKey,
	      value = FirstValue };

    _ ->
      erlang:error ({ bad_magic, Magic })
  end.

%% @spec iter_stream_next (Iter) -> { ok, Key, Value, NewIter }
%%                                | { eof, FinalIter }
%% @doc Convert an iterator to a stream. Return a function suitable for
%% use as the StreamNext argument to build/4.
%% @end

iter_stream_next (Iter) ->
  case Iter of
    ?OSMOS_END                   -> { eof, ?OSMOS_END };
    #iter { key = K, value = V } -> { ok, K, V, iter_next (Iter) }
  end.

%% @spec merge (FileA::osmos_file(),
%%              FileB::osmos_file(),
%%              Path::string(),
%%              Format::#osmos_table_format{},
%%              Merge) -> ok
%%       Merge = (Key::any(), ValueA::any(), ValueB::any()) -> any()
%% @doc Merge two open files into a new file at Path, using the given
%% Merge function to merge entries with the same key.
%% @end

merge (FileA, FileB, Path, Format, Merge) ->
  Next = fun (State) ->
	   merge_next (Merge, State)
	 end,
  State = { iter_begin (FileA), iter_begin (FileB) },
  { ok, _ } = build (Path, Format, Next, State),
  ok.

merge_next (_Merge, State = { ?OSMOS_END, ?OSMOS_END }) ->
  { eof, State };
merge_next (_Merge, { IA, ?OSMOS_END }) ->
  { ok, iter_key (IA), iter_value (IA), { iter_next (IA), ?OSMOS_END } };
merge_next (_Merge, { ?OSMOS_END, IB }) ->
  { ok, iter_key (IB), iter_value (IB), { ?OSMOS_END, iter_next (IB) } };
merge_next (Merge, { IA, IB }) ->
  case iter_less (IA, IB) of
    true ->
      NewState = { iter_next (IA), IB },
      { ok, iter_key (IA), iter_value (IA), NewState };
    false ->
      case iter_less (IB, IA) of
	true ->
	  NewState = { IA, iter_next (IB) },
	  { ok, iter_key (IB), iter_value (IB), NewState };
	false ->
	  K = iter_key (IA),
	  V = Merge (K, iter_value (IA), iter_value (IB)),
	  NewState = { iter_next (IA), iter_next (IB) },
	  { ok, K, V, NewState }
      end
  end.

%% @spec merge_delete (FileA::osmos_file(),
%%                     FileB::osmos_file(),
%%                     Path::string(),
%%                     Format::#osmos_table_format{},
%%                     Merge,
%%                     Delete) -> ok
%%       Merge = (Key::any(), ValueA::any(), ValueB::any()) -> any()
%%       Delete = (Key::any(), Value::any()) -> bool()
%% @doc Merge two open files into a new file at Path, using the given
%% Merge function to merge entries with the same key. Entries (after
%% merging, if applicable) for which Delete returns true will be omitted
%% from the output file.
%% @end

merge_delete (FileA, FileB, Path, Format, Merge, Delete) ->
  Next = fun (State) ->
	   merge_delete_next (Merge, Delete, State)
	 end,
  State = { iter_begin (FileA), iter_begin (FileB) },
  { ok, _ } = build (Path, Format, Next, State),
  ok.

merge_delete_next (Merge, Delete, State) ->
  case merge_next (Merge, State) of
    Eof = { eof, _ } ->
      Eof;
    Result = { ok, K, V, NewState } ->
      case Delete (K, V) of
	true ->
	  merge_delete_next (Merge, Delete, NewState);
	false ->
	  Result
      end
  end.

%
% format reading
%

read_root_header (#file { io = Io,
			  block_size = BlockSize,
			  total_blocks = TotalBlocks,
			  root_size = RootSize }) ->
  Pos = ?FILE_HEADER_SIZE + (TotalBlocks - 1) * BlockSize,
  MagicIndexRoot = ?MAGIC_INDEX_ROOT,
  { ok, << MagicIndexRoot:?MAGIC_BYTES/binary, Data/binary >> } =
    file:pread (Io, Pos, RootSize),
  { NKeys, AfterNKeys } = vli32_read (Data),
  { NKeys, AfterNKeys }.

read_root_first_key (AfterNKeys) ->
  { FirstKeySize, AfterFirstKeySize } = vli32_read (AfterNKeys),
  << FirstKey:FirstKeySize/binary,
     FirstKeyBlockNo:?BLOCK_NUMBER,
     AfterFirstKeyBlockNo/binary >> = AfterFirstKeySize,
  { FirstKey, FirstKeyBlockNo, AfterFirstKeyBlockNo }.

read_block_magic (#file { io = Io, block_size = BlockSize }, BlockNo) ->
  Pos = ?FILE_HEADER_SIZE + BlockNo * BlockSize,
  { ok, Data } = file:pread (Io, Pos, BlockSize),
  << Magic:?MAGIC_BYTES/binary, AfterMagic/binary >> = Data,
  { Magic, AfterMagic }.

read_index_first_key (AfterMagic) ->
  { NKeys, AfterNKeys } = vli32_read (AfterMagic),
  << FirstKeyBlockNo:?BLOCK_NUMBER,
     AfterFirstKeyBlockNo/binary >> = AfterNKeys,
  { NKeys, FirstKeyBlockNo, AfterFirstKeyBlockNo }.

read_index_key (PrevKey, Data) ->
  { PrefixSize, AfterPrefixSize } = vli32_read (Data),
  { NextKeySize, AfterNextKeySize } = vli32_read (AfterPrefixSize),
  << NextKeySuffix:NextKeySize/binary,
     NextKeyBlockNo:?BLOCK_NUMBER,
     AfterNextKeyBlockNo/binary >> = AfterNextKeySize,
  << Prefix:PrefixSize/binary, _/binary >> = PrevKey,
  NextKey = << Prefix/binary, NextKeySuffix/binary >>,
  { NextKey, NextKeyBlockNo, AfterNextKeyBlockNo }.

read_leaf_first_key (AfterMagic) ->
  { NValues, AfterNValues } = vli32_read (AfterMagic),
  { FirstValueSize, AfterFirstValueSize } = vli32_read (AfterNValues),
  << FirstValue:FirstValueSize/binary,
     AfterFirstValue/binary >> = AfterFirstValueSize,
  { NValues, FirstValue, AfterFirstValue }.

read_leaf_key (PrevKey, Data) ->
  { PrefixSize, AfterPrefixSize } = vli32_read (Data),
  { NextKeySize, AfterNextKeySize } = vli32_read (AfterPrefixSize),
  << NextKeySuffix:NextKeySize/binary, 
     AfterNextKeySuffix/binary >> = AfterNextKeySize,
  { ValueSize, AfterValueSize } = vli32_read (AfterNextKeySuffix),
  << Value:ValueSize/binary, AfterValue/binary >> = AfterValueSize,
  << Prefix:PrefixSize/binary, _/binary >> = PrevKey,
  NextKey = << Prefix/binary, NextKeySuffix/binary >>,
  { NextKey, Value, AfterValue }.

read_large_leaf_data (F, BlockNo, AfterMagic) ->
  { ValueSize, Data } = vli32_read (AfterMagic),
  DataSize = size (Data),
  case ValueSize > DataSize of
    true ->
      % overflows into next block
      read_large_leaf_data (F, BlockNo + 1, ValueSize - DataSize, [Data]);
    false ->
      case ValueSize < DataSize of
	true ->
	  << Value:ValueSize/binary, _/binary >> = Data,
	  Value;
	false ->
	  Data
      end
  end.

read_large_leaf_data (F, BlockNo, Remaining, Acc) ->
  { Magic, Data } = read_block_magic (F, BlockNo),
  case Magic of
    ?MAGIC_LEAF_LARGE_CONTINUE -> ok;
    _ -> erlang:error ({ bad_magic, Magic })
  end,
  DataSize = size (Data),
  case Remaining > DataSize of
    true ->
      % overflows into next block
      read_large_leaf_data (F, BlockNo + 1, Remaining - DataSize, [Data | Acc]);
    false ->
      case Remaining < DataSize of
	true ->
	  << Chunk:Remaining/binary, _/binary >> = Data,
	  lists:reverse ([ Chunk | Acc ]);
	false ->
	  lists:reverse ([ Data | Acc ])
      end
  end.

%
% misc
%

pad (0) ->
  [];
pad (Size) when is_integer (Size), Size > 0 ->
  list_to_binary (lists:duplicate (Size, 0)).

prefix_size (B1, B2) when is_binary (B1), is_binary (B2) ->
  prefix_size (B1, B2, 0).

prefix_size (<<>>, _, Size) ->
  Size;
prefix_size (_, <<>>, Size) ->
  Size;
prefix_size (<<A:8, As/binary>>, <<B:8, Bs/binary>>, Size) ->
  case A =:= B of
    true -> prefix_size (As, Bs, Size + 1);
    false -> Size
  end.

%
% tests
%

-ifdef (EUNIT).

empty_test () ->
  Next = fun (S) -> { eof, S } end,
  Path = test_tmp_path (?LINE),
  Fmt = osmos_table_format:new (binary, binary_replace, 256),
  { ok, _ } = build (Path, Fmt, Next, []),
  F = open (Fmt, Path),
  not_found = search (F, <<"hi">>),
  ok = close (F),
  ok = file:delete (Path).

basic_test_ () ->
  { timeout, 60, fun basic_test_run/0 }.

random_length_test_ () ->
  [ { timeout, 600, random_length_test_new (10000,  137, 256)   },
    { timeout, 600, random_length_test_new (10000,  137, 1024)  },
    { timeout, 600, random_length_test_new (10000,  137, 4096)  },
    { timeout, 600, random_length_test_new (10000,  137, 32768) },
    { timeout, 600, random_length_test_new (100000, 317, 4096)  } ].

tuple_test_ () ->
  [ { timeout, 300, tuple_test_new (5, 100000, 137, 4096) } ].

basic_test_run () ->
  Next = fun
	   (S = { Max, Max }) ->
	     { eof, S };
	   ({ N, Max }) ->
	     B = <<N:32/big-unsigned-integer>>,
	     { ok, B, B, { N + 1, Max } }
	 end,
  State = { 0, 65536 },
  Path = test_tmp_path (?LINE),
  Fmt = osmos_table_format:new (binary, binary_replace, 256),
  { ok, _ } = build (Path, Fmt, Next, State),
  F = open (Fmt, Path),
  FortyTwo = <<42:32/big-unsigned-integer>>,
  { ok, FortyTwo } = search (F, FortyTwo),

  FourteenNinetyTwo = <<1492:32/big-unsigned-integer>>,
  PartialIter = iter_lower_bound (F, FourteenNinetyTwo),
  FourteenNinetyTwo = iter_key (PartialIter),
  Path2 = test_tmp_path (?LINE),
  { ok, _ } = build (Path2, Fmt, fun iter_stream_next/1, PartialIter),

  ok = close (F),
  ok = file:delete (Path),

  F2 = open (Fmt, Path2),
  FortyTwo = <<42:32/big-unsigned-integer>>,
  not_found = search (F2, FortyTwo),
  { ok, FourteenNinetyTwo } = search (F2, FourteenNinetyTwo),
  SixteenTwenty = <<1620:32/big-unsigned-integer>>,
  { ok, SixteenTwenty } = search (F2, SixteenTwenty),
  ok = close (F2),
  ok = file:delete (Path2).

random_length_test_new (NRecords, SampleFreq, BlockSize) ->
  fun () ->
    { Next, State } =
      sample_stream_new (
	SampleFreq,
	{ fun
	    (S = { _Prev, Max, Max }) ->
	      { eof, S };
	    (_S = { Prev, Max, N }) ->
	      Value = random_int (),
	      KeyOut = Prev ++ random_string (random:uniform (32), []),
	      { ok,
		list_to_binary (KeyOut),
		<< Value:64 >>,
		{ incr_string (Prev), Max, N + 1 } }
	  end,
	  { random_string (), NRecords, 0 } }
      ),
    Path = test_tmp_path (?LINE),
    Fmt = osmos_table_format:new (binary, binary_replace, BlockSize),
    { ok, FinalState } = build (Path, Fmt, Next, State),
    Samples = sample_stream_samples (FinalState),
    F = open (Fmt, Path),
    assert_searches (?LINE, F, Samples, 0),

    FullIter = iter_begin (F),
    CopyPath = test_tmp_path (?LINE),
    { ok, _ } = build (CopyPath, Fmt, fun iter_stream_next/1, FullIter),

    Copy = open (Fmt, CopyPath),
    assert_iterators_equal (?LINE, FullIter, iter_begin (Copy)),

    ok = close (F),
    ok = file:delete (Path),

    assert_searches (?LINE, Copy, Samples, 0),
    assert_lower_bounds (?LINE, Copy, Samples, 0),

    PartSamples = [ { PartFirst, _ } | _ ] =
      lists:nthtail (length (Samples) div 2 + 1, Samples),
    PartIter = iter_lower_bound (Copy, PartFirst),
    PartFirst = iter_key (PartIter),
    PartPath = test_tmp_path (?LINE),
    { ok, _ } = build (PartPath, Fmt, fun iter_stream_next/1, PartIter),

    Part = open (Fmt, PartPath),
    assert_iterators_equal (?LINE, PartIter, iter_begin (Part)),

    ok = close (Copy),
    ok = file:delete (CopyPath),

    assert_searches (?LINE, Part, PartSamples, 0),
    assert_lower_bounds (?LINE, Part, PartSamples, 0),

    ok = close (Part),
    ok = file:delete (PartPath)
  end.

tuple_test_new (Arity, NRecords, SampleFreq, BlockSize) ->
  fun () ->
    { Next, State } =
      sample_stream_new (SampleFreq, tuple_stream_new (Arity, NRecords)),
    Path = test_tmp_path (?LINE),
    Fmt = osmos_table_format:new (term, term_replace, BlockSize),
    { ok, FinalState } = build (Path, Fmt, Next, State),
    Samples = sample_stream_samples (FinalState),
    F = open (Fmt, Path),
    assert_searches (?LINE, F, Samples, 0),

    FullIter = iter_begin (F),
    CopyPath = test_tmp_path (?LINE),
    { ok, _ } = build (CopyPath, Fmt, fun iter_stream_next/1, FullIter),

    Copy = open (Fmt, CopyPath),
    assert_iterators_equal (?LINE, FullIter, iter_begin (Copy)),

    ok = close (F),
    ok = file:delete (Path),

    assert_searches (?LINE, Copy, Samples, 0),
    assert_lower_bounds (?LINE, Copy, Samples, 0),

    PartSamples = [ { PartFirst, _ } | _ ] =
      lists:nthtail (length (Samples) div 2 + 1, Samples),
    PartIter = iter_lower_bound (Copy, PartFirst),
    PartFirst = iter_key (PartIter),
    PartPath = test_tmp_path (?LINE),
    { ok, _ } = build (PartPath, Fmt, fun iter_stream_next/1, PartIter),

    Part = open (Fmt, PartPath),
    assert_iterators_equal (?LINE, PartIter, iter_begin (Part), 0),

    ok = close (Copy),
    ok = file:delete (CopyPath),

    assert_searches (?LINE, Part, PartSamples, 0),
    assert_lower_bounds (?LINE, Part, PartSamples, 0),

    ok = close (Part),
    ok = file:delete (PartPath)
  end.

assert_searches (_Line, _F, [], _I) ->
  ok;
assert_searches (Line, F, [ { K, V } | Rest ], I) ->
  case ?MODULE:search (F, K) of
    { ok, V } ->
      ok;
    X ->
      throw ({ search_failed, Line, I, K, V, X })
  end,
  assert_searches (Line, F, Rest, I + 1).

assert_lower_bounds (_Line, _F, [], _I) ->
  ok;
assert_lower_bounds (Line, F, [ { K, V } | Rest ], I) ->
  case ?MODULE:iter_lower_bound (F, K) of
    ?OSMOS_END ->
      throw ({ lower_bound_eof, Line, I, K, V });
    It ->
      IK = ?MODULE:iter_key (It),
      case IK =:= K of
	true -> ok;
	false ->
	  throw ({ lower_bound_bad_key, Line, I, K, V, IK })
      end,
      IV = ?MODULE:iter_value (It),
      case IV =:= V of
	true -> ok;
	false ->
	  throw ({ lower_bound_bad_val, Line, I, K, V, IV })
      end,
      assert_lower_bounds (Line, F, Rest, I + 1)
  end.

assert_iterators_equal (Line, A, B) ->
  assert_iterators_equal (Line, A, B, 0).
assert_iterators_equal (Line, A, B, N) ->
  case { A, B } of
    { ?OSMOS_END, ?OSMOS_END } ->
      ok;
    { ?OSMOS_END, _ } ->
      throw ({ iters_unequal, Line, N, eof, iter_key (B) });
    { _, ?OSMOS_END } ->
      throw ({ iters_unequal, Line, N, iter_key (A), eof });
    { _, _ } ->
      case ?MODULE:iter_key (A) =:= ?MODULE:iter_key (B) of
	false ->
	  throw ({ iters_unequal, Line, N, iter_key (A), iter_key (B) });
	true ->
	  assert_iterators_equal (Line, iter_next (A), iter_next (B), N + 1)
      end
  end.

tuple_stream_new (Arity, N) ->
  { fun tuple_stream_next/1, { Arity, N, random_tuple (Arity) } }.

tuple_stream_next (S = { _, 0, _ }) ->
  { eof, S };
tuple_stream_next ({ Arity, N, PrevKeys }) ->
  NewKeys = incr_tuple (Arity, PrevKeys),
  case PrevKeys < NewKeys of
    true -> ok;
    false -> throw ({ bad_order, PrevKeys, NewKeys })
  end,
  true = PrevKeys < NewKeys,
  Key = list_to_tuple (NewKeys),
  Value = random:uniform (16#100000000) - 1,
  { ok, Key, Value, { Arity, N - 1, NewKeys } }.

random_tuple (Arity) ->
  lists:map (fun (_) ->
	       case random:uniform (3) of
		 1 -> random_int ();
		 2 -> random_string ();
		 3 -> random_binary ()
	       end
	     end,
	     lists:seq (1, Arity)).

incr_tuple (Arity, Keys) ->
  incr_tuple (random:uniform (Arity), Keys, []).

incr_tuple (0, [], Acc) ->
  lists:reverse (Acc);

incr_tuple (0, [ PrevKey | PrevKeys ], Acc) ->
  NextKey = case PrevKey of
    I when is_integer (I) -> random_int ();
    L when is_list (L) -> random_string ();
    B when is_binary (B) -> random_binary ()
  end,
  incr_tuple (0, PrevKeys, [ NextKey | Acc ]);

incr_tuple (1, [ PrevKey | PrevKeys ], Acc) ->
  NextKey = case PrevKey of
    I when is_integer (I) -> incr_int (I);
    L when is_list (L) -> incr_string (L);
    B when is_binary (B) -> incr_binary (B)
  end,
  incr_tuple (0, PrevKeys, [ NextKey | Acc ]);

incr_tuple (Skip, [ PrevKey | PrevKeys ], Acc) ->
  incr_tuple (Skip - 1, PrevKeys, [ PrevKey | Acc ]).

random_int () ->
  random:uniform (16#100000000) - 1.
incr_int (I) ->
  I + random:uniform (256).

random_string () ->
  random_string (32, []).

random_string (0, Acc) ->
  Acc;
random_string (N, Acc) ->
  random_string (N - 1, [ random_char () | Acc ]).

incr_string (S) ->
  Increment = random_string (random:uniform (8), []),
  incr_string (Increment, lists:reverse (S), [], 1).

-define (TEST_CHAR_MIN, $!).
-define (TEST_CHAR_MAX, $~).
-define (TEST_CHAR_RANGE, (?TEST_CHAR_MAX - ?TEST_CHAR_MIN)).

incr_string ([], [], Acc, 0) ->
  % XXX: can't apply carry here, since strings are compared left to right
  Acc;

incr_string ([], [ S | Ss ], Acc, Carry) ->
  { O, NewCarry } = char_add (S, ?TEST_CHAR_MIN, Carry),
  incr_string ([], Ss, [ O | Acc ], NewCarry);

incr_string ([ I | Is ], [ S | Ss ], Acc, Carry) ->
  { O, NewCarry } = char_add (S, I, Carry),
  incr_string (Is, Ss, [ O | Acc ], NewCarry).

char_add (A, B, Carry) ->
  Sum = Carry + (A - ?TEST_CHAR_MIN) + (B - ?TEST_CHAR_MIN),
  case Sum > ?TEST_CHAR_RANGE of
    true -> { ?TEST_CHAR_MIN + Sum rem ?TEST_CHAR_RANGE,
	      Sum div ?TEST_CHAR_RANGE };
    false -> { ?TEST_CHAR_MIN + Sum, 0 }
  end.

random_binary () ->
  list_to_binary (random_string ()).

incr_binary (B) ->
  list_to_binary (incr_string (binary_to_list (B))).

random_char () ->
  $! + random:uniform ($~ - $!) - 1.

sample_stream_new (Freq, { InputNext, InputState }) ->
  { fun sample_stream_next/1, { Freq, InputNext, InputState, 0, [] } }.

sample_stream_samples ({ _, _, _, _, Acc }) ->
  lists:reverse (Acc).

sample_stream_next ({ Freq, InputNext, InputState, Count, Acc }) ->
  case InputNext (InputState) of
    { eof, NewInputState } ->
      { eof, { Freq, InputNext, NewInputState, Count, Acc } };
    { ok, K, V, NewInputState } ->
      NewState = case Count + 1 == Freq of
	true -> { Freq, InputNext, NewInputState, 0, [ { K, V } | Acc ] };
        false -> { Freq, InputNext, NewInputState, Count + 1, Acc }
      end,
      { ok, K, V, NewState }
  end.

test_tmp_path (Line) ->
  io_lib:format ("tmp-test-~s-~s-~b", [ ?MODULE, os:getpid (), Line ]).

-endif.
