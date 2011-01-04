-ifndef (OSMOS_HRL).
-define (OSMOS_HRL, true).

-record (osmos_format, { to_binary, from_binary }).

-define (osmos_format_to_binary (F, X),
	 (((F)#osmos_format.to_binary) (X))).

-define (osmos_format_from_binary (F, X),
	 (((F)#osmos_format.from_binary) (X))).

-record (osmos_table_format,
	 { block_size,		% int
	   key_format,		% #osmos_format{}
	   key_less,		% (KeyA, KeyB) -> bool()
	   value_format,	% #osmos_format{}
	   merge,		% (Key, EarlierValue, LaterValue) -> Value
	   short_circuit,	% (Key, Value) -> bool()
	   delete }).		% (Key, Value) -> bool()

-define (osmos_table_format_key_to_binary (F, K),
	 (?osmos_format_to_binary ((F)#osmos_table_format.key_format, (K)))).

-define (osmos_table_format_key_from_binary (F, B),
	 (?osmos_format_from_binary ((F)#osmos_table_format.key_format, (B)))).

-define (osmos_table_format_value_to_binary (F, V),
	 (?osmos_format_to_binary ((F)#osmos_table_format.value_format, (V)))).

-define (osmos_table_format_value_from_binary (F, B),
	 (?osmos_format_from_binary ((F)#osmos_table_format.value_format,
				     (B)))).

-define (osmos_table_format_less (F, K1, K2),
	 ((F)#osmos_table_format.key_less) ((K1), (K2))).

-define (OSMOS_END, '$end_of_table').

-define (OSMOS_MIN_BLOCK_SIZE, 64).

-endif.
