-ifndef (OSMOS_VLI_HRL).
-define (OSMOS_VLI_HRL, true).

%
% variable-length integers
%

vli32_read (<< 2#1100:4,     N:12, R/binary >>) -> { N, R };
vli32_read (<< 2#1101:4,     N:20, R/binary >>) -> { N, R };
vli32_read (<< 2#1110:4,     N:28, R/binary >>) -> { N, R };
vli32_read (<< 2#11110000:8, N:32, R/binary >>) -> { N, R };
vli32_read (<<               N:8,  R/binary >>) -> { N, R }.

vli32_write (N) when N < 2#11000000	-> <<               N:8  >>;
vli32_write (N) when N < 16#1000	-> << 2#1100:4,     N:12 >>;
vli32_write (N) when N < 16#100000	-> << 2#1101:4,     N:20 >>;
vli32_write (N) when N < 16#10000000	-> << 2#1110:4,     N:28 >>;
vli32_write (N) when N < 16#100000000	-> << 2#11110000:8, N:32 >>.

% XXX: stupid dialyzer
-ifdef (OSMOS_VLI_LENGTH).
vli32_length (N) when N < 2#11000000	-> 1;
vli32_length (N) when N < 16#1000	-> 2;
vli32_length (N) when N < 16#100000	-> 3;
vli32_length (N) when N < 16#10000000	-> 4;
vli32_length (N) when N < 16#100000000	-> 5. 
-endif.

-endif.
