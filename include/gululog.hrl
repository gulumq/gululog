
-ifndef(GULULOG_HRL).
-define(GULULOG_HRL, true).

-type logid() :: 0..18446744073709551615. %% 1 bsl 64 - 1
-type segid() :: logid(). %% The first log ID in a log segment
-type offset() :: 0..4294967295.   %% in-segment log id offset
-type position() :: 0..4294967295. %% in-file byte position
-type filename() :: string().
-type dirname() :: string().
-type logvsn()  :: 1..255.
-type micro() :: pos_integer().
-type bytecnt() :: pos_integer().

-define(LOGVSN, 1).

-endif.

