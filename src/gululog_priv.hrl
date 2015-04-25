
-ifndef(GULULOG_PRIV_HRL).
-define(GULULOG_PRIV_HRL, true).

-include("gululog.hrl").

-type logid()    :: gululog_logid().
-type os_sec()   :: gululog_timestamp().
-type os_micro() :: pos_integer().

-type segid()    :: logid().       %% The first log ID in a log segment
-type offset()   :: 0..4294967295. %% in-segment log id offset
-type position() :: 0..4294967295. %% in-file byte position
-type filename() :: string().
-type dirname()  :: string().
-type logvsn()   :: 1..255.
-type bytecnt()  :: pos_integer().
-type header()   :: binary().
-type body()     :: binary().

-define(LOGVSN, 1). %% Current version
-define(DOT_IDX, ".idx").
-define(DOT_SEG, ".seg").
-define(undef, undefined).

-endif.

