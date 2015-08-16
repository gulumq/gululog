
-ifndef(GULULOG_HRL).
-define(GULULOG_HRL, true).

-type gululog_logid()     :: 0..18446744073709551615. %% 2 ^ 64 - 1
-type gululog_timestamp() :: non_neg_integer(). %% unix epoch seconds
-type gululog_header()    :: binary().
-type gululog_body()      :: binary().

-define(GULULOG_DEFAULT_CACHE_POLICY, minimum).

-define(GULULOG_DEFAULT_SEG_MB, 100). %% default segment size in MB

-type gululog_cache_policy() :: minimum %% cache only the first and last entry per segment
                              | all %% cache all log entries in all segments
                              | {every, pos_integer()}. %% cache every N-th log entries

-type gululog_options() :: [ {cache_policy, gululog_cache_policy()}
                           | {segMB, pos_integer()}
                           | {init_segid, pos_integer()}
                           ].


-record(gululog, { logid     :: gululog_logid()
                 , header    :: gululog_header()
                 , body      :: undefined %% if skipped when reading
                              | gululog_body()
                 }).

-type gululog() :: #gululog{}.

-endif.

