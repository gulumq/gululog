
-ifndef(GULULOG_HRL).
-define(GULULOG_HRL, true).

-type gululog_logid()     :: 0..18446744073709551615. %% 2 ^ 64 - 1
-type gululog_timestamp() :: pos_integer(). %% unix epoch seconds
-type gululog_header()    :: binary().
-type gululog_body()      :: binary().

-record(gululog, { logid     :: gululog_logid()
                 , logged_on :: gululog_timestamp()
                 , header    :: gululog_header()
                 , body      :: undefined %% if skipped when reading
                              | gululog_body()
                 }).

-type gululog() :: #gululog{}.

-endif.

