%% @doc Log topic

-module(gululog_topic).

-export([ init/2
        , append/3
        , close/1
        ]).

-export_type([topic/0]).

-include("gululog_priv.hrl").

%%%*_ MACROS and SPECS =========================================================

-define(INIT_ERR(FIELD_NAME),
        erlang:error({field_value_missing, FIELD_NAME})).

-type index() :: gululog_idx:index().
-type cursor() :: gululog_w_cur:cursor().

-record(topic, { dir   = ?INIT_ERR(dir)   :: dirname()
               , segMB = ?INIT_ERR(segMB) :: bytecnt()
               , idx                      :: index()
               , cur                      :: cursor()
               , logid                    :: logid()
               }).

-opaque topic() :: #topic{}.

-define(DEFAULT_SEG_MB, 100). %% default segment size in MB

-type boolean_option() :: atom().
-type option_name() :: atom().
-type option_value() :: term().
-type option() :: [boolean_option() | {option_name(), option_value()}].
-type options() :: [option()].

-define(MEGA, 1000000).

%%%*_ API FUNCTIONS ============================================================

%% @doc Initialize topic from the given directory.
%% Assuming the directory has integrity log files ensured by
%% gululog_repair:repair_dir/2
%% @end
-spec init(dirname(), options()) -> topic().
init(Dir, Options) ->
  SegMB = keyget(segMB, Options, ?DEFAULT_SEG_MB),
  Cur = gululog_w_cur:open(Dir),
  Idx = gululog_idx:init(Dir),
  NextLogId = case gululog_idx:get_latest_logid(Idx) of
                false -> 0;
                N     -> N
              end,
  #topic{ dir   = Dir
        , idx   = Idx
        , cur   = Cur
        , segMB = SegMB
        , logid = NextLogId
        }.

%% @doc Append a new log entry to the given topic.
%% Index and segments are switched to new files in case the segment file has
%% hit the size limit.
%% @end
-spec append(topic(), header(), body()) -> topic().
append(#topic{ dir   = Dir
             , idx   = Idx
             , cur   = Cur
             , segMB = SegMB
             , logid = LogId
             } = Topic, Header, Body) ->
  Position = gululog_w_cur:next_log_position(Cur),
  %% NB! Swith before (but NOT after) appending.
  %% this is to minimize the chance of requiring a repair at restart.
  case Position >= SegMB * ?MEGA of
    true ->
      NewCur = gululog_w_cur:switch(Dir, Cur, LogId),
      NewIdx = gululog_idx:switch(Dir, Idx, LogId),
      append(Topic#topic{idx = NewIdx, cur = NewCur}, Header, Body);
    false ->
      NewCur = gululog_w_cur:append(Cur, LogId, Header, Body),
      NewIdx = gululog_idx:append(Idx, LogId, Position),
      Topic#topic{ cur   = NewCur
                 , idx   = NewIdx
                 , logid = LogId + 1
                 }
  end.

%% @doc Close index and segment writer cursor.
-spec close(topic()) -> ok.
close(#topic{idx = Idx, cur = Cur}) ->
  ok = gululog_w_cur:flush_close(Cur),
  ok = gululog_idx:flush_close(Idx).

%%%*_ PRIVATE FUNCTIONS ========================================================

%% @private Get value from key-value list, return default if not found
-spec keyget(Key::term(), [{Key::term(), Value::term()}], Default::term()) -> Value::term().
keyget(Key, KvList, Default) ->
  case lists:keyfind(Key, 1, KvList) of
    {Key, Value} -> Value;
    false        -> Default
  end.

%%%*_ TESTS ====================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
