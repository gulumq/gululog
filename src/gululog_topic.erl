%% @doc Log topic

-module(gululog_topic).

-export([ init/2
        , append/3
        , close/1
        , force_switch/1
        , truncate/3
        , delete_oldest_seg/1
        , delete_oldest_seg/2
        , first_logid_since/2
        , get_oldest_seg_age_sec/1
        , get_latest_logid_and_ts/1
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
               , last_logid               :: false | logid()
               , last_ts                  :: false | os_sec()
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
%% Assuming the directory has integrity in log files ensured by
%% gululog_repair:repair_dir/2
%% @end
-spec init(dirname(), options()) -> topic().
init(Dir, Options) ->
  SegMB = keyget(segMB, Options, ?DEFAULT_SEG_MB),
  Cur = gululog_w_cur:open(Dir),
  Idx = gululog_idx:init(Dir),
  maybe_switch_to_new_version(
    #topic{ dir        = Dir
          , idx        = Idx
          , cur        = Cur
          , segMB      = SegMB
          , last_logid = gululog_idx:get_latest_logid(Idx)
          , last_ts    = gululog_idx:get_latest_ts(Idx)
          }).

%% @doc Append a new log entry to the given topic.
%% Index and segments are switched to new files in case the segment file has
%% hit the size limit.
%% @end
-spec append(topic(), header(), body()) -> topic().
append(#topic{ dir        = Dir
             , idx        = Idx
             , cur        = Cur
             , segMB      = SegMB
             , last_logid = LastLogId
             , last_ts    = LastTs
             } = Topic, Header, Body) ->
  LogId = next_logid(LastLogId),
  Position = gululog_w_cur:next_log_position(Cur),
  %% NB! Swith before (but NOT after) appending.
  %% this is to minimize the chance of requiring a repair at restart.
  case Position >= SegMB * ?MEGA of
    true ->
      NewCur = gululog_w_cur:switch(Dir, Cur, LogId),
      NewIdx = gululog_idx:switch(Dir, Idx, LogId),
      append(Topic#topic{idx = NewIdx, cur = NewCur}, Header, Body);
    false ->
      Ts     = next_ts(LastTs),
      NewCur = gululog_w_cur:append(Cur, LogId, Header, Body),
      NewIdx = gululog_idx:append(Idx, LogId, Position, Ts),
      Topic#topic{ cur        = NewCur
                 , idx        = NewIdx
                 , last_logid = LogId
                 , last_ts    = Ts
                 }
  end.

%% @doc Get last appended log infomation.
%% This is the information in reply to the producers.
%% @end
-spec get_latest_logid_and_ts(topic()) -> false | {logid(), os_sec()}.
get_latest_logid_and_ts(#topic{ last_logid = LogId
                              , last_ts    = Ts
                              }) ->
  LogId =/= false andalso {LogId, Ts}.

%% @doc Get the age (in seconds) of the oldest segment.
%% The age of a segment = the age of the latest log entrie in the segment.
%% @end
-spec get_oldest_seg_age_sec(topic()) -> false | os_sec().
get_oldest_seg_age_sec(#topic{dir = Dir, idx = Idx}) ->
  case gululog_idx:get_oldest_segid(Idx) of
    false -> false;
    SegId -> gululog_dt:os_sec() - gululog_idx:get_seg_latest_ts(Dir, Idx, SegId)
  end.

%% @doc Find first logid which was appended at or after the given
%% timestamp (server time). Return 'false' if no such log entry.
%% @end
-spec first_logid_since(topic(), os_sec()) -> false | logid().
first_logid_since(#topic{dir = Dir, idx = Idx}, Ts) ->
  gululog_idx:first_logid_since(Dir, Idx, Ts).

%% @doc Close index and segment writer cursor.
-spec close(topic()) -> ok.
close(#topic{idx = Idx, cur = Cur}) ->
  ok = gululog_w_cur:flush_close(Cur),
  ok = gululog_idx:flush_close(Idx).

%% @doc Force switching to a new segment.
-spec force_switch(topic()) -> topic().
force_switch(#topic{ dir        = Dir
                   , idx        = Idx
                   , cur        = Cur
                   , last_logid = LastLogId
                   } = Topic) ->
  LogId  = next_logid(LastLogId),
  NewCur = gululog_w_cur:switch(Dir, Cur, LogId),
  NewIdx = gululog_idx:switch(Dir, Idx, LogId),
  Topic#topic{ idx = NewIdx
             , cur = NewCur
             }.

%% @doc Truncate the topic from (including) the given logid.
-spec truncate(topic(), logid(), ?undef | dirname()) -> {topic(), [file_op()]}.
truncate(#topic{dir = Dir, idx = Idx, cur = Cur} = Topic, LogId, BackupDir) ->
  case gululog_idx:locate(Dir, Idx, LogId) of
    false ->
      {Topic, []};
    {SegId, Position} ->
      {NewIdx, IdxFileOpList} =
        gululog_idx:truncate(Dir, Idx, SegId, LogId, BackupDir),
      {NewCur, SegFileOpList} =
        gululog_w_cur:truncate(Dir, Cur, SegId, Position, BackupDir),
      NewTopic = Topic#topic{idx = NewIdx, cur = NewCur},
      {maybe_switch_to_new_version(NewTopic),
       IdxFileOpList ++ SegFileOpList}
  end.

%% @equiv delete_oldest_seg/2
-spec delete_oldest_seg(topic()) -> {topic(), [file_op()]}.
delete_oldest_seg(Topic) -> delete_oldest_seg(Topic, ?undef).

%% @doc Delete oldest segment from topic.
%% Return new topic and a list of deleted files with OP tags.
%% The delete is ignored, (return empty file list) when:
%% 1. Nothing to delete, topic is empty
%% 2. The oldest segment is also the newest
%%    --- This is considered purging the entrir topic, should be done using
%%        truncation API instead
%% @end
-spec delete_oldest_seg(topic(), ?undef | dirname()) -> {topic(), [file_op()]}.
delete_oldest_seg(#topic{dir = Dir, idx = Idx, cur = Cur} = Topic, BackupDir) ->
  case gululog_idx:delete_oldest_seg(Dir, Idx, BackupDir) of
    {NewIdx, false} ->
      %% ignored
      {Topic#topic{idx = NewIdx}, []};
    {NewIdx, {SegId, IdxFileOp}} ->
      {NewCur, SegFileOp} = gululog_w_cur:delete_seg(Dir, Cur, SegId, BackupDir),
      {Topic#topic{idx = NewIdx, cur = NewCur}, [IdxFileOp, SegFileOp]}
  end.

%%%*_ PRIVATE FUNCTIONS ========================================================

%% @private Keep logid sequential.
next_logid(false) -> 0;
next_logid(LogId) -> LogId + 1.

%% @private Keep timestamp monotonic.
next_ts(false) ->
  gululog_dt:os_sec();
next_ts(Ts) ->
  case gululog_dt:os_sec() of
    NewTs when NewTs >= Ts -> NewTs;
    _                      -> Ts
  end.

%% @private Nothing to do so far.
maybe_switch_to_new_version(Topic) -> Topic.

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

next_ts_test() ->
  meck:new(gululog_dt, [passthrough, no_passthrough_cover]),
  Arg = [false, 4, 5, 5, 5, 5, 5], %% next_ts/1 arguments
  Seq = [4,     5, 4, 3, 3, 2, 6], %% gululog_dt:os_sec return values
  Exp = [4,     5, 5, 5, 5, 5, 6], %% expected result of next_ts/1
  meck:sequence(gululog_dt, os_sec, 0, Seq),
  ArgExp = lists:zip(Arg, Exp),
  lists:foreach(fun({Arg_, Exp_}) ->
                  ?assertEqual(Exp_, next_ts(Arg_))
                end, ArgExp),
  meck:unload(gululog_dt),
  ok.

-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:

