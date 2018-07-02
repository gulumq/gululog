%% @doc Log topic

-module(gululog_topic).

-export([ init/2
        , append/3
        , flush/1
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
               , init_segid               :: segid()
               , idx                      :: index()
               , cur                      :: cursor()
               }).

-opaque topic() :: #topic{}.

-type options() :: gululog_options().

-define(MEGA, 1000000).

%%%*_ API FUNCTIONS ============================================================

%% @doc Initialize topic from the given directory.
%% Options:
%%   {cache_policy, CachePolicy}
%%     where CachePolicy is one of:
%%       minimum                : cache only the first entry per segment
%%       all                    : cache all log entries in all segments
%%       {every, pos_integer()} : cache every N-th log entries
%%   {segMB, Limit}:
%%     Segment file size limit (MB), gululog_topic will roll to next
%%     segment when the size has *exceeded* this limit.
%%   {init_segid, FirstLogId}:
%%     First segment ID to init from, default 0.
%%     This is to allow a truncated topic to be re-initialized
%%     from the last logid before trunctation; also, a higher
%%     level application may need to initialize a topic as a copy
%%     of a remote topic, this option should be the same as the
%%     remote first segid of the remote topic.
%% @end
-spec init(dirname(), options()) -> topic().
init(Dir, Options) ->
  SegMB = keyget(segMB, Options, ?GULULOG_DEFAULT_SEG_MB),
  InitFromSegId = keyget(init_segid, Options, 0),
  Cur0 = gululog_w_cur:open(Dir, InitFromSegId),
  Idx = gululog_idx:init(Dir, InitFromSegId, Options),
  Cur =
    case gululog_idx:get_latest_logid(Idx) of
      false ->
        {NewCur0, _} =
          gululog_w_cur:truncate(Dir, Cur0, InitFromSegId, 1, ?undef),
        NewCur0;
      LogId ->
        gululog_w_cur:handle_corrupted_tail(Cur0, LogId)
  end,
  Topic =
    #topic{ dir        = Dir
          , idx        = Idx
          , cur        = Cur
          , segMB      = SegMB
          , init_segid = InitFromSegId
          },
  maybe_switch_to_new_version(Topic).

%% @doc Append a new log entry to the given topic.
%% Index and segments are switched to new files in case the segment file has
%% hit the size limit.
%% @end
-spec append(topic(), header(), body()) -> topic().
append(#topic{ dir        = Dir
             , idx        = Idx
             , cur        = Cur
             , segMB      = SegMB
             } = Topic, Header, Body) ->
  LogId = next_logid(Topic),
  Position = gululog_w_cur:next_log_position(Cur),
  %% NB! Swith before (but NOT after) appending.
  %% this is to minimize the chance of requiring a repair at restart.
  case Position >= SegMB * ?MEGA of
    true ->
      NewCur = gululog_w_cur:switch(Dir, Cur, LogId),
      NewIdx = gululog_idx:switch(Dir, Idx, LogId),
      append(Topic#topic{idx = NewIdx, cur = NewCur}, Header, Body);
    false ->
      Ts     = next_ts(gululog_idx:get_latest_logid(Idx)),
      NewCur = gululog_w_cur:append(Cur, LogId, Header, Body),
      NewIdx = gululog_idx:append(Idx, LogId, Position, Ts),
      Topic#topic{ cur        = NewCur
                 , idx        = NewIdx
                 }
  end.

%% @doc Get last appended log infomation.
%% This is the information in reply to the producers.
%% @end
-spec get_latest_logid_and_ts(topic()) -> false | {logid(), os_sec()}.
get_latest_logid_and_ts(#topic{idx = Idx}) ->
  LogId = gululog_idx:get_latest_logid(Idx),
  LogId =/= false andalso {LogId, gululog_idx:get_latest_ts(Idx)}.

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

%% @doc Flush index and segment writer cursor.
-spec flush(topic()) -> ok.
flush(#topic{idx = Idx, cur = Cur}) ->
  ok = gululog_w_cur:flush(Cur),
  ok = gululog_idx:flush(Idx).

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
                   } = Topic) ->
  LogId  = next_logid(Topic),
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
next_logid(#topic{init_segid = InitSegId, idx = Idx}) ->
  case gululog_idx:get_latest_logid(Idx) of
    false -> InitSegId;
    LogId -> LogId + 1
  end.

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

