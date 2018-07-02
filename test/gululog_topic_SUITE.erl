-module(gululog_topic_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("../include/gululog.hrl").
-include("../src/gululog_priv.hrl").

%% Test server callbacks
-export([suite/0]).
-export([all/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_testcase/2]).
-export([end_per_testcase/2]).

%% cases
-export([ t_basic_flow/1
        , t_flush/1
        , t_init_with_segid/1
        , t_truncate/1
        , t_delete_oldest_seg/1
        , t_get_oldest_seg_age_seg/1
        , t_handle_corrupted_tail/1
        ]).

-define(config(KEY), proplists:get_value(KEY, Config)).

suite() -> [{timetrap, {seconds,30}}].

init_per_suite(Config) ->
  {ok, Cwd} = file:get_cwd(),
  Dir = filename:join(Cwd, "topic-suite"),
  [{dir, Dir} | Config].

end_per_suite(_Config) -> ok.

init_per_testcase(Case, Config) ->
  ok = gululog_test_lib:cleanup(?config(dir)),
  ?MODULE:Case({init, Config}).

end_per_testcase(Case, Config) ->
  ?MODULE:Case({'end', Config}).

all() -> [F || {F, _A} <- module_info(exports),
                  case atom_to_list(F) of
                    "t_" ++ _ -> true;
                    _         -> false
                  end].

t_basic_flow({init, Config}) -> Config;
t_basic_flow({'end', _Config}) -> ok;
t_basic_flow(Config) when is_list(Config) ->
  Dir = ?config(dir),
  T0 = gululog_topic:init(Dir, [{segMB, 1}]),
  Body = fun(C) -> list_to_binary(lists:duplicate(500000, C)) end,
  Ts = gululog_dt:os_sec(),
  T1 = gululog_topic:append(T0, <<"header0">>, Body($0)),
  ?assertMatch({0, _}, gululog_topic:get_latest_logid_and_ts(T1)),
  T2 = gululog_topic:append(T1, <<"header1">>, Body($1)),
  ?assertMatch({1, _}, gululog_topic:get_latest_logid_and_ts(T2)),
  T3 = gululog_topic:append(T2, <<"header2">>, Body($2)),
  ?assertMatch({2, _}, gululog_topic:get_latest_logid_and_ts(T3)),
  ok = gululog_topic:close(T3),
  T4 = gululog_topic:init(Dir, [{segMB, 1}]),
  T5 = gululog_topic:append(T4, <<"header3">>, Body($3)),
  ?assertMatch({3, _}, gululog_topic:get_latest_logid_and_ts(T5)),
  ?assertEqual(0, gululog_topic:first_logid_since(T5, Ts)),
  ok = gululog_topic:close(T5),
  Files = [ idx_name(Dir, 0)
          , idx_name(Dir, 2)
          , seg_name(Dir, 0)
          , seg_name(Dir, 2)
          ],
  lists:foreach(
    fun(File) ->
      ?assertEqual(true, filelib:is_file(File))
    end, Files),

  C0 = gululog_r_cur:open(Dir, 0),
  {C1, Log0} = gululog_r_cur:read(C0),
  ?assertMatch(#gululog{header = <<"header0">>, body = <<"0", _/binary>>}, Log0),
  {C2, Log1} = gululog_r_cur:read(C1),
  ?assertMatch(#gululog{header = <<"header1">>, body = <<"1", _/binary>>}, Log1),
  ?assertEqual(eof, gululog_r_cur:read(C2)),
  ok = gululog_r_cur:close(C2),

  C3 = gululog_r_cur:open(Dir, 2),
  {C4, Log3} = gululog_r_cur:read(C3),
  ?assertMatch(#gululog{header = <<"header2">>, body = <<"2", _/binary>>}, Log3),
  {C5, Log4} = gululog_r_cur:read(C4),
  ?assertMatch(#gululog{header = <<"header3">>, body = <<"3", _/binary>>}, Log4),
  ?assertEqual(eof, gululog_r_cur:read(C5)),
  ok = gululog_r_cur:close(C5),
  ok.

t_flush({init, Config}) -> Config;
t_flush({'end', _Config}) -> ok;
t_flush(Config) when is_list(Config) ->
  Dir = ?config(dir),
  T0 = gululog_topic:init(Dir, [{segMB, 1}]),
  T1 = gululog_topic:append(T0, <<"header1">>, <<"body1">>),
  T2 = gululog_topic:append(T1, <<"header2">>, <<"body2">>),

  ok = gululog_topic:flush(T2),
  C0 = gululog_r_cur:open(Dir, 0),
  {C1, Log1} = gululog_r_cur:read(C0, []),
  {C2, Log2} = gululog_r_cur:read(C1, []),
  ?assertMatch(#gululog{ header = <<"header1">>
                       , body   = <<"body1">>}, Log1),
  ?assertMatch(#gululog{ header = <<"header2">>
                       , body   = <<"body2">>}, Log2),

  T3 = gululog_topic:append(T2, <<"header3">>, <<"body3">>),
  ok = gululog_topic:flush(T3),
  {C3, Log3} = gululog_r_cur:read(C2, []),
  ?assertMatch(#gululog{ header = <<"header3">>
                       , body   = <<"body3">>}, Log3),
  ?assertMatch(eof, gululog_r_cur:read(C3, [])),
  ok.

%% @doc Init topic with a given segid as the first one.
t_init_with_segid({init, Config}) ->
  Config;
t_init_with_segid({'end', _Config}) ->
  ok;
t_init_with_segid(Config) when is_list(Config) ->
  Dir = ?config(dir),
  T0 = gululog_topic:init(Dir, [{segMB, 1}, {init_segid, 42}]),
  ?assertEqual(false, gululog_topic:get_latest_logid_and_ts(T0)),
  T1 = gululog_topic:append(T0, <<"h">>, <<"b">>),
  ?assertMatch({42, _}, gululog_topic:get_latest_logid_and_ts(T1)),
  ok = gululog_topic:close(T1),

  C0 = gululog_r_cur:open(Dir, 42),
  {C1, Log0} = gululog_r_cur:read(C0),
  ?assertMatch(#gululog{header = <<"h">>, body = <<"b">>}, Log0),
  ?assertEqual(eof, gululog_r_cur:read(C1)),
  ok = gululog_r_cur:close(C1),

  Files = [ idx_name(Dir, 42)
          , seg_name(Dir, 42)
          ],
  lists:foreach(
    fun(File) ->
      ?assertEqual(true, filelib:is_file(File))
    end, Files),
  ok.

t_truncate({init, Config}) ->
  Config;
t_truncate({'end', _Config}) ->
  ok;
t_truncate(Config) when is_list(Config) ->
  Dir = ?config(dir),
  BackupDir = filename:join(Dir, "backup"),
  ChangeList =
    [ {append,       <<"key">>, <<"value">>}
    , {append,       <<"key">>, <<"value">>}
    , {append,       <<"key">>, <<"value">>}
    , {append,       <<"key">>, <<"value">>}
    , {append,       <<"key">>, <<"value">>}
    , force_switch
    , {append,       <<"key">>, <<"value">>}
    , force_switch
    , {append,       <<"key">>, <<"value">>}
    , {append,       <<"key">>, <<"value">>}
    , {append,       <<"key">>, <<"value">>}
    , force_switch
    , {append,       <<"key">>, <<"value">>}
    ],
  %% generate test case data
  T14 = lists:foldl(
          fun({append, Header, Body}, IdxIn) ->
                gululog_topic:append(IdxIn, Header, Body);
             (force_switch, IdxIn) ->
                gululog_topic:force_switch(IdxIn)
          end, gululog_topic:init(Dir, []), ChangeList),
  %% 1st truncate
  {T15, Result1} = gululog_topic:truncate(T14, 10, ?undef),
  ?assertEqual([], Result1),
  {T16, Result4} = gululog_topic:truncate(T15, 7, ?undef),
  ?assertEqual([{?OP_DELETED,   idx_name(Dir, 9)},
                {?OP_DELETED,   seg_name(Dir, 9)},
                {?OP_TRUNCATED, idx_name(Dir, 6)},
                {?OP_TRUNCATED, seg_name(Dir, 6)}], lists:sort(Result4)),
  {T17, Result5} = gululog_topic:truncate(T16, 5, BackupDir),
  ?assertEqual([{?OP_DELETED, idx_name(Dir, 5)},
                {?OP_DELETED, seg_name(Dir, 5)},
                {?OP_DELETED, idx_name(Dir, 6)},
                {?OP_DELETED, seg_name(Dir, 6)}], lists:sort(Result5)),
  {T18, Result6} = gululog_topic:truncate(T17, 3, BackupDir),
  Expect = [{?OP_TRUNCATED, idx_name(Dir, 0)},
            {?OP_TRUNCATED, seg_name(Dir, 0)}],
  ?assertEqual(Expect, lists:sort(Result6)),
  Expect1 = [idx_name(BackupDir, 0),
             seg_name(BackupDir, 0),
             idx_name(BackupDir, 5),
             seg_name(BackupDir, 5),
             idx_name(BackupDir, 6),
             seg_name(BackupDir, 6)],
  ?assertEqual(Expect1, lists:sort(gululog_name:wildcard_idx_name_reversed(BackupDir)
                                   ++ gululog_name:wildcard_seg_name_reversed(BackupDir))),
  ok = gululog_topic:close(T18).

t_delete_oldest_seg({init, Config}) -> Config;
t_delete_oldest_seg({'end', _Config}) -> ok;
t_delete_oldest_seg(Config) when is_list(Config) ->
  Dir = ?config(dir),
  BackupDir = filename:join(Dir, "backup"),
  ChangeList =
    [ {append,       <<"key">>, <<"value">>} %% logid = 0, segid = 0
    , force_switch
    , {append,       <<"key">>, <<"value">>} %% logid = 1, segid = 1
    , force_switch
    , {append,       <<"key">>, <<"value">>} %% logid = 2, segid = 2
    ],
  %% generate test case data
  T0 = lists:foldl(
         fun({append, Header, Body}, TopicIn) ->
               gululog_topic:append(TopicIn, Header, Body);
            (force_switch, TopicIn) ->
               gululog_topic:force_switch(TopicIn)
         end, gululog_topic:init(Dir, []), ChangeList),
  %% first
  {T1, FileOps1} = gululog_topic:delete_oldest_seg(T0),
  ?assertEqual([{?OP_DELETED, idx_name(Dir, 0)},
                {?OP_DELETED, seg_name(Dir, 0)}], FileOps1),
  %% second
  {T2, FileOps2} = gululog_topic:delete_oldest_seg(T1, BackupDir),
  ?assertEqual([{?OP_DELETED, idx_name(Dir, 1)},
                {?OP_DELETED, seg_name(Dir, 1)}], FileOps2),
  Expect = [idx_name(BackupDir, 1),
            seg_name(BackupDir, 1)],
  ?assertEqual(Expect, lists:sort(gululog_name:wildcard_idx_name_reversed(BackupDir)
                                   ++ gululog_name:wildcard_seg_name_reversed(BackupDir))),
  {T3, FileOps3} = gululog_topic:delete_oldest_seg(T2),
  ?assertEqual([], FileOps3),
  ok = gululog_topic:close(T3).

t_get_oldest_seg_age_seg({init, Config}) ->
  meck:new(gululog_dt, [passthrough, no_passthrough_cover]),
  Config;
t_get_oldest_seg_age_seg({'end', _Config}) ->
  meck:unload(gululog_dt),
  ok;
t_get_oldest_seg_age_seg(Config) when is_list(Config) ->
  Dir = ?config(dir),
  OpList =
    [ {append, 0}
    , {expect, 0}
    , force_switch
    , {expect, 0}
    , delete_oldest
    , {expect, false}
    , {append, 1}
    , {expect, -1}
    , {append, 1}
    , {expect, -1}
    , {append, 2}
    , {expect, -2}
    , force_switch
    , {expect, -2}
    , {append, 3}
    , {expect, -2}
    ],
  SetTime = fun(Timestamp) -> meck:expect(gululog_dt, os_sec, 0, Timestamp) end,
  lists:foldl(
    fun({append, OnTimestamp}, TopicIn) ->
          SetTime(OnTimestamp),
          gululog_topic:append(TopicIn, <<"header">>, <<"body">>);
       (force_switch, TopicIn) ->
          gululog_topic:force_switch(TopicIn);
       (delete_oldest, TopicIn) ->
          {TopicOut, _} = gululog_topic:delete_oldest_seg(TopicIn),
          TopicOut;
       ({expect, ExpectedAge}, Topic) ->
          SetTime(0), %% set current time to 0 for deterministic
          ?assertEqual(ExpectedAge, gululog_topic:get_oldest_seg_age_sec(Topic)),
          Topic
    end, gululog_topic:init(Dir, []), OpList),
  ok.

t_handle_corrupted_tail({init, Config}) ->
  Config;
t_handle_corrupted_tail({'end', _Config}) ->
  ok;
t_handle_corrupted_tail(Config) when is_list(Config) ->
  %% 1, seg write ok, idx not
  %% 2, seg write ok, idx write not complete
  %% 3, seg write not complete
  Dir = ?config(dir),
  ChangeList =
    [ {append, <<"h1">>, <<"b1">>}
    , {append, <<"h2">>, <<"b2">>}
    ],
  %% generate test case data
  T0 = lists:foldl(
         fun({append, Header, Body}, IdxIn) ->
               gululog_topic:append(IdxIn, Header, Body);
            (force_switch, IdxIn) ->
               gululog_topic:force_switch(IdxIn)
         end, gululog_topic:init(Dir, []), ChangeList),
  ok = gululog_topic:close(T0),
  IdxFile = idx_name(Dir, 0),
  SegFile = seg_name(Dir, 0),
  %% simulate case 1 && 3
  {ok, Fdw} = file:open(SegFile, [read, write, raw, binary]),
  file:position(Fdw, eof),
  file:write(Fdw, <<"c">>),
  file:close(Fdw),
  T1 = gululog_topic:init(Dir, []),
  %% check content via read cursor
  C0 = gululog_r_cur:open(Dir, 0),
  {C1, Log0} = gululog_r_cur:read(C0),
  ?assertMatch(#gululog{header = <<"h1">>, body = <<"b1">>}, Log0),
  {C2, Log1} = gululog_r_cur:read(C1),
  ?assertMatch(#gululog{header = <<"h2">>, body = <<"b2">>}, Log1),
  ?assertEqual(eof, gululog_r_cur:read(C2)),
  ok = gululog_topic:close(T1),
  %% simulate case 2
  {ok, Fdidx} = file:open(IdxFile, [read, write, raw, binary]),
  {ok, Size} = file:position(Fdidx, eof),
  file:position(Fdidx, Size - 1),
  file:truncate(Fdidx),
  file:close(Fdidx),
  T2 = gululog_topic:init(Dir, []),
  %% check content via read cursor
  C00 = gululog_r_cur:open(Dir, 0),
  {C01, Log00} = gululog_r_cur:read(C00),
  ?assertMatch(#gululog{header = <<"h1">>, body = <<"b1">>}, Log00),
  ?assertEqual(eof, gululog_r_cur:read(C01)),
  ok = gululog_topic:close(T2),
  ok.

%%%_* Help functions ===========================================================

idx_name(Dir, SegId) -> gululog_name:mk_idx_name(Dir, SegId).

seg_name(Dir, SegId) -> gululog_name:mk_seg_name(Dir, SegId).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
