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
        , t_truncate/1
        , t_delete_oldest_seg/1
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
  T1 = gululog_topic:append(T0, <<"header0">>, Body($0)),
  T2 = gululog_topic:append(T1, <<"header1">>, Body($1)),
  T3 = gululog_topic:append(T2, <<"header2">>, Body($2)),
  ok = gululog_topic:close(T3),
  T4 = gululog_topic:init(Dir, [{segMB, 1}]),
  _T5 = gululog_topic:append(T4, <<"header3">>, Body($3)),
  Files = [ gululog_name:mk_idx_name(Dir, 0)
          , gululog_name:mk_idx_name(Dir, 2)
          , gululog_name:mk_seg_name(Dir, 0)
          , gululog_name:mk_seg_name(Dir, 2)
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

t_truncate({init, Config}) ->
  Config;
t_truncate({'end', _Config}) ->
  ok;
t_truncate(Config) when is_list(Config) ->
  Dir = ?config(dir),
  BackupDir = filename:join(Dir, "backup"),
  CaseList =
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
          end, gululog_topic:init(Dir, []), CaseList),
  %% 1st truncate
  {T15, Result1} = gululog_topic:truncate(T14, 10, ?undef),
  ?assertEqual([], Result1),
  {T16, Result4} = gululog_topic:truncate(T15, 7, ?undef),
  ?assertEqual([{?OP_DELETED,   gululog_name:mk_idx_name(Dir, 9)},
                {?OP_DELETED,   gululog_name:mk_seg_name(Dir, 9)},
                {?OP_TRUNCATED, gululog_name:mk_idx_name(Dir, 6)},
                {?OP_TRUNCATED, gululog_name:mk_seg_name(Dir, 6)}], lists:sort(Result4)),
  {T17, Result5} = gululog_topic:truncate(T16, 5, BackupDir),
  ?assertEqual([{?OP_DELETED, gululog_name:mk_idx_name(Dir, 5)},
                {?OP_DELETED, gululog_name:mk_seg_name(Dir, 5)},
                {?OP_DELETED, gululog_name:mk_idx_name(Dir, 6)},
                {?OP_DELETED, gululog_name:mk_seg_name(Dir, 6)}], lists:sort(Result5)),
  {_T18, Result6} = gululog_topic:truncate(T17, 3, BackupDir),
  Expect = [{?OP_TRUNCATED, gululog_name:mk_idx_name(Dir, 0)},
            {?OP_TRUNCATED, gululog_name:mk_seg_name(Dir, 0)}],
  ?assertEqual(Expect, lists:sort(Result6)),
  Expect1 = [gululog_name:mk_idx_name(BackupDir, 0),
             gululog_name:mk_seg_name(BackupDir, 0),
             gululog_name:mk_idx_name(BackupDir, 5),
             gululog_name:mk_seg_name(BackupDir, 5),
             gululog_name:mk_idx_name(BackupDir, 6),
             gululog_name:mk_seg_name(BackupDir, 6)],
  ?assertEqual(Expect1, lists:sort(gululog_name:wildcard_idx_name_reversed(BackupDir)
                                   ++ gululog_name:wildcard_seg_name_reversed(BackupDir))).

t_delete_oldest_seg({'init', Config}) ->
  Config;
t_delete_oldest_seg({'end', _Config}) ->
  ok;
t_delete_oldest_seg(Config) when is_list(Config) ->
  Dir = ?config(dir),
  BackupDir = filename:join(Dir, "backup"),
  CaseList =
    [ {append,       <<"key">>, <<"value">>} %% logid = 0, segid = 0
    , {append,       <<"key">>, <<"value">>} %% logid = 1, segid = 0
    , {append,       <<"key">>, <<"value">>} %% logid = 2, segid = 0
    , {append,       <<"key">>, <<"value">>} %% logid = 3, segid = 0
    , {append,       <<"key">>, <<"value">>} %% logid = 4, segid = 0
    , force_switch
    , {append,       <<"key">>, <<"value">>} %% logid = 5, segid = 5
    , force_switch
    , {append,       <<"key">>, <<"value">>} %% logid = 6, segid = 6
    , {append,       <<"key">>, <<"value">>} %% logid = 7, segid = 6
    , {append,       <<"key">>, <<"value">>} %% logid = 8, segid = 6
    , force_switch
    , {append,       <<"key">>, <<"value">>} %% logid = 9, segid = 7
    ],
  %% generate test case data
  InitTopic = lists:foldl(
                fun({append, Header, Body}, IdxIn) ->
                      gululog_topic:append(IdxIn, Header, Body);
                   (force_switch, IdxIn) ->
                      gululog_topic:force_switch(IdxIn)
                end, gululog_topic:init(Dir, []), CaseList),
  %% first
  ?assertEqual({[?OP_DELETED], InitTopic}, gululog_topic:delete_oldest_seg(InitTopic)),
  %% second
  ?assertEqual({[?OP_DELETED], InitTopic}, gululog_topic:delete_oldest_seg(InitTopic, BackupDir)),
  Expect = [gululog_name:mk_idx_name(BackupDir, 5),
            gululog_name:mk_seg_name(BackupDir, 5)],
  ?assertEqual(Expect, lists:sort(gululog_name:wildcard_idx_name_reversed(BackupDir)
                                   ++ gululog_name:wildcard_seg_name_reversed(BackupDir))),
  ?assertEqual({[?OP_DELETED], InitTopic}, gululog_topic:delete_oldest_seg(InitTopic)),
  ?assertEqual({[], InitTopic}, gululog_topic:delete_oldest_seg(InitTopic)),
  ?assertEqual({[], InitTopic}, gululog_topic:delete_oldest_seg(InitTopic, BackupDir)).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
