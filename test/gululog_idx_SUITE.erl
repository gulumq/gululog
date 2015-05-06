
-module(gululog_idx_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
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
        , t_init_from_existing/1
        , t_scan_file_to_locate/1
        , t_truncate/1
        ]).

-define(config(KEY), proplists:get_value(KEY, Config)).

suite() -> [{timetrap, {seconds,30}}].

init_per_suite(Config) ->
  {ok, Cwd} = file:get_cwd(),
  Dir = filename:join(Cwd, "idx-suite"),
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

%% @doc Test a basic indexing work flow.
t_basic_flow({init, Config}) -> Config;
t_basic_flow({'end', _Config}) -> ok;
t_basic_flow(Config) when is_list(Config) ->
  Dir = ?config(dir),
  Index0 = gululog_idx:init(Dir),
  ?assertEqual(false, gululog_idx:get_latest_logid(Index0)),
  Index0 = gululog_idx:append(Index0, 0, 1),
  Index0 = gululog_idx:append(Index0, 1, 10),
  Index0 = gululog_idx:append(Index0, 2, 40),
  Index3 = gululog_idx:switch_append(Dir, Index0, 3, 1),
  Index4 = gululog_idx:switch_append(Dir, Index3, 4, 1),
  Index  = gululog_idx:switch(Dir, Index4, _NexLogId = 5),
  Expects = [ {0, {0, 1}}
            , {1, {0, 10}}
            , {2, {0, 40}}
            , {3, {3, 1}}
            , {4, {4, 1}}
            , {5, false}
            ],
  lists:foreach(fun({LogId, ExpectedLocation}) ->
                  Location = gululog_idx:locate(Dir, Index, LogId),
                  ?assertEqual(ExpectedLocation, Location)
                end, Expects),
  ?assertEqual(4, gululog_idx:get_latest_logid(Index)),
  %% delete segment 0
  ?assertEqual(0, gululog_idx:delete_oldest_seg(Dir, Index)),
  %% delete segment 3
  ?assertEqual(3, gululog_idx:delete_oldest_seg(Dir, Index)),
  %% verify that segment 4 is still there
  ?assertEqual(4, gululog_idx:get_latest_logid(Index)),
  %% delete segment 4
  ?assertEqual(4, gululog_idx:delete_oldest_seg(Dir, Index)),
  %% nothing left (segment 5 is empty)
  ?assertEqual(false, gululog_idx:delete_oldest_seg(Dir, Index)),
  %% verify nothing left
  ?assertEqual(false, gululog_idx:get_latest_logid(Index)),
  ok.

%% @doc Init from existing files.
t_init_from_existing({init, Config}) ->
  Dir = ?config(dir),
  Index0 = gululog_idx:init(Dir),
  Index0 = gululog_idx:append(Index0, 0, 1),
  Index0 = gululog_idx:append(Index0, 1, 10),
  Index1 = gululog_idx:switch_append(Dir, Index0, 2, 3),
  Index1 = gululog_idx:append(Index1, 3, 50),
  ok = gululog_idx:flush_close(Index1),
  Config;
t_init_from_existing({'end', _Config}) -> ok;
t_init_from_existing(Config) when is_list(Config) ->
  Dir = ?config(dir),
  Index0 = gululog_idx:init(Dir),
  ?assertEqual(3, gululog_idx:get_latest_logid(Index0)),
  Expects = [ {0, {0, 1}}
            , {1, {0, 10}}
            , {2, {2, 3}}
            , {3, {2, 50}}
            , {4, {4, 1}}
            ],
  Index = gululog_idx:switch(Dir, Index0, _NextLogId = 4),
  Index = gululog_idx:append(Index, 4, 1),
  lists:foreach(fun({LogId, ExpectedLocation}) ->
                  Location = gululog_idx:locate(Dir, Index, LogId),
                  ?assertEqual(ExpectedLocation, Location)
                end, Expects),
  ?assertEqual(4, gululog_idx:get_latest_logid(Index)),
  ok.

%% @doc Delete log entry from index cache, scan idx file to locate.
t_scan_file_to_locate({init, Config}) ->
  Dir = ?config(dir),
  Idx0 = gululog_idx:init(Dir),
  Idx1 = gululog_idx:append(Idx0, 0, 1),
  Idx2 = gululog_idx:append(Idx1, 1, 10),
  Idx3 = gululog_idx:append(Idx2, 2, 20),
  Idx4 = gululog_idx:append(Idx3, 3, 32),
  ok = gululog_idx:flush_close(Idx4),
  Config;
t_scan_file_to_locate({'end', _Config}) -> ok;
t_scan_file_to_locate(Config) when is_list(Config) ->
  Dir = ?config(dir),
  Idx0 = gululog_idx:init(Dir),
  ?assertEqual(false, gululog_idx:locate(Dir, Idx0, 4)),
  ?assertEqual({0, 10}, gululog_idx:locate(Dir, Idx0, 1)),
  Idx1 = gululog_idx:delete_from_cache(Idx0, 0),
  Idx1 = gululog_idx:delete_from_cache(Idx0, 1), %% assert no change
  Idx1 = gululog_idx:delete_from_cache(Idx0, 1), %% delete twice assert no change
  ?assertEqual({0, 10}, gululog_idx:locate(Dir, Idx1, 1)),
  ?assertEqual({0, 32}, gululog_idx:locate(Dir, Idx1, 3)),
  Idx2 = gululog_idx:delete_from_cache(Idx1, 3),
  ?assertEqual({0, 32}, gululog_idx:locate(Dir, Idx2, 3)),
  ?assertEqual(false, gululog_idx:locate(Dir, Idx0, 4)),
  ok.

t_truncate({init, Config}) ->
  Config;
t_truncate({'end', _Config}) ->
  ok;
t_truncate(Config) when is_list(Config) ->
  Dir = ?config(dir),
  T1 = gululog_topic:init(Dir, []),
  T2 = gululog_topic:append(T1, <<"key">>, <<"value">>),
  T3 = gululog_topic:append(T2, <<"key">>, <<"value">>),
  T4 = gululog_topic:append(T3, <<"key">>, <<"value">>),
  T5 = gululog_topic:append(T4, <<"key">>, <<"value">>),
  T6 = gululog_topic:append(T5, <<"key">>, <<"value">>),
  T7 = gululog_topic:force_switch(T6),
  T8 = gululog_topic:append(T7, <<"key">>, <<"value">>),
  T8 = gululog_topic:append(T7, <<"key">>, <<"value">>),
  T9 = gululog_topic:force_switch(T8),
  T10 = gululog_topic:append(T9, <<"key">>, <<"value">>),
  Idx = erlang:element(4, T10),
  Expect = [{gululog_name:mk_idx_name(Dir, 0), gululog_name:mk_seg_name(Dir, 0)},
            {gululog_name:mk_idx_name(Dir, 5), gululog_name:mk_seg_name(Dir, 5)},
            {gululog_name:mk_idx_name(Dir, 6), gululog_name:mk_seg_name(Dir, 6)}],
  {ok, TruncateResult} = gululog_idx:truncate(Dir, Idx, 3),
  ?assertEqual(Expect, lists:sort(TruncateResult)).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
