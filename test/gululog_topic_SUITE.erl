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
t_basic_flow({'end', Config}) -> ok;
t_basic_flow(Config) when is_list(Config) ->
  Dir = ?config(dir),
  T0 = gululog_topic:init(Dir, [{segMB, 1}]),
  Body = fun(C) -> list_to_binary(lists:duplicate(500000, C)) end,
  T1 = gululog_topic:append(T0, <<"header0">>, Body($0)),
  T2 = gululog_topic:append(T1, <<"header1">>, Body($1)),
  T3 = gululog_topic:append(T2, <<"header2">>, Body($2)),
  ok = gululog_topic:close(T3),
  T4 = gululog_topic:init(Dir, [{segMB, 1}]),
  T5 = gululog_topic:append(T4, <<"header3">>, Body($3)),
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

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
