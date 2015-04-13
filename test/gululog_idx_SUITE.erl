
-module(gululog_idx_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Test server callbacks
-export([suite/0]).
-export([all/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_testcase/2]).
-export([end_per_testcase/2]).

%% cases
-export([t_basic_flow/1]).

suite() -> [{timetrap, {seconds,30}}].

init_per_suite(Config) ->
  Dir = filename:join([".", "tmp", atom_to_list(?MODULE)]),
  [{dir, Dir} | Config].

end_per_suite(_Config) -> ok.

init_per_testcase(_Case, Config) ->
  {dir, Dir} = lists:keyfind(dir, 1, Config),
  IdxFiles = filelib:wildcard("*.idx", Dir),
  lists:foreach(fun(File) -> file:delete(File) end, IdxFiles),
  Config.

end_per_testcase(_Case, _Config) -> ok.

all() -> [F || {F, _A} <- module_info(exports),
                  case atom_to_list(F) of
                    "t_" ++ _ -> true;
                    _         -> false
                  end].

%% @doc Test a basic indexing work flow.
t_basic_flow(Config) ->
  {dir, Dir} = lists:keyfind(dir, 1, Config),
  Index0 = gululog_idx:init(Dir),
  ?assertEqual(false, gululog_idx:get_last_logid(Index0)),
  ok = gululog_idx:append(Index0, 0, 0),
  ok = gululog_idx:append(Index0, 1, 10),
  ok = gululog_idx:append(Index0, 2, 40),
  Index3 = gululog_idx:switch_append(Dir, Index0, 3, 50),
  Index4 = gululog_idx:switch_append(Dir, Index3, 4, 60),
  Index  = gululog_idx:switch(Dir, Index4, 5),
  ?assertEqual({0, 0},  gululog_idx:locate(Dir, Index, 0)),
  ?assertEqual({0, 10}, gululog_idx:locate(Dir, Index, 1)),
  ?assertEqual({0, 40}, gululog_idx:locate(Dir, Index, 2)),
  ?assertEqual({3, 50}, gululog_idx:locate(Dir, Index, 3)),
  ?assertEqual({4, 60}, gululog_idx:locate(Dir, Index, 4)),
  ?assertEqual(false,   gululog_idx:locate(Dir, Index, 5)),
  ?assertEqual(4, gululog_idx:get_last_logid(Index)),
  ok.

