
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
-export([ t_basic_flow/1
        , t_init_from_existing/1
        ]).

suite() -> [{timetrap, {seconds,30}}].

init_per_suite(Config) ->
  Dir = filename:join([".", "tmp", atom_to_list(?MODULE)]),
  [{dir, Dir} | Config].

end_per_suite(_Config) -> ok.

init_per_testcase(Case, Config) ->
  {dir, Dir} = lists:keyfind(dir, 1, Config),
  IdxFiles = filelib:wildcard("*.idx", Dir),
  lists:foreach(fun(File) ->
                  ok = file:delete(filename:join(Dir, File))
                end, IdxFiles),
  ?MODULE:Case({init, Config}).

end_per_testcase(Case, Config) ->
  ?MODULE:Case({'end', Config}).

all() -> [F || {F, _A} <- module_info(exports),
                  case atom_to_list(F) of
                    "t_" ++ _ -> true;
                    _         -> false
                  end].

-define(config(KEY), proplists:get_value(KEY, Config)).

%% @doc Test a basic indexing work flow.
t_basic_flow({init, Config}) -> Config;
t_basic_flow({'end', _Config}) -> ok;
t_basic_flow(Config) ->
  Dir = ?config(dir),
  Index0 = gululog_idx:init(Dir),
  ?assertEqual(false, gululog_idx:get_last_logid(Index0)),
  ok = gululog_idx:append(Index0, 0, 1),
  ok = gululog_idx:append(Index0, 1, 10),
  ok = gululog_idx:append(Index0, 2, 40),
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
  ?assertEqual(4, gululog_idx:get_last_logid(Index)),
  ok.

%% @doc Init from existing files.
t_init_from_existing({init, Config}) ->
  Dir = ?config(dir),
  Index0 = gululog_idx:init(Dir),
  ok = gululog_idx:append(Index0, 0, 1),
  ok = gululog_idx:append(Index0, 1, 10),
  Index1 = gululog_idx:switch_append(Dir, Index0, 2, 3),
  ok = gululog_idx:append(Index1, 3, 50),
  ok = gululog_idx:close(Index1),
  Config;
t_init_from_existing({'end', _Config}) ->
  ok;
t_init_from_existing(Config) ->
  Dir = ?config(dir),
  Index0 = gululog_idx:init(Dir),
  ?assertEqual(3, gululog_idx:get_last_logid(Index0)),
  Expects = [ {0, {0, 1}}
            , {1, {0, 10}}
            , {2, {2, 3}}
            , {3, {2, 50}}
            , {4, {4, 1}}
            ],
  Index = gululog_idx:switch(Dir, Index0, _NextLogId = 4),
  ok = gululog_idx:append(Index, 4, 1),
  lists:foreach(fun({LogId, ExpectedLocation}) ->
                  Location = gululog_idx:locate(Dir, Index, LogId),
                  ?assertEqual(ExpectedLocation, Location)
                end, Expects),
  ?assertEqual(4, gululog_idx:get_last_logid(Index)),
  ok.

