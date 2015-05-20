
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
        , t_read_file_entry_to_locate/1
        , t_truncated_latest_logid/1
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
  Events = [ {append, 0, 1}
           , {append, 1, 10}
           , {append, 2, 40}
           , {switch_append, 3, 1}
           , {switch_append, 4, 1}
           ],
  Idx0 = init_idx(Dir, Events),
  Idx1 = gululog_idx:switch(Dir, Idx0, _NexLogId = 5),
  Expects = [ {0, {0, 1}}
            , {1, {0, 10}}
            , {2, {0, 40}}
            , {3, {3, 1}}
            , {4, {4, 1}}
            , {5, false}
            ],
  lists:foreach(fun({LogId, ExpectedLocation}) ->
                  Location = gululog_idx:locate(Dir, Idx1, LogId),
                  ?assertEqual(ExpectedLocation, Location)
                end, Expects),
  ?assertMatch(4, gululog_idx:get_latest_logid(Idx1)),
  %% delete segment 0
  {Idx2, {0, _}} = gululog_idx:delete_oldest_seg(Dir, Idx1, ?undef),
  %% delete segment 3
  {Idx3, {3, _}} = gululog_idx:delete_oldest_seg(Dir, Idx2, ?undef),
  %% verify that segment 4 is still there
  ?assertMatch(4, gululog_idx:get_latest_logid(Idx3)),
  %% delete segment 4
  {Idx4, {4, _}} = gululog_idx:delete_oldest_seg(Dir, Idx3, ?undef),
  %% nothing left (segment 5 is empty)
  {Idx5, false} = gululog_idx:delete_oldest_seg(Dir, Idx4, ?undef),
  %% verify nothing left
  ?assertMatch(false, gululog_idx:get_latest_logid(Idx5)),
  ok = gululog_idx:flush_close(Idx5).

%% @doc Init from existing files.
t_init_from_existing({init, Config}) ->
  Dir = ?config(dir),
  Idx = init_idx(Dir, [ {append,        0, 1}
                      , {append,        1, 10}
                      , {switch_append, 2, 3}
                      , {append,        3, 50}
                      ]),
  ok = gululog_idx:flush_close(Idx),
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
  Index = gululog_idx:append(Index, 4, 1, ts()),
  lists:foreach(fun({LogId, ExpectedLocation}) ->
                  Location = gululog_idx:locate(Dir, Index, LogId),
                  ?assertEqual(ExpectedLocation, Location)
                end, Expects),
  ?assertEqual(4, gululog_idx:get_latest_logid(Index)),
  ok.

%% @doc Delete log entry from index cache, read idx file to locate.
t_read_file_entry_to_locate({init, Config}) ->
  Dir = ?config(dir),
  Events = [ {append, 0, 1}
           , {append, 1, 10}
           , {append, 2, 20}
           , {append, 3, 32}
           ],
  Idx = init_idx(Dir, Events),
  ok = gululog_idx:flush_close(Idx),
  Config;
t_read_file_entry_to_locate({'end', _Config}) -> ok;
t_read_file_entry_to_locate(Config) when is_list(Config) ->
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

%% @doc Delete log entry from index cache, truncate after the deleted logid
%% verify the truncated latest is correct
%% @end
t_truncated_latest_logid({init, Config}) ->
  Dir = ?config(dir),
  Events = [ {append, 0, 1}
           , {append, 1, 10}
           , {append, 2, 20}
           ],
  Idx = init_idx(Dir, Events),
  ok = gululog_idx:flush_close(Idx),
  Config;
t_truncated_latest_logid({'end', _Config}) -> ok;
t_truncated_latest_logid(Config) when is_list(Config) ->
  Dir = ?config(dir),
  Idx0 = gululog_idx:init(Dir),
  ?assertEqual(2, gululog_idx:get_latest_logid(Idx0)),
  Idx1 = gululog_idx:delete_from_cache(Idx0, 1),
  {Idx, _} = gululog_idx:truncate(Dir, Idx1, 0, 2, ?undef),
  ?assertEqual(1, gululog_idx:get_latest_logid(Idx)),
  ok.

%%%_* Help functions ===========================================================

ts() -> gululog_dt:os_sec().

init_idx(Dir, Events) -> init_idx(Dir, gululog_idx:init(Dir), Events).

init_idx(_Dir, Idx, []) -> Idx;
init_idx(Dir, Idx, [{append, LogId, Pos} | Events]) ->
  NewIdx = gululog_idx:append(Idx, LogId, Pos, ts()),
  init_idx(Dir, NewIdx, Events);
init_idx(Dir, Idx, [{switch_append, LogId, Pos} | Events]) ->
  NewIdx = gululog_idx:switch_append(Dir, Idx, LogId, Pos, ts()),
  init_idx(Dir, NewIdx, Events).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
