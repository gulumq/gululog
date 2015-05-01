-module(gululog_repair_SUITE).

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
-export([ t_nothing_to_repair/1
        ]).

-define(config(KEY), proplists:get_value(KEY, Config)).

suite() -> [{timetrap, {seconds,30}}].

init_per_suite(Config) ->
  {ok, Cwd} = file:get_cwd(),
  Dir = filename:join(Cwd, "repair-suite"),
  [{dir, Dir} | Config].

end_per_suite(_Config) ->
  ok.

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

%% @doc Test a basic reader & writer cursor work flow.
t_nothing_to_repair({init, Config}) -> Config;
t_nothing_to_repair({'end', _Config}) -> ok;
t_nothing_to_repair(Config) ->
  Dir = ?config(dir),
  ?assertEqual({ok, []}, gululog_repair:repair_dir(Dir)).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
