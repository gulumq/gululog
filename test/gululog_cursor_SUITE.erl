
-module(gululog_cursor_SUITE).

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
        ]).

-define(config(KEY), proplists:get_value(KEY, Config)).

suite() -> [{timetrap, {seconds,30}}].

init_per_suite(Config) ->
  Dir = filename:join([".", "test_data"]),
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
t_basic_flow({init, Config}) -> Config;
t_basic_flow({'end', _Config}) -> ok;
t_basic_flow(Config) ->
  Dir = ?config(dir),
  W_Cursor0 = gululog_w_cur:open(Dir),
  W_Cursor1 = gululog_w_cur:append(W_Cursor0, _LogId = 1, <<"header1">>, <<"body1">>),
  W_Cursor2 = gululog_w_cur:append(W_Cursor1, _logId = 2, <<"header2">>, <<"body2">>),
  gululog_w_cur:close(W_Cursor2),
  R_Cursor0 = gululog_r_cur:open(Dir, 0),
  {R_Cursor1, Log1} = gululog_r_cur:read(R_Cursor0, _Options = []),
  ?assertMatch(#gululog{ header = <<"header1">>
                       , body   = <<"body1">>}, Log1),
  {R_Cursor2, Log2} = gululog_r_cur:read(R_Cursor1, [skip_body]),
  ?assertMatch(#gululog{ header = <<"header2">>
                       , body   = undefined}, Log2),
  gululog_r_cur:close(R_Cursor2),
  ok.


