
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
        , t_open_exists/1
        ]).

-define(config(KEY), proplists:get_value(KEY, Config)).

suite() -> [{timetrap, {seconds,30}}].

init_per_suite(Config) ->
  {ok, Cwd} = file:get_cwd(),
  Dir = filename:join(Cwd, "cursor-suite"),
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
  W_Cursor_ = gululog_w_cur:open(Dir, 0),
  W_Cursor0 = gululog_w_cur:append(W_Cursor_, 0, <<"header0">>, <<"body0">>),
  W_Cursor1 = gululog_w_cur:append(W_Cursor0, 1, <<"header1">>, <<"body1">>),
  ok = gululog_w_cur:flush_close(W_Cursor1),
  R_Cursor_ = gululog_r_cur:open(Dir, 0),
  {R_Cursor0, Log0} = gululog_r_cur:read(R_Cursor_, _Options = []),
  ?assertMatch(#gululog{ header = <<"header0">>
                       , body   = <<"body0">>}, Log0),
  {R_Cursor1, Log1} = gululog_r_cur:read(R_Cursor0, [skip_body]),
  ?assertMatch(#gululog{ header = <<"header1">>
                       , body   = undefined}, Log1),
  ?assertEqual(eof, gululog_r_cur:read(R_Cursor1, [])),
  ok = gululog_r_cur:close(R_Cursor1),
  ok.

t_open_exists({init, Config}) ->
  Dir = ?config(dir),
  W_Cursor_ = gululog_w_cur:open(Dir, 0),
  W_Cursor0 = gululog_w_cur:append(W_Cursor_, 0, <<"header0">>, <<"body0">>),
  W_Cursor1 = gululog_w_cur:append(W_Cursor0, 1, <<"header1">>, <<"body1">>),
  W_Cursor2 = gululog_w_cur:switch_append(Dir, W_Cursor1, 2, <<"header2">>, <<"body2">>),
  ok = gululog_w_cur:flush_close(W_Cursor2),
  Config;
t_open_exists({'end', _Config}) -> ok;
t_open_exists(Config) ->
  Dir = ?config(dir),
  W_Cursor2 = gululog_w_cur:open(Dir, 0),
  _ = gululog_w_cur:append(W_Cursor2, 3, <<"header3">>, <<"body3">>),
  timer:sleep(timer:seconds(2)),

  R_Cursor_ = gululog_r_cur:open(Dir, 0),
  {R_Cursor0, Log0} = gululog_r_cur:read(R_Cursor_, []),
  {R_Cursor1, Log1} = gululog_r_cur:read(R_Cursor0, [skip_body]),
  ?assertMatch(#gululog{ header = <<"header0">>
                       , body   = <<"body0">>}, Log0),
  ?assertMatch(#gululog{ header = <<"header1">>
                       , body   = undefined}, Log1),
  ?assertEqual(eof, gululog_r_cur:read(R_Cursor1, [])),
  ok = gululog_r_cur:close(R_Cursor1),

  R_Cursor2_ = gululog_r_cur:open(Dir, 2),
  {R_Cursor2, Log2} = gululog_r_cur:read(R_Cursor2_, []),
  {R_Cursor3, Log3} = gululog_r_cur:read(R_Cursor2, []),
  ?assertMatch(#gululog{ header = <<"header2">>
                       , body   = <<"body2">>}, Log2),
  ?assertMatch(#gululog{ header = <<"header3">>
                       , body   = <<"body3">>}, Log3),
  ?assertEqual(eof, gululog_r_cur:read(R_Cursor3, [])),
  ok.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
