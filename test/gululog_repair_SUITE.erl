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
        , t_backup_seg/1
        , t_backup_idx/1
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
t_nothing_to_repair(Config) when is_list(Config) ->
  Dir = ?config(dir),
  ?assertEqual({ok, []}, gululog_repair:repair_dir(Dir)).


%% @doc No index file paired with a segment file.
t_backup_seg({init, Config}) ->
  Dir = ?config(dir),
  %% add a new segment file but no index file
  Cur = gululog_w_cur:open(Dir),
  ok = gululog_w_cur:flush_close(Cur),
  Config;
t_backup_seg({'end', _Config}) -> ok;
t_backup_seg(Config) when is_list(Config) ->
  Dir = ?config(dir),
  BackupDir = filename:join(Dir, "backup"),
  {ok, RepairedFiles} = gululog_repair:repair_dir(Dir, BackupDir),
  SegFile = gululog_name:mk_seg_name(Dir, 0),
  ?assertEqual([{?REPAIR_BACKEDUP, SegFile}], RepairedFiles),
  BackupFile = gululog_name:mk_seg_name(BackupDir, 0),
  ?assertEqual(true, filelib:is_file(BackupFile)).

t_backup_idx({init, Config}) ->
  Dir = ?config(dir),
  %% create some files
  Topic0 = gululog_topic:init(Dir, []),
  Topic1 = gululog_topic:append(Topic0, <<"header">>, <<"body">>),
  ok = gululog_topic:close(Topic1),
  %% add a new segment file but no index file
  Idx0 = gululog_idx:init(Dir),
  Idx = gululog_idx:switch(Dir, Idx0, 1),
  ok = gululog_idx:flush_close(Idx),
  Config;
t_backup_idx({'end', _Config}) -> ok;
t_backup_idx(Config) when is_list(Config) ->
  Dir = ?config(dir),
  BackupDir = filename:join(Dir, "backup"),
  {ok, RepairedFiles} = gululog_repair:repair_dir(Dir, BackupDir),
  Idx0File = gululog_name:mk_idx_name(Dir, 0),
  Seg0File = gululog_name:mk_seg_name(Dir, 0),
  Idx1File = gululog_name:mk_idx_name(Dir, 1),
  ?assertEqual([{?REPAIR_BACKEDUP, Idx1File}], RepairedFiles),
  BackupFile = gululog_name:mk_idx_name(BackupDir, 1),
  ?assertEqual(true, filelib:is_file(Idx0File)),
  ?assertEqual(true, filelib:is_file(Seg0File)),
  ?assertEqual(true, filelib:is_file(BackupFile)).

%%%*_ PRIVATE FUNCTIONS ========================================================

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
