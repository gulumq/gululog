-module(gululog_repair_SUITE).

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
-export([ t_nothing_to_repair/1
        , t_backup_seg/1
        , t_backup_idx/1
        , t_truncate_corrupted_1st_log_body/1
        , t_truncate_index_ahead/1
        , t_truncate_seg_ahead/1
        , t_empty_seg_file/1
        , t_empty_idx_file/1
        , t_empty_idx_and_seg_file/1
        ]).

-define(config(KEY), proplists:get_value(KEY, Config)).

suite() -> [{timetrap, {seconds,30}}].

init_per_suite(Config) ->
  {ok, Cwd} = file:get_cwd(),
  Dir = filename:join(Cwd, "repair-suite"),
  %% name has to be "backup" to make gululog_test_lib:cleanup/1 work
  BackupDir = filename:join(Dir, "backup"),
  [{dir, Dir}, {backup_dir, BackupDir} | Config].

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
  ?assertEqual({ok, []}, gululog_repair:repair_dir(Dir, ?undef)).


%% @doc No index file paired with a segment file.
%% Remove (backup) segment file
%% @end
t_backup_seg({init, Config}) ->
  Dir = ?config(dir),
  %% add a new segment file but no index file
  Cur = gululog_w_cur:open(Dir, 0),
  ok = gululog_w_cur:flush_close(Cur),
  Config;
t_backup_seg({'end', _Config}) -> ok;
t_backup_seg(Config) when is_list(Config) ->
  Dir = ?config(dir),
  BackupDir = ?config(backup_dir),
  {ok, RepairedFiles} = gululog_repair:repair_dir(Dir, BackupDir),
  SegFile = gululog_name:mk_seg_name(Dir, 0),
  ?assertEqual([{?OP_DELETED, SegFile}], RepairedFiles),
  BackupFile = gululog_name:mk_seg_name(BackupDir, 0),
  ?assertEqual(true, filelib:is_file(BackupFile)).

%% @doc No segment file paired with a index file.
%% Remove (backup) index file.
%% @end
t_backup_idx({init, Config}) ->
  Dir = ?config(dir),
  %% create some files
  Topic0 = gululog_topic:init(Dir, []),
  Topic1 = gululog_topic:append(Topic0, <<"header">>, <<"body">>),
  ok = gululog_topic:close(Topic1),
  %% add a new segment file but no index file
  Idx0 = gululog_idx:init(Dir, 0, []),
  Idx = gululog_idx:switch(Dir, Idx0, 1),
  ok = gululog_idx:flush_close(Idx),
  Config;
t_backup_idx({'end', _Config}) -> ok;
t_backup_idx(Config) when is_list(Config) ->
  Dir = ?config(dir),
  BackupDir = ?config(backup_dir),
  {ok, RepairedFiles} = gululog_repair:repair_dir(Dir, BackupDir),
  Idx0File = gululog_name:mk_idx_name(Dir, 0),
  Seg0File = gululog_name:mk_seg_name(Dir, 0),
  Idx1File = gululog_name:mk_idx_name(Dir, 1),
  ?assertEqual([{?OP_DELETED, Idx1File}], RepairedFiles),
  BackupFile = gululog_name:mk_idx_name(BackupDir, 1),
  %% assert segment 0 is untouched
  ?assertEqual(true, filelib:is_file(Idx0File)),
  ?assertEqual(true, filelib:is_file(Seg0File)),
  %% assert segment 1 is backedup
  ?assertEqual(true, filelib:is_file(BackupFile)).

%% @doc The viery first log entry, truncating it would cause removal.
%% of both index and segment file
%% @end
t_truncate_corrupted_1st_log_body({init, Config}) ->
  Dir = ?config(dir),
  SegId = 0,
  %% write a log
  Topic0 = gululog_topic:init(Dir, [{segMB, 1}]),
  Topic1 = gululog_topic:append(Topic0, <<"header">>, <<"body-ok">>),
  ok = gululog_topic:close(Topic1),
  %% corrupt the last log body
  SegFile = gululog_name:mk_seg_name(Dir, SegId),
  {ok, Fd} = file:open(SegFile, [write, read, binary, raw]),
  {ok, Pos0} = file:position(Fd, eof),
  {ok, _} = file:position(Fd, Pos0 - 2),
  ok = file:write(Fd, <<"nok">>),
  Config;
t_truncate_corrupted_1st_log_body({'end', _Config}) -> ok;
t_truncate_corrupted_1st_log_body(Config) when is_list(Config) ->
  Dir = ?config(dir),
  BackupDir = ?config(backup_dir),
  {ok, RepairedFiles} = gululog_repair:repair_dir(Dir, BackupDir),
  Idx0File = gululog_name:mk_idx_name(Dir, 0),
  Seg0File = gululog_name:mk_seg_name(Dir, 0),
  Idx0BackupFile = gululog_name:mk_idx_name(BackupDir, 0),
  Seg0BackupFile = gululog_name:mk_seg_name(BackupDir, 0),
  ?assertEqual([{?OP_DELETED, Idx0File},
                {?OP_DELETED, Seg0File}], RepairedFiles),
  %% assert segment 0 files are removed
  ?assertEqual(false, filelib:is_file(Idx0File)),
  ?assertEqual(false, filelib:is_file(Seg0File)),
  %% assert segment 0 files are backedup
  ?assertEqual(true, filelib:is_file(Idx0BackupFile)),
  ?assertEqual(true, filelib:is_file(Seg0BackupFile)),
  ok.

%% @doc Index entries are ahead of segment entry
%% expecting the index entries ahead are cut off when repair
%% @end
t_truncate_index_ahead({init, Config}) ->
  Dir = ?config(dir),
  %% write a log
  Topic0 = gululog_topic:init(Dir, [{segMB, 1}]),
  Topic1 = gululog_topic:append(Topic0, <<"header">>, <<"body">>),
  ok = gululog_topic:close(Topic1),
  %% write ahead some index entries.
  Idx0 = gululog_idx:init(Dir, 0, []),
  Idx1 = gululog_idx:append(Idx0, 1, 100, gululog_dt:os_sec()),
  Idx2 = gululog_idx:append(Idx1, 2, 2000, gululog_dt:os_sec()),
  ok = gululog_idx:flush_close(Idx2),
  Config;
t_truncate_index_ahead({'end', _Config}) -> ok;
t_truncate_index_ahead(Config) when is_list(Config) ->
  Dir = ?config(dir),
  BackupDir = ?config(backup_dir),
  {ok, RepairedFiles} = gululog_repair:repair_dir(Dir, BackupDir),
  Idx0File = gululog_name:mk_idx_name(Dir, 0),
  Seg0File = gululog_name:mk_seg_name(Dir, 0),
  ?assertEqual([{?OP_TRUNCATED, Idx0File}], RepairedFiles),
  Idx0BackupFile = gululog_name:mk_idx_name(BackupDir, 0),
  Seg0BackupFile = gululog_name:mk_seg_name(BackupDir, 0),
  %% assert segment 0 files are still there
  ?assertEqual(true, filelib:is_file(Idx0File)),
  ?assertEqual(true, filelib:is_file(Seg0File)),
  %% assert segment 0 files are backedup
  ?assertEqual(true, filelib:is_file(Idx0BackupFile)),
  ?assertEqual(false, filelib:is_file(Seg0BackupFile)),
  %% initialize index for sanity check.
  Idx = gululog_idx:init(Dir, 0, []),
  ?assertEqual(0, gululog_idx:get_latest_logid(Idx)),
  ok.

%% @doc Log entries in segment file is ahead of index.
%% expecting the log entries ahead are cut off when repair
%% @end
t_truncate_seg_ahead({init, Config}) ->
  Dir = ?config(dir),
  %% write a log
  Topic0 = gululog_topic:init(Dir, [{segMB, 1}]),
  Topic1 = gululog_topic:append(Topic0, <<"header0">>, <<"body0">>),
  ok = gululog_topic:close(Topic1),
  %% write ahead some log entries in seg file
  Cur0 = gululog_w_cur:open(Dir, 0),
  Cur1 = gululog_w_cur:append(Cur0, 1, <<"header1">>, <<"body1">>),
  Cur2 = gululog_w_cur:append(Cur1, 1, <<"header2">>, <<"body2">>),
  gululog_w_cur:flush_close(Cur2),
  Config;
t_truncate_seg_ahead({'end', _Config}) -> ok;
t_truncate_seg_ahead(Config) when is_list(Config) ->
  Dir = ?config(dir),
  BackupDir = ?config(backup_dir),
  {ok, RepairedFiles} = gululog_repair:repair_dir(Dir, BackupDir),
  Idx0File = gululog_name:mk_idx_name(Dir, 0),
  Seg0File = gululog_name:mk_seg_name(Dir, 0),
  ?assertEqual([{?OP_TRUNCATED, Seg0File}], RepairedFiles),
  Idx0BackupFile = gululog_name:mk_idx_name(BackupDir, 0),
  Seg0BackupFile = gululog_name:mk_seg_name(BackupDir, 0),
  %% assert segment 0 files are still there
  ?assertEqual(true, filelib:is_file(Idx0File)),
  ?assertEqual(true, filelib:is_file(Seg0File)),
  %% assert segment 0 files are backedup
  ?assertEqual(false, filelib:is_file(Idx0BackupFile)),
  ?assertEqual(true, filelib:is_file(Seg0BackupFile)),
  %% initialize index for sanity check.
  Cur0 = gululog_r_cur:open(Dir, 0),
  {Cur1, Log0} = gululog_r_cur:read(Cur0),
  ?assertMatch(#gululog{ header = <<"header0">>
                       , body   = <<"body0">>
                       }, Log0),
  ?assertEqual(eof, gululog_r_cur:read(Cur1)),
  ok = gululog_r_cur:close(Cur1),
  ok.

t_empty_seg_file({init, Config}) ->
  Dir = ?config(dir),
  %% write a log
  Topic0 = gululog_topic:init(Dir, [{segMB, 1}]),
  OneMB_Body = list_to_binary(lists:duplicate(1000000, $0)),
  Topic1 = gululog_topic:append(Topic0, <<"header0">>, OneMB_Body),
  ok = gululog_topic:close(Topic1),
  %% switch to new segment, append index entry
  Idx0 = gululog_idx:init(Dir, 0, []),
  Idx1 = gululog_idx:switch_append(Dir, Idx0, 1, 1, gululog_dt:os_sec()),
  ok = gululog_idx:flush_close(Idx1),
  %% make an empty segment file
  ok = file:write_file(gululog_name:mk_seg_name(Dir, 1), <<>>),
  Config;
t_empty_seg_file({'end', _Config}) -> ok;
t_empty_seg_file(Config) when is_list(Config) ->
  Dir = ?config(dir),
  BackupDir = ?config(backup_dir),
  {ok, RepairedFiles} = gululog_repair:repair_dir(Dir, BackupDir),
  Idx0File = gululog_name:mk_idx_name(Dir, 0),
  Idx1File = gululog_name:mk_idx_name(Dir, 1),
  Seg0File = gululog_name:mk_seg_name(Dir, 0),
  Seg1File = gululog_name:mk_seg_name(Dir, 1),
  ?assertEqual([{?OP_DELETED, Idx1File},
                {?OP_DELETED, Seg1File}], RepairedFiles),
  Idx1BackupFile = gululog_name:mk_idx_name(BackupDir, 1),
  Seg1BackupFile = gululog_name:mk_seg_name(BackupDir, 1),
  %% assert segment 0 files are still there
  ?assertEqual(true, filelib:is_file(Idx0File)),
  ?assertEqual(true, filelib:is_file(Seg0File)),
  %% assert segment 1 files are removed
  ?assertEqual(false, filelib:is_file(Idx1File)),
  ?assertEqual(false, filelib:is_file(Seg1File)),
  %% assert segment 1 files are backedup
  ?assertEqual(true, filelib:is_file(Idx1BackupFile)),
  ?assertEqual(true, filelib:is_file(Seg1BackupFile)),
  ok.

t_empty_idx_file({init, Config}) ->
  Dir = ?config(dir),
  %% write a log
  Topic0 = gululog_topic:init(Dir, [{segMB, 1}]),
  Topic1 = gululog_topic:append(Topic0, <<"header0">>, <<"body0">>),
  ok = gululog_topic:close(Topic1),
  %% append a new log entry to segment file
  Cur0 = gululog_w_cur:open(Dir, 0),
  Cur1 = gululog_w_cur:switch_append(Dir, Cur0, 1, <<"h1">>, <<"b1">>),
  ok = gululog_w_cur:flush_close(Cur1),
  %% make an empty idx file
  ok = file:write_file(gululog_name:mk_idx_name(Dir, 1), <<>>),
  Config;
t_empty_idx_file({'end', _Config}) -> ok;
t_empty_idx_file(Config) when is_list(Config) ->
  Dir = ?config(dir),
  BackupDir = ?config(backup_dir),
  {ok, RepairedFiles} = gululog_repair:repair_dir(Dir, BackupDir),
  Idx0File = gululog_name:mk_idx_name(Dir, 0),
  Idx1File = gululog_name:mk_idx_name(Dir, 1),
  Seg0File = gululog_name:mk_seg_name(Dir, 0),
  Seg1File = gululog_name:mk_seg_name(Dir, 1),
  ?assertEqual([{?OP_DELETED, Idx1File},
                {?OP_DELETED, Seg1File}], RepairedFiles),
  Idx1BackupFile = gululog_name:mk_idx_name(BackupDir, 1),
  Seg1BackupFile = gululog_name:mk_seg_name(BackupDir, 1),
  %% assert segment 0 files are still there
  ?assertEqual(true, filelib:is_file(Idx0File)),
  ?assertEqual(true, filelib:is_file(Seg0File)),
  %% assert segment 1 files are removed
  ?assertEqual(false, filelib:is_file(Idx1File)),
  ?assertEqual(false, filelib:is_file(Seg1File)),
  %% assert segment 1 files are backedup
  ?assertEqual(true, filelib:is_file(Idx1BackupFile)),
  ?assertEqual(true, filelib:is_file(Seg1BackupFile)),
  ok.

%% @doc Both the latest idx and seg file have only a version byte written.
t_empty_idx_and_seg_file({init, Config}) ->
  Dir = ?config(dir),
  %% write a log
  Topic0 = gululog_topic:init(Dir, [{segMB, 1}]),
  Topic1 = gululog_topic:append(Topic0, <<"header0">>, <<"body0">>),
  Topic2 = gululog_topic:force_switch(Topic1),
  Topic3 = gululog_topic:force_switch(Topic2), %% switch twice should be fine
  ok = gululog_topic:close(Topic3),
  Config;
t_empty_idx_and_seg_file({'end', _Config}) -> ok;
t_empty_idx_and_seg_file(Config) when is_list(Config) ->
  Dir = ?config(dir),
  {ok, RepairedFiles} = gululog_repair:repair_dir(Dir, ?undef),
  Idx0File = gululog_name:mk_idx_name(Dir, 0),
  Idx1File = gululog_name:mk_idx_name(Dir, 1),
  Seg0File = gululog_name:mk_seg_name(Dir, 0),
  Seg1File = gululog_name:mk_seg_name(Dir, 1),
  ?assertEqual([{?OP_DELETED, Idx1File},
                {?OP_DELETED, Seg1File}], RepairedFiles),
  %% assert segment 0 files are still there
  ?assertEqual(true, filelib:is_file(Idx0File)),
  ?assertEqual(true, filelib:is_file(Seg0File)),
  %% assert segment 1 files are removed
  ?assertEqual(false, filelib:is_file(Idx1File)),
  ?assertEqual(false, filelib:is_file(Seg1File)),
  ok.

%%%*_ PRIVATE FUNCTIONS ========================================================

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
