%% @doc Repair log index/segment integrity.
%% NB: This module is called by gululog_writer before it starts reading
%% the index files and open the lastest segment file for appending new logs.

-module(gululog_repair).

-export([ repair_dir/1
        , repair_dir/2
        ]).

%%%*_ MACROS and SPECS =========================================================

-include("gululog_priv.hrl").

-type cache() :: gululog_idx:cache().
-type r_cursor() :: gululog_r_cur:cursor().
-type tag() :: repair_tag().

%%%*_ API FUNCTIONS ============================================================

%% @doc Repair log integrity in the given dir.
%% There is no aotomicity when switching .idx and .seg files to new segments,
%% this function is to repair the data integrity in the given directory.
%% 1. move the un-paired ones from the log directory to backup directory.
%% 2. resect the corrupted segmentfile (file before resection is backed up).
%% @end
-spec repair_dir(dirname()) -> {ok, [{tag(), filename()}]} | no_return().
repair_dir(Dir) -> repair_dir(Dir, ?undef).

-spec repair_dir(dirname(), ?undef | dirname()) ->
        {ok, [{tag(), filename()}]} | no_return().
repair_dir(Dir, ?undef) ->
  BackupDir = filename:join(Dir, "repair-" ++ time_now_str()),
  repair_dir(Dir, BackupDir);
repair_dir(Dir, BackupDir) ->
  case filelib:is_dir(Dir) of
    true ->
      IdxFiles = gululog_name:wildcard_full_path_name_reversed(Dir, ?DOT_IDX),
      SegFiles = gululog_name:wildcard_full_path_name_reversed(Dir, ?DOT_SEG),
      {ok, BackedupFiles} = repair_dir(IdxFiles, SegFiles, BackupDir, []),
      {ok, RepairedFiles} = repair_seg(IdxFiles -- BackedupFiles,
                                       SegFiles -- BackedupFiles, Dir, BackupDir),
      {ok, [{?REPAIR_BACKEDUP, F} || F <- BackedupFiles] ++ RepairedFiles};
    false ->
      %% nonexist dir, do nothing
      {ok, []}
  end.

%%%*_ PRIVATE FUNCTIONS ========================================================

%% @private Repair log integrity in the given dir.
-spec repair_dir([filename()], [filename()], dirname(), [filename()]) ->
        {ok, [filename()]} | no_return().
repair_dir([], [], _BackupDir, BackedupFiles) ->
  {ok, BackedupFiles};
repair_dir([IdxFile | IdxFiles], [], BackupDir, BackedupFiles) ->
  ok = move_idx_file(IdxFile, BackupDir),
  repair_dir(IdxFiles, [], BackupDir, [IdxFile | BackedupFiles]);
repair_dir([], [SegFile | SegFiles], BackupDir, BackedupFiles) ->
  ok = move_seg_file(SegFile, BackupDir),
  repair_dir([], SegFiles, BackupDir, [SegFile | BackedupFiles]);
repair_dir([IdxFile | IdxFiles], [SegFile | SegFiles], BackupDir, BackedupFiles) ->
  IdxSegId = gululog_name:to_segid(IdxFile),
  SegSegId = gululog_name:to_segid(SegFile),
  case IdxSegId =:= SegSegId of
    true  ->
      repair_dir(IdxFiles, SegFiles, BackupDir, BackedupFiles);
    false when IdxSegId > SegSegId ->
      %% index file is ahead of segment file
      ok = move_idx_file(IdxFile, BackupDir),
      repair_dir(IdxFiles, [SegFile | SegFiles], BackupDir,
                 [IdxFile | BackedupFiles]);
    false when IdxSegId < SegSegId ->
      %% segment file is ahead of index file
      ok = move_seg_file(SegFile, BackupDir),
      repair_dir([IdxFile | IdxFiles], SegFiles, BackupDir,
                 [SegFile | BackedupFiles])
  end.

%% @private Repair segment file.
%% Assuming that the index file is never corrupted.
%% In case there is a resection of corrupted segment tail,
%% an resection is done for the index file as well.
%% @end
-spec repair_seg([filename()], [filename()], dirname(), dirname()) ->
        {ok, [{tag(), filename()}]}.
repair_seg([], [], _Dir, _BackupDir) -> {ok, []};
repair_seg([IdxFile | _], [SegFile | _], Dir, BackupDir) ->
  IndexCache = gululog_idx:init_cache([IdxFile]),
  try
    repair_seg(IndexCache, IdxFile, SegFile, Dir, BackupDir)
  after
    gululog_idx:close_cache(IndexCache)
  end.

repair_seg(IndexCache, IdxFile, SegFile, Dir, BackupDir) ->
  LatestLogId = gululog_idx:get_latest_logid(IndexCache),
  SegId = gululog_name:to_segid(IdxFile),
  RCursor = gululog_r_cur:open(Dir, SegId),
  case integral_pos(SegId, IndexCache, LatestLogId, RCursor) of
    bof ->
      %% the whole segment is empty or corrupted
      ok = move_idx_file(IdxFile, BackupDir),
      ok = move_seg_file(SegFile, BackupDir),
      {ok, [{?REPAIR_BACKEDUP, IdxFile}, {?REPAIR_BACKEDUP, SegFile}]};
    LatestLogId ->
      %% Latest log is integral, nothing to repair
      true = is_integer(LatestLogId), %% assert
      {ok, []};
    LogId ->
      true = is_integer(LatestLogId),
      true = is_integer(LogId),
      true = (LogId < LatestLogId),
      IdxBytes = gululog_idx:get_next_position_in_index_file(IdxFile, LogId),
      {SegId, SegBytes} = gululog_idx:locate_in_cache(IndexCache, LogId + 1),
      ok = truncate_idx_file(IdxFile, IdxBytes, BackupDir),
      ok = truncate_seg_file(IdxFile, SegBytes, BackupDir),
      {ok, [{?REPAIR_RESECTED, IdxFile}, {?REPAIR_RESECTED, SegFile}]}
  end.

%% @private Scan from the latest log entry until integrity is found.
%% Return bof if: 1) the index file is empty
%%                2) the segment file is empty
%%                3) the entire segment file is corrupted
%% otherwise return the latest integral logid.
%% @end
-spec integral_pos(segid(), cache(), false | logid(), empty | r_cursor()) -> logid().
integral_pos(_SegId, _IndexCache, _LogId, empty) ->
  %% seg file is empty
  bof;
integral_pos(_SegId, _IndexCache, false, RCursor) ->
  %% index file has no entry
  ok = gululog_r_cur:close(RCursor),
  bof;
integral_pos(SegId, _IndexCache, LogId, RCursor) when LogId < SegId ->
  %% Scaned all the way to the beginning of the segment
  ok = gululog_r_cur:close(RCursor),
  bof;
integral_pos(SegId, IndexCache, LogId, RCursor0) ->
  {SegId, Pos} = gululog_idx:locate_in_cache(IndexCache, LogId),
  RCursor1 = gululog_r_cur:reposition(RCursor0, Pos),
  case try_read_log(RCursor1) of
    {ok, RCursor2} ->
      %% no corruption, return current logid being scaned
      ok = gululog_r_cur:close(RCursor2),
      LogId;
    {error, _Reason} ->
      %% not found or corrupted, keep scaning ealier logs
      integral_pos(SegId, IndexCache, LogId - 1, RCursor1)
  end.

%% @private Try read a log entry from reader cursor.
%% return new cursor if succeeded, otherwise error with reason.
%% @end
-spec try_read_log(r_cursor()) -> {ok, r_cursor()} | {error, Reason}
        when Reason :: not_found
                     | corrupted_header
                     | corrupted_body.
try_read_log(RCursor) ->
  try gululog_r_cur:read(RCursor, []) of
    {RCursor2, _Log}         -> {ok, RCursor2};
    eof                      -> {error, not_found}
  catch
    throw : corrupted_header -> {error, corrupted_header};
    throw : corrupted_body   -> {error, corrupted_header}
  end.

%% @private Move idx file to target dir.
-spec move_idx_file(filename(), dirname()) -> ok.
move_idx_file(IdxFile, TargetDir) -> move_file(IdxFile, TargetDir, ?DOT_IDX).

%% @private Move seg file to target dir.
-spec move_seg_file(filename(), dirname()) -> ok.
move_seg_file(SegFile, TargetDir) -> move_file(SegFile, TargetDir, ?DOT_SEG).

%% @private Copy the file to another name and remove the source file.
-spec move_file(filename(), dirname(), string()) -> ok.
move_file(Source, TargetDir, Suffix) ->
  ok = copy_file(Source, TargetDir, Suffix),
  ok = file:delete(Source).

%% @private Copy .idx or .seg file to the given directory.
-spec copy_file(filename(), dirname(), string()) -> ok.
copy_file(Source, TargetDir, Suffix) ->
  SegId = gululog_name:to_segid(Source),
  TargetFile = gululog_name:from_segid(TargetDir, SegId) ++ Suffix,
  ok = filelib:ensure_dir(TargetFile),
  {ok, _} = file:copy(Source, TargetFile),
  ok.

%% @private Truncate idx file from the given position.
-spec truncate_idx_file(filename(), position(), dirname()) -> ok.
truncate_idx_file(IdxFile, Position, BackupDir) ->
  truncate_file(IdxFile, Position, BackupDir, ?DOT_IDX).

%% @private Truncate seg file from the given position.
-spec truncate_seg_file(filename(), position(), dirname()) -> ok.
truncate_seg_file(SegFile, Position, BackupDir) ->
  truncate_file(SegFile, Position, BackupDir, ?DOT_SEG).

%% @private Backup the original file, then truncate at the given position.
-spec truncate_file(filename(), position(), dirname(), string()) -> ok.
truncate_file(FileName, Position, BackupDir, Suffix) ->
  ok = copy_file(FileName, BackupDir, Suffix),
  %% open with 'read' mode, otherwise truncate does not work
  {ok, Fd} = file:open(FileName, [write, read, raw, binary]),
  try
    {ok, Position} = file:position(Fd, Position),
    ok = file:truncate(Fd)
  after
    file:close(Fd)
  end.

%% @private make a timestamp
-spec time_now_str() -> string().
time_now_str() ->
  gululog_dt:sec_to_utc_str_compact(gululog_dt:os_sec()).

%%%*_ TESTS ====================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

move_test_() ->
  {ok, Cwd} = file:get_cwd(),
  Dir = filename:join(Cwd, "move-test"),
  _ = file:del_dir(Dir),
  ok = filelib:ensure_dir(filename:join(Dir, "foo")),
  FileBaseName = gululog_name:from_segid(Dir, 1),
  BackupDir = filename:join(Dir, "backup"),
  BackupFileBaseName = gululog_name:from_segid(BackupDir, 1),
  IdxFile = FileBaseName ++ ?DOT_IDX,
  SegFile = FileBaseName ++ ?DOT_SEG,
  BackupIdxFile = BackupFileBaseName ++ ?DOT_IDX,
  BackupSegFile = BackupFileBaseName ++ ?DOT_SEG,
  ok = file:write_file(IdxFile, <<"idx">>, [binary]),
  ok = file:write_file(SegFile, <<"seg">>, [binary]),
  [ { "move idx file"
    , fun() ->
        ok = move_idx_file(IdxFile, BackupDir),
        ?assertEqual({ok, <<"idx">>}, file:read_file(BackupIdxFile)),
        ?assertEqual(false, filelib:is_file(IdxFile))
      end
    }
  , { "move seg file"
    , fun() ->
        ok = move_seg_file(SegFile, BackupDir),
        ?assertEqual({ok, <<"seg">>}, file:read_file(BackupSegFile)),
        ?assertEqual(false, filelib:is_file(SegFile))
      end
    }
  ].

truncate_test_() ->
  {ok, Cwd} = file:get_cwd(),
  Dir = filename:join(Cwd, "truncate-test"),
  _ = file:del_dir(Dir),
  ok = filelib:ensure_dir(filename:join(Dir, "foo")),
  FileBaseName = gululog_name:from_segid(Dir, 1),
  BackupDir = filename:join(Dir, "backup"),
  BackupFileBaseName = gululog_name:from_segid(BackupDir, 1),
  IdxFile = FileBaseName ++ ?DOT_IDX,
  SegFile = FileBaseName ++ ?DOT_SEG,
  BackupIdxFile = BackupFileBaseName ++ ?DOT_IDX,
  BackupSegFile = BackupFileBaseName ++ ?DOT_SEG,
  ok = file:write_file(IdxFile, <<"0123456789">>, [binary]),
  ok = file:write_file(SegFile, <<"0123456789">>, [binary]),
  [ { "truncate idx file"
    , fun() ->
        ok = truncate_idx_file(IdxFile, 1, BackupDir),
        ?assertEqual({ok, <<"0">>},          file:read_file(IdxFile)),
        ?assertEqual({ok, <<"0123456789">>}, file:read_file(BackupIdxFile))
      end
    }
  , { "truncate seg file"
    , fun() ->
        ok = truncate_seg_file(SegFile, 10, BackupDir),
        ?assertEqual({ok, <<"0123456789">>}, file:read_file(SegFile)),
        ?assertEqual({ok, <<"0123456789">>}, file:read_file(BackupSegFile))
      end
    }
  ].

-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
