%% @doc Repair log index/segment integrity.
%% NB: This module is called by gululog_writer before it starts reading
%% the index files and open the lastest segment file for appending new logs.

-module(gululog_repair).

-export([ repair_dir/1
        , repair_dir/2
        ]).

-export([ copy_file/2
        , maybe_truncate_file/3]).

%%%*_ MACROS and SPECS =========================================================

-include("gululog_priv.hrl").

-type cache() :: gululog_idx:cache().
-type r_cursor() :: gululog_r_cur:cursor().
-type tag() :: repair_tag().

%%%*_ API FUNCTIONS ============================================================

%% @doc Repair log integrity in the given dir.
%% There is no aotomicity when switching .idx and .seg files to new segments,
%% this function is to repair the data integrity in the given directory.
%% 1. remove (maybe backup) unpaired .idx and .seg fiels.
%% 2. resect the corrupted .idx and .seg tails
%%    (files are backed up before resection if backup dir is given).
%% @end
-spec repair_dir(dirname()) -> {ok, [{tag(), filename()}]} | no_return().
repair_dir(Dir) -> repair_dir(Dir, ?undef).

-spec repair_dir(dirname(), ?undef | dirname()) ->
        {ok, [{tag(), filename()}]} | no_return().
repair_dir(Dir, BackupDir) ->
  case filelib:is_dir(Dir) of
    true ->
      IdxFiles = gululog_name:wildcard_idx_name_reversed(Dir),
      SegFiles = gululog_name:wildcard_seg_name_reversed(Dir),
      RepairedFiles1 = repair_dir(IdxFiles, SegFiles, BackupDir),
      RemovedFiles = [F || {_ReapirTag, F} <- RepairedFiles1],
      RepairedFiles2 = repair_seg(IdxFiles -- RemovedFiles,
                                  SegFiles -- RemovedFiles,
                                  Dir, BackupDir),
      {ok, RepairedFiles1 ++ RepairedFiles2};
    false ->
      %% nonexist dir, do nothing
      {ok, []}
  end.

%%%*_ PRIVATE FUNCTIONS ========================================================

%% @private Repair log integrity in the given dir.
%% remove (backup if backup dir is given) unpaired index and segment files
%% @end
-spec repair_dir([filename()], [filename()], ?undef | dirname()) ->
        [{tag(), filename()}].
repair_dir(IdxFiles, SegFiles, BackupDir) ->
  ToSegIdFun = fun gululog_name:filename_to_segid/1,
  IdxSegIds = sets:from_list(lists:map(ToSegIdFun, IdxFiles)),
  SegSegIds = sets:from_list(lists:map(ToSegIdFun, SegFiles)),
  UnpairedIdxFiles =
    lists:filter(
      fun(IdxFile) ->
        not sets:is_element(ToSegIdFun(IdxFile), SegSegIds)
      end, IdxFiles),
  UnpairedSegFiles =
    lists:filter(
      fun(SegFile) ->
        not sets:is_element(ToSegIdFun(SegFile), IdxSegIds)
      end, SegFiles),
  lists:map(fun(FileName) -> remove_file(FileName, BackupDir) end,
            UnpairedIdxFiles ++ UnpairedSegFiles).

%% @private Repair segment file.
%% Assuming that the index file is never corrupted.
%% In case there is a resection of corrupted segment tail,
%% an resection is done for the index file as well.
%% @end
-spec repair_seg([filename()], [filename()], dirname(), ?undef | dirname()) ->
        [{tag(), filename()}].
repair_seg([], [], _Dir, _BackupDir) -> [];
repair_seg([IdxFile | _], [SegFile | _], Dir, BackupDir) ->
  IndexCache = gululog_idx:init_cache([IdxFile]),
  try
    repair_seg(IndexCache, IdxFile, SegFile, Dir, BackupDir)
  after
    gululog_idx:close_cache(IndexCache)
  end.

repair_seg(IndexCache, IdxFile, SegFile, Dir, BackupDir) ->
  LatestLogId = gululog_idx:get_latest_logid(IndexCache),
  SegId = gululog_name:filename_to_segid(IdxFile),
  RCursor = gululog_r_cur:open(Dir, SegId),
  case integral_pos(SegId, IndexCache, IdxFile, LatestLogId, RCursor) of
    bof ->
      %% the whole segment is empty or corrupted
      [remove_file(IdxFile, BackupDir), remove_file(SegFile, BackupDir)];
    {IdxPos, SegPos} ->
      IsIdxRepaired = maybe_truncate_file(IdxFile, IdxPos, BackupDir),
      IsSegRepaired = maybe_truncate_file(SegFile, SegPos, BackupDir),
      [{?REPAIR_RESECTED, IdxFile} || IsIdxRepaired] ++
      [{?REPAIR_RESECTED, SegFile} || IsSegRepaired]
  end.

%% @private Scan from the latest log entry until integrity is found.
%% Return bof if: 1) the index file is empty
%%                2) the segment file is empty
%%                3) the entire segment file is corrupted
%% otherwise return the latest integral positions in index and segment file.
%% @end
-spec integral_pos(segid(), cache(), filename(), false | logid(), empty | r_cursor()) ->
        bof | {IdxPos::position(), SegPos::position()}.
integral_pos(_SegId, _IndexCache, _IdxFile, _LogId, empty) ->
  %% seg file is empty
  bof;
integral_pos(_SegId, _IndexCache, _IdxFile, false, RCursor) ->
  %% index file has no entry
  ok = gululog_r_cur:close(RCursor),
  bof;
integral_pos(SegId, _IndexCache, _IdxFile, LogId, RCursor) when LogId < SegId ->
  %% Scaned all the way to the beginning of the segment
  ok = gululog_r_cur:close(RCursor),
  bof;
integral_pos(SegId, IndexCache, IdxFile, LogId, RCursor0) ->
  {SegId, Pos} = gululog_idx:locate_in_cache(IndexCache, LogId),
  RCursor1 = gululog_r_cur:reposition(RCursor0, Pos),
  case try_read_log(RCursor1) of
    {ok, RCursor2} ->
      %% no corruption, return current logid being scaned
      IdxPos = gululog_idx:get_position_in_index_file(IdxFile, LogId + 1),
      SegPos = gululog_r_cur:current_position(RCursor2),
      ok = gululog_r_cur:close(RCursor2),
      {IdxPos, SegPos};
    {error, _Reason} ->
      %% not found or corrupted, keep scaning ealier logs
      integral_pos(SegId, IndexCache, IdxFile, LogId - 1, RCursor1)
  end.

%% @private Try read a log entry from reader cursor.
%% return new cursor if succeeded, otherwise error with reason.
%% @end
-spec try_read_log(r_cursor()) -> {ok, r_cursor()} | {error, Reason}
        when Reason :: not_found
                     | bad_meta_size
                     | corrupted_meta
                     | corrupted_header
                     | corrupted_body.
try_read_log(RCursor) ->
  try gululog_r_cur:read(RCursor, []) of
    {RCursor2, _Log}         -> {ok, RCursor2};
    eof                      -> {error, not_found}
  catch
    throw : Reason           -> {error, Reason}
  end.

%% @private Copy the file to another name and remove the source file.
-spec remove_file(filename(), ?undef | dirname()) -> ok.
remove_file(FileName, ?undef) ->
  ok = file:delete(FileName),
  {?REPAIR_DELETED, FileName};
remove_file(FileName, TargetDir) ->
  ok = copy_file(FileName, TargetDir),
  ok = file:delete(FileName),
  {?REPAIR_BACKEDUP, FileName}.

%% @private Copy .idx or .seg file to the given directory.
-spec copy_file(filename(), dirname()) -> ok.
copy_file(Source, TargetDir) ->
  TargetFile = backup_filename(Source, TargetDir),
  ok = filelib:ensure_dir(TargetFile),
  {ok, _} = file:copy(Source, TargetFile),
  ok.

%% @private Backup the original file, then truncate at the given position.
-spec maybe_truncate_file(filename(), position(), ?undef | dirname()) -> boolean().
maybe_truncate_file(FileName, Position, BackupDir) ->
  %% open with 'read' mode, otherwise truncate does not work
  {ok, Fd} = file:open(FileName, [write, read, raw, binary]),
  try
    {ok, Size} = file:position(Fd, eof),
    true = (Position =< Size), %% assert
    case Position < Size of
      true ->
        [ok = copy_file(FileName, BackupDir) || BackupDir =/= ?undef],
        {ok, Position} = file:position(Fd, Position),
        ok = file:truncate(Fd),
        true;
      false ->
        false
    end
  after
    file:close(Fd)
  end.

%% @private Make backup file name.
-spec backup_filename(filename(), dirname()) -> filename().
backup_filename(SourceName, BackupDir) ->
  SegId = gululog_name:filename_to_segid(SourceName),
  case gululog_name:filename_to_type(SourceName) of
    ?FILE_TYPE_IDX -> gululog_name:mk_idx_name(BackupDir, SegId);
    ?FILE_TYPE_SEG -> gululog_name:mk_seg_name(BackupDir, SegId)
  end.

%%%*_ TESTS ====================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

move_test_() ->
  {ok, Cwd} = file:get_cwd(),
  Dir = filename:join(Cwd, "move-test"),
  _ = file:del_dir(Dir),
  ok = filelib:ensure_dir(filename:join(Dir, "foo")),
  SegId = 1,
  BackupDir = filename:join(Dir, "backup"),
  IdxFile = gululog_name:mk_idx_name(Dir, SegId),
  SegFile = gululog_name:mk_seg_name(Dir, SegId),
  BackupIdxFile = gululog_name:mk_idx_name(BackupDir, SegId),
  BackupSegFile = gululog_name:mk_seg_name(BackupDir, SegId),
  ok = file:write_file(IdxFile, <<"idx">>, [binary]),
  ok = file:write_file(SegFile, <<"seg">>, [binary]),
  [ { "move idx file"
    , fun() ->
        _ = remove_file(IdxFile, BackupDir),
        ?assertEqual({ok, <<"idx">>}, file:read_file(BackupIdxFile)),
        ?assertEqual(false, filelib:is_file(IdxFile))
      end
    }
  , { "move seg file"
    , fun() ->
        _ = remove_file(SegFile, ?undef),
        ?assertEqual(false, filelib:is_file(BackupSegFile)),
        ?assertEqual(false, filelib:is_file(SegFile))
      end
    }
  ].

truncate_test_() ->
  {ok, Cwd} = file:get_cwd(),
  Dir = filename:join(Cwd, "truncate-test"),
  _ = file:del_dir(Dir),
  ok = filelib:ensure_dir(filename:join(Dir, "foo")),
  SegId = 1,
  BackupDir = filename:join(Dir, "backup"),
  IdxFile = gululog_name:mk_idx_name(Dir, SegId),
  SegFile = gululog_name:mk_seg_name(Dir, SegId),
  BackupIdxFile = gululog_name:mk_idx_name(BackupDir, SegId),
  ok = file:write_file(IdxFile, <<"0123456789">>, [binary]),
  ok = file:write_file(SegFile, <<"0123456789">>, [binary]),
  [ { "truncate idx file"
    , fun() ->
        true = maybe_truncate_file(IdxFile, 1, BackupDir),
        ?assertEqual({ok, <<"0">>}, file:read_file(IdxFile)),
        ?assertEqual({ok, <<"0123456789">>}, file:read_file(BackupIdxFile))
      end
    }
  , { "truncate seg file"
    , fun() ->
        false = maybe_truncate_file(SegFile, 10, BackupDir),
        ?assertEqual({ok, <<"0123456789">>}, file:read_file(SegFile))
      end
    }
  ].

-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
