%% @doc File

-module(gululog_file).

-export([ remove_file/2
        , copy_file/2
        , maybe_truncate_file/3
        ]).

%%%*_ MACROS and SPECS =========================================================

-include("gululog_priv.hrl").

%%%*_ API FUNCTIONS ============================================================

%% @doc Copy the file to another name and remove the source file.
%% @end
-spec remove_file(filename(), ?undef | dirname()) -> ok.
remove_file(FileName, ?undef) ->
  ok = file:delete(FileName),
  {?REPAIR_DELETED, FileName};
remove_file(FileName, TargetDir) ->
  TargetFile = backup_filename(FileName, TargetDir),
  ok = filelib:ensure_dir(TargetFile),
  ok = file:rename(FileName, TargetFile),
  {?REPAIR_BACKEDUP, FileName}.

%% @doc Maybe backup the original file, then truncate at the given position.
%% @end
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

%% @doc Copy .idx or .seg file to the given directory.
%% @end
-spec copy_file(filename(), dirname()) -> ok.
copy_file(Source, TargetDir) ->
  TargetFile = backup_filename(Source, TargetDir),
  ok = filelib:ensure_dir(TargetFile),
  {ok, _} = file:copy(Source, TargetFile),
  ok.

%%%*_ PRIVATE FUNCTIONS ========================================================

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

-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
