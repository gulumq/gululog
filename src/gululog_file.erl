%% @doc Gululog index and segment file operation APIs.

-module(gululog_file).

-export([ delete/2
        , maybe_truncate/3
        ]).

%%%*_ MACROS and SPECS =========================================================

-include("gululog_priv.hrl").

%%%*_ API FUNCTIONS ============================================================

%% @doc Copy the file to another name and delete the source file.
-spec delete(filename(), ?undef | dirname()) -> file_op().
delete(FileName, ?undef) ->
  ok = file:delete(FileName),
  {?OP_DELETED, FileName};
delete(FileName, TargetDir) ->
  TargetFile = backup_filename(FileName, TargetDir),
  ok = filelib:ensure_dir(TargetFile),
  ok = file:rename(FileName, TargetFile),
  {?OP_DELETED, FileName}.

%% @doc Maybe backup the original file, then truncate at the given position.
-spec maybe_truncate(filename(), position(), ?undef | dirname()) -> [file_op()].
maybe_truncate(FileName, Position, BackupDir) ->
  %% open with 'read' mode, otherwise truncate does not work
  {ok, Fd} = file:open(FileName, [write, read, raw, binary]),
  try
    {ok, Size} = file:position(Fd, eof),
    true = (Position =< Size), %% assert
    case Position < Size of
      true ->
        [ok = copy(FileName, BackupDir) || BackupDir =/= ?undef],
        {ok, Position} = file:position(Fd, Position),
        ok = file:truncate(Fd),
        [{?OP_TRUNCATED, FileName}];
      false ->
        []
    end
  after
    file:close(Fd)
  end.

%%%*_ PRIVATE FUNCTIONS ========================================================

%% @private Copy .idx or .seg file to the given directory.
-spec copy(filename(), dirname()) -> ok.
copy(Source, TargetDir) ->
  TargetFile = backup_filename(Source, TargetDir),
  ok = filelib:ensure_dir(TargetFile),
  {ok, _} = file:copy(Source, TargetFile),
  ok.

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
        _ = delete(IdxFile, BackupDir),
        ?assertEqual({ok, <<"idx">>}, file:read_file(BackupIdxFile)),
        ?assertEqual(false, filelib:is_file(IdxFile))
      end
    }
  , { "move seg file"
    , fun() ->
        _ = delete(SegFile, ?undef),
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
        ?assertEqual([{?OP_TRUNCATED, IdxFile}], maybe_truncate(IdxFile, 1, BackupDir)),
        ?assertEqual({ok, <<"0">>}, file:read_file(IdxFile)),
        ?assertEqual({ok, <<"0123456789">>}, file:read_file(BackupIdxFile))
      end
    }
  , { "truncate seg file"
    , fun() ->
        ?assertEqual([], maybe_truncate(SegFile, 10, BackupDir)),
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
