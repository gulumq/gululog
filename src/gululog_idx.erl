%% @doc LogId -> {SegId, Position} index.
%% LogId: Strict monotonic non-negative positive integer ID
%% SegId: The fist log ID in a log segment file, SegId is used as file name.
%% Position: Log byte position (see file:position/1) in a segment file

-module(gululog_idx).

%% APIs for writer (owner)
-export([ init/1              %% Initialize log index from the given log file directory
        , flush_close/1       %% close the writer cursor
        , append/3            %% Append a new log entry to index
        , switch/3            %% switch to a new segment
        , switch_append/4     %% switch then append
        , delete_oldest_seg/3 %% Delete oldest segment from index, backup it first if necessary
        , delete_from_cache/2 %% Delete given log entry from index cache
        , truncate/5          %% Truncate cache and file from the given logid (inclusive)
        ]).

%% APIs for readers (public access)
-export([ locate/3            %% Locate {SegId, Position} for a given LogId
        , get_latest_logid/1  %% latest logid in ets
        , init_cache/1
        , close_cache/1
        , locate_in_cache/2
        ]).

%% APIs for repair / truncation
-export([get_position_in_index_file/2]).

-export_type([ index/0
             , cache/0
             ]).

%%%*_ MACROS and SPECS =========================================================

-include("gululog_priv.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-opaque cache() :: ets:tid() | atom().

-record(idx, { version :: logvsn()
             , segid   :: segid()
             , fd      :: file:fd()
             , tid     :: cache()
             }).

-opaque index() :: #idx{}.

-define(ETS_ENTRY(SEGID_, LOGID_, POSITION_),
        {LOGID_, {SEGID_, POSITION_}}).
-define(TO_FILE_ENTRY(SEGID_, LOGID_, POSITION_),
        <<(LOGID_ - SEGID_):32, POSITION_:32>>).
-define(FROM_FILE_ENTRY_V1(SEGID_, FILE_ENTRY_BIG_INTEGER_),
        begin
          <<OFFSET_:32, POSITION_:32>> = <<FILE_ENTRY_BIG_INTEGER_:64>>,
          ?ETS_ENTRY(SEGID_, SEGID_ + OFFSET_, POSITION_)
        end).
-define(FILE_ENTRY_BYTES_V1, 8). %% Number of bytes in file per index entry.
-define(FILE_ENTRY_BITS_V1, 64). %% Number of bits in file per index entry.
-define(FILE_READ_CHUNK, (1 bsl 20)). %% Number of index entries per file read.

%%%*_ API FUNCTIONS ============================================================

%% @doc Initialize log index in the given directory.
%% The directory is created if not exists already
%% New index file is initialized if the given directry is empty
%% @end
-spec init(dirname()) -> index() | {error, no_return()}.
init(Dir) ->
  ok = filelib:ensure_dir(filename:join(Dir, "foo")),
  {IsNew, IndexFiles} = case wildcard_reversed(Dir) of
                          []    -> {true, [mk_name(Dir, 0)]};
                          Files -> {false, Files}
                        end,
  LatestSegment = hd(IndexFiles),
  SegId = gululog_name:filename_to_segid(LatestSegment),
  {Version, WriterFd} = open_writer_fd(IsNew, LatestSegment),
  Tid = init_cache(IndexFiles),
  #idx{ version = Version
      , segid   = SegId
      , fd      = WriterFd
      , tid     = Tid
      }.

%% @doc Read index files to populate ets cache table.
-spec init_cache([filename()]) -> cache().
init_cache(IndexFiles) ->
  Tid = ets:new(?MODULE, [ ordered_set
                         , public
                         , {read_concurrency, true} ]),
  ok = init_ets_from_index_files(Tid, IndexFiles),
  Tid.

%% @doc Close write fd, delete ets cache table.
-spec flush_close(index()) -> ok.
flush_close(#idx{fd = Fd, tid = Tid}) ->
  ok = file_sync_close(Fd),
  ok = close_cache(Tid),
  ok.

%% @doc Append a new index entry.
%% NB: There is no validation on the new LogId and Position to be appended
%% 1. LogId should be equal to SegId when appending to a new segment
%% 2. LogId should be monotonic. i.e. NewLogId >= LatestLogId + 1
%% 3. Position should be (at least MIN_LOG_SIZE) greater than the latest position
%% @end
-spec append(index(), logid(), position()) -> index() | no_return().
append(#idx{ version = ?LOGVSN
           , segid   = SegId
           , fd      = Fd
           , tid     = Tid
           } = Idx, LogId, Position) ->
  ok = file:write(Fd, ?TO_FILE_ENTRY(SegId, LogId, Position)),
  ets:insert(Tid, ?ETS_ENTRY(SegId, LogId, Position)),
  Idx.

%% @doc Delete oldest segment from index.
%% Return the new index and the deleted file with OP tag
%% Return 'false' instead of file with OP tag when:
%% 1. Nothing to delete
%% 2. The oldest is also the latest, it is considered purging the entire topic
%%    should be done using truncate API instead.
%% File is backed up in case backup dir is given.
%% @end
-spec delete_oldest_seg(dirname(), index(), ?undef | dirname()) ->
        {index(), false | {segid(), file_op()}}.
delete_oldest_seg(Dir, #idx{tid = Tid, segid = CurrentSegId} = Index, BackupDir) ->
  case get_oldest_segid(Index) of
    SegIdToDelete when is_integer(SegIdToDelete) andalso
                       SegIdToDelete < CurrentSegId ->
      Ms = ets:fun2ms(fun(?ETS_ENTRY(SegId, _, _)) -> SegId =:= SegIdToDelete end),
      _ = ets:select_delete(Tid, Ms),
      FileOp = gululog_file:delete(mk_name(Dir, SegIdToDelete), BackupDir),
      {Index, {SegIdToDelete, FileOp}};
    _ ->
      {Index, false}
  end.

%% @doc Switch to a new log segment
-spec switch(dirname(), index(), logid()) -> index().
switch(Dir, #idx{fd = Fd} = Idx, NextLogId) ->
  NewSegId = NextLogId,
  ok = file_sync_close(Fd),
  {?LOGVSN, NewFd} = open_writer_fd(_IsNew = true, mk_name(Dir, NewSegId)),
  Idx#idx{segid = NewSegId, fd = NewFd}.

%% @doc Switch to a new log segment, append new index entry.
-spec switch_append(dirname(), index(), logid(), position()) -> index().
switch_append(Dir, Idx, LogId, Position) ->
  NewIdx = switch(Dir, Idx, LogId),
  append(NewIdx, LogId, Position).

%% @doc Locate {SegId, Position} for a given LogId
%% return {segid(), position()} if the given LogId is found
%% return 'false' if not in valid range
%% @end
-spec locate(dirname(), index() | cache(), logid()) ->
        {segid(), position()} | false | no_return().
locate(Dir, #idx{tid = Tid}, LogId) ->
  locate(Dir, Tid, LogId);
locate(Dir, Tid, LogId) ->
  case locate_in_cache(Tid, LogId) of
    false ->
      case is_out_of_range(Tid, LogId) of
        true ->
          false;
        false ->
          PrevLogId = ets:prev(Tid, LogId),
          [?ETS_ENTRY(SegId, _, _)] = ets:lookup(Tid, PrevLogId),
          scan_locate(Dir, SegId, LogId)
      end;
    Location ->
      Location
  end.

%% @doc Locate {SegId, Position} for a given LogId
%% return {segid(), position()} from cached records
%% @end
-spec locate_in_cache(cache(), logid()) ->
        {segid(), position()} | false | no_return().
locate_in_cache(Tid, LogId) ->
  case ets:lookup(Tid, LogId) of
    []                                   -> false;
    [?ETS_ENTRY(SegId, LogId, Position)] -> {SegId, Position}
  end.

%% @doc Get latest logid from index.
%% return 'false' if it is an empty index.
%% @end
-spec get_latest_logid(index() | cache()) -> logid() | false.
get_latest_logid(#idx{tid = Tid}) ->
  get_latest_logid(Tid);
get_latest_logid(Tid) ->
  case ets:last(Tid) of
    '$end_of_table' -> false;
    LogId           -> LogId
  end.

-spec close_cache(cache()) -> ok.
close_cache(Tid) ->
  true = ets:delete(Tid),
  ok.

%% @doc To find the byte offset for the given logid in the index file.
%% Called when trying to repair possibly corrupted segment
%% or when truncating logs
%% @end
-spec get_position_in_index_file(filename(), logid()) -> position().
get_position_in_index_file(FileName, LogId) ->
  SegId = gululog_name:filename_to_segid(FileName),
  Fd = open_reader_fd(FileName),
  try
    {ok, <<Version:8>>} = file:read(Fd, 1),
    (LogId - SegId) * file_entry_bytes(Version) + 1
  after
    file:close(Fd)
  end.

%% @doc Delete given log entry (logid) from index.
%% Return new index.
%% NB! Refuse to delete the first log entry of each segment (i.e. when segid = logid).
%% @end
-spec delete_from_cache(index() | cache(), logid()) -> cache() | index().
delete_from_cache(#idx{tid = Tid} = Idx, LogId) ->
  Tid = delete_from_cache(Tid, LogId),
  Idx;
delete_from_cache(Tid, LogId) ->
  LatestLogId = get_latest_logid(Tid),
  case LogId =:= LatestLogId of
    true ->
      false; %% never delete the latest to keep a correect boundary check
    false ->
      case ets:lookup(Tid, LogId) of
        [] ->
          false; %% either out of range, or already deleted
        [?ETS_ENTRY(SegId, SegId, _Pos)] ->
          false; %% refuse to delete
        [?ETS_ENTRY(_SegId, LogId, _Pos)] ->
          ets:delete(Tid, LogId)
      end
  end,
  Tid.

%% @doc Truncate from the given logid from cache and index file.
%% Return new index(), the truncated segid, and a list of deleted segids
%% @end
-spec truncate(dirname(), index(), segid(), logid(), ?undef | dirname()) ->
        {index(), [file_op()]}.
truncate(Dir, #idx{tid = Tid, fd = Fd} = Idx, SegId, LogId, BackupDir) ->
  false = is_out_of_range(Tid, LogId), %% assert
  %% Find all the Segids that are greater than the given segid -- to be deleted
  Ms = ets:fun2ms(fun(?ETS_ENTRY(I, I, _)) when I > SegId -> I end),
  DeleteSegIdList0 = ets:select(Tid, Ms),
  %% In case truncating from the very beginning, delete instead
  {SegIdToTruncate, DeleteSegIdList} =
    case SegId =:= LogId of
      true  -> {?undef, [SegId | DeleteSegIdList0]};
      false -> {SegId, DeleteSegIdList0}
    end,
  %% close writer fd
  ok = file_sync_close(Fd),
  %% delete idx file for > segid
  FileOpList1 = truncate_delete_do(DeleteSegIdList, Dir, BackupDir),
  %% truncate idx file for = segid
  FileOpList2 = truncate_truncate_do(Dir, SegIdToTruncate, LogId, BackupDir),
  NewIdx =
    %% verify the given logid is the first one
    case LogId =:= ets:first(Tid) of
      true ->
        [] = wildcard_reversed(Dir), %% assert
        ok = close_cache(Tid),
        init(Dir);
      false ->
        NewTid = truncate_cache(Tid, LogId),
        {NewSegId, _} = locate_in_cache(Tid, LogId - 1),
        FileName = mk_name(Dir, NewSegId),
        {Version, NewFd} = open_writer_fd(false, FileName),
        Idx#idx{ version = Version
               , fd      = NewFd
               , segid   = NewSegId
               , tid     = NewTid
               }
    end,
  {NewIdx, FileOpList1 ++ FileOpList2}.

%%%*_ PRIVATE FUNCTIONS ========================================================

%% @private Truncate cache, from the given logid (inclusive).
-spec truncate_cache(cache(), logid()) -> cache().
truncate_cache(Tid, LogId) ->
  Ms = ets:fun2ms(fun(?ETS_ENTRY(_, LogIdX, _)) -> LogIdX >= LogId end),
  _ = ets:select_delete(Tid, Ms),
  Tid.


%% @private Scan the index file to locate the log position in segment file
%% This function is called only when ets cache is not hit
%% @end
-spec scan_locate(dirname(), segid(), logid()) ->
        {segid(), position()} | no_return().
scan_locate(Dir, SegId, LogId) ->
  true = (LogId > SegId), %% assert
  FileName = mk_name(Dir, SegId),
  Fd = open_reader_fd(FileName),
  try
    {ok, <<Version:8>>} = file:read(Fd, 1),
    scan_locate_per_vsn(Fd, SegId, LogId, Version)
  after
    file:close(Fd)
  end.

-spec scan_locate_per_vsn(file:fd(), segid(), logid(), logvsn()) ->
        {segid(), position()} | no_return().
scan_locate_per_vsn(Fd, SegId, LogId, 1) ->
  %% The offset caculate by per-entry size + one byte version
  Location = (LogId - SegId) * ?FILE_ENTRY_BYTES_V1 + 1,
  {ok, <<FileEntry:?FILE_ENTRY_BITS_V1>>} =
    file:pread(Fd, Location, ?FILE_ENTRY_BYTES_V1),
  ?ETS_ENTRY(SegId, LogId, Position) = ?FROM_FILE_ENTRY_V1(SegId, FileEntry),
  {SegId, Position}.

%% @private Check if the given log ID is out of indexing range.
%% 'true' when trying to locate a 'future' log
%% or e.g. an old segment has been removed.
%% @end
-spec is_out_of_range(cache(), logid()) -> boolean().
is_out_of_range(Tid, LogId) ->
  Latest = ets:last(Tid),
  (Latest =:= '$end_of_table') orelse %% empty table
  (Latest < LogId)             orelse %% too new
  (ets:first(Tid) > LogId).           %% too old

%% @private Create ets table to keep the index entries.
%% TODO: less indexing for earlier segments in case there are too many entries.
%% @end
-spec init_ets_from_index_files(cache(), [filename()]) -> ok | no_return().
init_ets_from_index_files(_Tid, []) -> ok;
init_ets_from_index_files(Tid, [FileName | Rest]) ->
  SegId = gululog_name:filename_to_segid(FileName),
  Fd = open_reader_fd(FileName),
  case file:read(Fd, 1) of
    eof ->
      ok = file:close(Fd),
      init_ets_from_index_files(Tid, Rest);
    {ok, <<Version:8>>} ->
      try
        ok = init_ets_from_index_file(Version, Tid, SegId, Fd)
      after
        ok = file:close(Fd)
      end,
      init_ets_from_index_files(Tid, Rest)
  end.

-spec init_ets_from_index_file(logvsn(), cache(), segid(), file:fd()) -> ok | no_return().
init_ets_from_index_file(_Version = 1, Tid, SegId, Fd) ->
  case file:read(Fd, ?FILE_ENTRY_BYTES_V1 * ?FILE_READ_CHUNK) of
    eof ->
      ok;
    {ok, ChunkBin} ->
      [ ets:insert(Tid, ?FROM_FILE_ENTRY_V1(SegId, Entry))
        || <<Entry:?FILE_ENTRY_BITS_V1>> <= ChunkBin ],
      init_ets_from_index_file(1, Tid, SegId, Fd)
  end.

%% @private Find all the index files in the given directory
%% return all filenames in reversed order.
%% @end
-spec wildcard_reversed(dirname()) -> [filename()].
wildcard_reversed(Dir) -> gululog_name:wildcard_idx_name_reversed(Dir).

%% @private Open 'raw' mode fd for writer to 'append'.
-spec open_writer_fd(IsNew :: boolean(), filename()) -> {logvsn(), file:fd()} | no_return().
open_writer_fd(true, FileName) ->
  {ok, Fd} = file:open(FileName, [write, read, raw, binary]),
  ok = file:write(Fd, <<?LOGVSN:8>>),
  {?LOGVSN, Fd};
open_writer_fd(false, FileName) ->
  {ok, Fd} = file:open(FileName, [write, read, raw, binary]),
  {ok, <<Version:8>>} = file:read(Fd, 1),
  {ok, Position} = file:position(Fd, eof),
  %% Hopefully, this assertion never fails,
  %% In case it happens, add a function to truncate the corrupted tail.
  0 = (Position - 1) rem file_entry_bytes(Version), %% assert
  {Version, Fd}.

%% @private Get per-version file entry bytes.
-spec file_entry_bytes(logvsn()) -> bytecnt().
file_entry_bytes(1) -> ?FILE_ENTRY_BYTES_V1.

%% @private Open 'raw' mode fd for reader.
-spec open_reader_fd(filename()) -> file:fd() | no_return().
open_reader_fd(FileName) ->
  {ok, Fd} = file:open(FileName, [read, raw, binary]),
  Fd.

%% @private Make index file path/name
-spec mk_name(dirname(), segid()) -> filename().
mk_name(Dir, SegId) -> gululog_name:mk_idx_name(Dir, SegId).

%% @private Get oldest segid from index
-spec get_oldest_segid(index()) -> logid() | false.
get_oldest_segid(#idx{tid = Tid}) ->
  case ets:first(Tid) of
    '$end_of_table' ->
      false;
    LogId ->
      [{LogId, {SegId, _}}] = ets:lookup(Tid, LogId),
      LogId = SegId %% assert
  end.

%% @private Sync and and close file.
-spec file_sync_close(file:fd()) -> ok.
file_sync_close(Fd) ->
  ok = file:sync(Fd),
  ok = file:close(Fd).

%% @private Delete index files.
%% It is very important to delete files in reversed order in order to keep
%% data integrity (in case this function crashes in the middle for example)
%% @end
-spec truncate_delete_do([segid()], dirname(), ?undef | dirname()) -> [file_op()].
truncate_delete_do(DeleteSegIdList, Dir, BackupDir) ->
  lists:map(fun(SegId) ->
              gululog_file:delete(mk_name(Dir, SegId), BackupDir)
            end, lists:reverse(lists:sort(DeleteSegIdList))).

%% @private Truncate index file.
-spec truncate_truncate_do(dirname(), ?undef | segid(), logid(),
                           ?undef | dirname()) -> [file_op()].
truncate_truncate_do(_Dir, ?undef, _LogId, _BackupDir) -> [];
truncate_truncate_do(Dir, SegId, LogId, BackupDir) ->
  IdxFile = mk_name(Dir, SegId),
  IdxPosition = get_position_in_index_file(IdxFile, LogId),
  [_ | _] = gululog_file:maybe_truncate(IdxFile, IdxPosition, BackupDir). %% assert

%%%*_ TESTS ====================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

test_dir() ->
  {ok, Dir} = file:get_cwd(),
  filename:join(Dir, "gululog_idx-ut").

gululog_idx_test_() ->
  { foreach
  , fun() ->
      Dir = test_dir(),
      DataList =
        [ {append,        0, 1}
        , {append,        1, 10}
        , {append,        2, 20}
        , {append,        3, 30}
        , {append,        4, 40}
        , {switch_append, 5, 1}
        , {append,        6, 60}
        , {switch_append, 7, 1}
        , {append,        8, 80}
        , {append,        9, 90}
        , {append,        10, 100}
        ],
      Idx = lists:foldl(
              fun({append, LogId, Position}, IdxIn) ->
                    append(IdxIn, LogId, Position);
                 ({switch_append, LogId, Position}, IdxIn) ->
                    switch_append(Dir, IdxIn, LogId, Position)
              end, init(Dir), DataList),
      ok = flush_close(Idx)
    end
  , fun(_) ->
      gululog_test_lib:cleanup(test_dir())
    end
  , [ { "basic truncation test"
      , fun() ->
          Dir             = test_dir(),
          BackupDir       = filename:join(Dir, "backup"),
          BackupDirDelete = filename:join(Dir, "backup_delete"),
          Idx0 = init(Dir),
          Expect1 = [{0, {0, 1}},
                     {1, {0, 10}},
                     {2, {0, 20}},
                     {3, {0, 30}},
                     {4, {0, 40}},
                     {5, {5, 1}},
                     {6, {5, 60}},
                     {7, {7, 1}},
                     {8, {7, 80}},
                     {9, {7, 90}},
                     {10, {7, 100}}],
          ?assertEqual(Expect1, ets:tab2list(Idx0#idx.tid)),
          ?assertException(error, {badmatch, true},
                           truncate(Dir, Idx0, 7, 11, ?undef)),
          Idx12 = Idx0, %% bing lazy to update the suffix number
          %% truncate the last log
          LogId1 = 10,
          {SegId1, _} = locate(Dir, Idx12, LogId1),
          {Idx13, Truncated1} = truncate(Dir, Idx12, SegId1, LogId1, ?undef),
          ?assertEqual(7, Idx13#idx.segid),
          ?assertEqual([{?OP_TRUNCATED, mk_name(Dir, 7)}], Truncated1),
          %% truncate 9
          LogId2 = 9,
          {SegId2, _} = locate(Dir, Idx13, LogId2),
          {Idx14, Truncated2} = truncate(Dir, Idx13, SegId2, LogId2, BackupDir),
          ?assertEqual([{?OP_TRUNCATED, mk_name(Dir, 7)}], Truncated2),
          ?assertEqual([mk_name(BackupDir, 7)],
                        wildcard_reversed(BackupDir)),
          ?assertEqual(7, Idx14#idx.segid),
          %% truncate 7
          LogIdone = 7,
          {SegIdone, _} = locate(Dir, Idx14, LogIdone),
          {Idxone,   _} = truncate(Dir, Idx14, SegIdone, LogIdone, ?undef),
          ?assertEqual(5, Idxone#idx.segid),
          %% truncate 6
          LogId3 = 6,
          {SegId3, _} = locate(Dir, Idxone, LogId3),
          {Idx15, Truncated3} = truncate(Dir, Idxone, SegId3, LogId3, BackupDirDelete),
          ?assertEqual([{?OP_TRUNCATED, mk_name(Dir, 5)}], lists:sort(Truncated3)),
          ?assertEqual([mk_name(BackupDirDelete, 5)],
                        wildcard_reversed(BackupDirDelete)),
          ?assertEqual(5, Idx15#idx.segid),
          %% truncate 3
          LogId4 = 3,
          {Segid4, _} = locate(Dir, Idx15, LogId4),
          {Idx16, Truncated4} = truncate(Dir, Idx15, Segid4, LogId4, ?undef),
          ?assertEqual([{?OP_DELETED, mk_name(Dir, 5)},
                        {?OP_TRUNCATED, mk_name(Dir, 0)}],
                       lists:sort(Truncated4)),
          ?assertEqual(0, Idx16#idx.segid),
          Expect2 = [{0, {0, 1}},
                     {1, {0, 10}},
                     {2, {0, 20}}],
          EtsTable2 = Idx16#idx.tid,
          ?assertEqual(Expect2, ets:tab2list(EtsTable2)),
          %% truncate to the very beginning
          LogId5 = 0,
          {SegId5, _} = locate(Dir, Idx16, LogId5),
          {Idx17, _}  = truncate(Dir, Idx16, SegId5, LogId5, ?undef),
          ?assertEqual(0, Idx17#idx.segid),
          ?assertEqual([], ets:tab2list(Idx17#idx.tid)),
          %% re-init
          ok = flush_close(Idx17),
          NewIdx = init(Dir),
          NewEtsTable = NewIdx#idx.tid,
          ?assertEqual([], ets:tab2list(NewEtsTable)),
          ok = flush_close(NewIdx),
          ok
        end
      }
    , { "delete oldest seg"
      , fun() ->
          Dir = test_dir(),
          BackupDir = filename:join(Dir, "backup"),
          Idx0 = init(Dir),
          DeleteFile1 = mk_name(Dir, 0),
          ?assertEqual({Idx0, {0, {?OP_DELETED, DeleteFile1}}}, delete_oldest_seg(Dir, Idx0, ?undef)),
          ?assertEqual(false, lists:member(DeleteFile1, wildcard_reversed(Dir))),
          DeleteFile2 = mk_name(Dir, 5),
          ?assertEqual({Idx0, {5, {?OP_DELETED, DeleteFile2}}}, delete_oldest_seg(Dir, Idx0, BackupDir)),
          ?assertEqual([mk_name(BackupDir, 5)], wildcard_reversed(BackupDir)),
          ?assertEqual(false, lists:member(DeleteFile2, wildcard_reversed(Dir))),
          ?assertEqual({Idx0, false}, delete_oldest_seg(Dir, Idx0, ?undef))
        end
      }
    , { "delete old seg + truncate"
      , fun() ->
          Dir = test_dir(),
          Idx0 = init(Dir),
          ?assertMatch({_, {0, _}}, delete_oldest_seg(Dir, Idx0, ?undef)),
          Files = lists:sort(wildcard_reversed(Dir)),
          {Idx1, Truncated} = truncate(Dir, Idx0, 5, 5, ?undef),
          FilesDeleted = lists:sort(lists:map(fun({?OP_DELETED, Fn}) -> Fn end, Truncated)),
          ?assertEqual(Files, FilesDeleted),
          ?assertEqual(0, Idx1#idx.segid)
        end
      }
    ]
  }.

-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
