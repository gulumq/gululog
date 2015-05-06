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
        , delete_oldest_seg/2 %% Delete oldest segment from index
        , delete_from_cache/2 %% Delete given log entry from index cache
        , truncate/3          %% Truncate after given logid
        ]).

%% APIs for readers (public access)
-export([ locate/3            %% Locate {SegId, Position} for a given LogId
        , get_latest_logid/1  %% latest logid in ets
        , init_cache/1
        , close_cache/1
        , locate_in_cache/2
        ]).

%% APIs for repair
-export([get_next_position_in_index_file/2]).

-export_type([ index/0
             , cache/0
             ]).

%%%*_ MACROS and SPECS =========================================================

-include("gululog_priv.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-opaque cache() :: ets:tid().

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
  {IsNew, IndexFiles} = case wildcard_reverse(Dir) of
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

%% @doc Delete oldest segment from index
%% return the segid that is deleted, return 'false' in case:
%% 1. nothing to delete
%% 2. the oldest is also the latest, it is considered as purging the entire log
%% @end
-spec delete_oldest_seg(dirname(), index()) -> segid() | false.
delete_oldest_seg(Dir, #idx{tid = Tid, segid = CurrentSegId} = Index) ->
  case get_oldest_segid(Index) of
    SegIdToDelete when is_integer(SegIdToDelete) andalso SegIdToDelete < CurrentSegId ->
      Ms = ets:fun2ms(fun(?ETS_ENTRY(SegId, _, _)) -> SegId =:= SegIdToDelete end),
      _ = ets:select_delete(Tid, Ms),
      ok = file:delete(mk_name(Dir, SegIdToDelete)),
      SegIdToDelete;
    _ ->
      false
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
%% return 'false' iif it is an empty index.
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

%% @doc To find the byte offset for the logid next to the given one in the index file.
%% Clled when trying to repair possibly corrupted segment
%% @end
-spec get_next_position_in_index_file(filename(), logid()) -> position().
get_next_position_in_index_file(FileName, LogId) ->
  SegId = gululog_name:filename_to_segid(FileName),
  Fd = open_reader_fd(FileName),
  try
    {ok, <<Version:8>>} = file:read(Fd, 1),
    (LogId - SegId + 1) * file_entry_bytes(Version) + 1
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

%% @doc Truncate after given logid from index and segment
%% Return idx file and seg file for deleted or truncated
%% @End
-spec truncate(dirname(), index(), logid()) -> {ok, [{filename(), filename()}]}.
truncate(Dir, #idx{tid = Tid} = _Idx, LogIdToTruncate) ->
  {LogIdToTruncateSegId, _} = locate_in_cache(Tid, LogIdToTruncate),
  Ms = ets:fun2ms(fun(?ETS_ENTRY(_, LogId, _) = EtsEntry) when LogId > LogIdToTruncate -> EtsEntry end),
  case EtsEntryList = ets:select(Tid, Ms) of
    [] ->
      {ok, []};
    _ ->
      {TruncateList, DeleteList} =
        lists:partition(fun(?ETS_ENTRY(SegIdToTruncateX, _, _)) -> SegIdToTruncateX == LogIdToTruncateSegId end, EtsEntryList),
      DeleteResult   = truncate_delete_do(DeleteList, Tid, Dir),
      TruncateResult = truncate_truncate_do(TruncateList, Tid, Dir, LogIdToTruncate),
      {ok, DeleteResult ++ TruncateResult}
  end.

%%%*_ PRIVATE FUNCTIONS ========================================================

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
-spec wildcard_reverse(dirname()) -> [filename()].
wildcard_reverse(Dir) -> gululog_name:wildcard_idx_name_reversed(Dir).

%% @private Open 'raw' mode fd for writer to 'append'.
-spec open_writer_fd(boolean(), filename()) -> file:fd() | no_return().
open_writer_fd(true, FileName) ->
  {ok, Fd} = file:open(FileName, [write, read, raw, binary]),
  ok = file:write(Fd, <<?LOGVSN:8>>),
  {?LOGVSN, Fd};
open_writer_fd(false, FileName) ->
  {ok, Fd} = file:open(FileName, [write, read, raw, binary]),
  %% read two bytes instead of 1
  {ok, <<Version:8>>} = file:read(Fd, 1),
  %% more than the version byte
  {ok, Position} = file:position(Fd, eof),
  %% Hopefully, this assertion never fails,
  %% In case it happens, add a function to resect the corrupted tail.
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
-spec file_sync_close(file:fds()) -> ok.
file_sync_close(Fd) ->
  ok = file:sync(Fd),
  ok = file:close(Fd).

%% @private Delete index and segment files for truncate
-spec truncate_delete_do([{logid(), {segid(), position()}}], cache(), dirname()) ->
  [{filename(), filename()}].
truncate_delete_do([], _Tid, _Dir) ->
  [];
truncate_delete_do(DeleteList, Tid, Dir) ->
  [begin
     ets:delete(Tid, LogIdX),
     ok = file:delete(SegFileD = gululog_name:mk_seg_name(Dir, SegIdX)),
     ok = file:delete(IdxFileD = gululog_name:mk_idx_name(Dir, SegIdX)),
     {IdxFileD, SegFileD}
   end || {LogIdX, {SegIdX, _}} <- DeleteList].

%% @private Truncate index and segment files for truncate
-spec truncate_truncate_do([{logid(), {segid(), position()}}], cache(), dirname(), logid()) ->
  [{filename(), filename()}].
truncate_truncate_do([], _Tid, _Dir, _LogIdToTruncate) ->
  [];
truncate_truncate_do(TruncateList, Tid, Dir, LogIdToTruncate) ->
  [ets:delete(Tid, LogIdX) || {LogIdX, {_SegIdX, _}} <- TruncateList],
  %% truncate seg file
  {SegIdToTruncate, SegPosition} = locate_in_cache(Tid, LogIdToTruncate),
  SegFile = gululog_name:mk_seg_name(Dir, SegIdToTruncate),
  {ok, FdSeg} = file:open(SegFile, [write, read, raw, binary]),
  {ok, _NewSegPosition} = file:position(FdSeg, SegPosition),
  file:truncate(FdSeg),
  ok = file_sync_close(FdSeg),
  %% truncate idx file
  IdxFile = gululog_name:mk_idx_name(Dir, SegIdToTruncate),
  IdxPosition = get_next_position_in_index_file(IdxFile, LogIdToTruncate),
  {ok, FdIdx} = file:open(IdxFile, [write, read, raw, binary]),
  {ok, _NewIdxPosition} = file:position(FdIdx, IdxPosition),
  file:truncate(FdIdx),
  ok = file_sync_close(FdIdx),

  [{IdxFile, SegFile}].

%%%*_ TESTS ====================================================================

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
