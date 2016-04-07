%% @doc LogId -> {Timestamp, SegId, Position} index.
%% LogId: Strict monotonic non-negative integer ID
%% Timestamp: Sever time in second precision.
%% SegId: The fist log ID in a log segment file, SegId is used as file name.
%% Position: Log byte position (see file:position/1) in a segment file
%% @end

-module(gululog_idx).

%% Write APIs
-export([ init/3              %% Initialize log index from the given log file directory and the first logid
        , flush_close/1       %% close the writer cursor
        , append/4            %% Append a new log entry to index
        , switch/3            %% switch to a new segment
        , switch_append/5     %% switch then append
        , delete_oldest_seg/3 %% Delete oldest segment from index, backup it first if necessary
        , delete_from_cache/2 %% Delete given log entry from index cache
        , truncate/5          %% Truncate cache and file from the given logid (inclusive)
        ]).

%% Read APIs
-export([ locate/3            %% Locate {SegId, Position} for a given LogId
        , get_oldest_segid/1  %% Get oldest segid
        , get_latest_logid/1  %% Latest logid in ets
        , get_latest_ts/1     %% Latest log timestamp
        , get_seg_oldest_ts/3 %% timestamp of the oldest log entrie in the given segment
        , get_seg_latest_ts/3 %% timestamp of the latest log entrie in the given segment
        , first_logid_since/3 %% First logid since the given timestamp (second inclusive)
        ]).

%% APIs for internal use
-export([ init/2
        , get_position_in_index_file/2
        ]).

-export_type([ index/0
             ]).

%%%*_ MACROS and SPECS =========================================================

-include("gululog_priv.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-type cache() :: ets:tid().
-type cache_policy() :: gululog_cache_policy().
-type options() :: gululog_options().

-record(idx, { version :: logvsn()
             , segid   :: segid()
             , fd      :: file:fd()
             , tid     :: cache()
             , options :: options()
             , latest  :: false | entry()
             }).

-opaque index() :: #idx{}.
-type entry() :: {logid(), os_sec(), segid(), position()}.


-define(ENTRY(LogId, Ts, SegId, Position),
             {LogId, Ts, SegId, Position}).

-define(FILE_ENTRY(LogId, Ts, SegId, Position),
        <<(LogId - SegId):32, Ts:32, Position:32>>).

-define(FILE_ENTRY_BYTES_V1, 12). %% Number of bytes in file per index entry.
-define(FILE_ENTRY_BITS_V1,  96). %% Number of bits  in file per index entry.

-define(FILE_READ_CHUNK, (1 bsl 10)). %% Number of index entries per file read.

-define(EOT, '$end_of_table').

%%%*_ API FUNCTIONS ============================================================

%% @doc Initialize log index in the given directory.
%% The directory is created if not exists already
%% New index file is initialized if the given directry is empty
%% @end
-spec init(dirname(), segid(), options()) -> index().
init(Dir, OldestSegId, Options) ->
  ok = filelib:ensure_dir(filename:join(Dir, "foo")),
  IndexFiles = case wildcard_reversed(Dir) of
                 []    -> [mk_name(Dir, OldestSegId)];
                 Files -> Files
               end,
  init(IndexFiles, idx_options(Options)).

%% @doc Read index files to populate ets cache table.
-spec init([filename()], options()) -> cache().
init(IndexFiles, Options) ->
  LatestSegment = hd(IndexFiles),
  SegId = gululog_name:filename_to_segid(LatestSegment),
  {Version, WriterFd} = open_writer_fd(LatestSegment),
  Tid = ets:new(?MODULE, [ ordered_set
                         , public
                         , {read_concurrency, true}
                         ]),
  NewTid = init_cache_from_files(Tid, IndexFiles, Options),
  #idx{ version = Version
      , segid   = SegId
      , fd      = WriterFd
      , tid     = NewTid
      , options = Options
      , latest  = get_latest_entry_in_file(SegId, LatestSegment)
      }.

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
-spec append(index(), logid(), position(), os_sec()) -> index().
append(#idx{ version  = ?LOGVSN
           , segid    = SegId
           , fd       = Fd
           , tid      = Tid
           , options  = Options
           } = Idx, LogId, Position, Ts) ->
  CacheEntry = ?ENTRY(LogId, Ts, SegId, Position),
  ok = file:write(Fd, ?FILE_ENTRY(LogId, Ts, SegId, Position)),
  NewTid = maybe_insert(Tid, CacheEntry, Options),
  Idx#idx{tid = NewTid, latest = CacheEntry}.

%% @doc Switch to a new log segment
-spec switch(dirname(), index(), logid()) -> index().
switch(Dir, #idx{fd = Fd} = Idx, NextLogId) ->
  NewSegId = NextLogId,
  ok = file_sync_close(Fd),
  {?LOGVSN, NewFd} = open_writer_fd(mk_name(Dir, NewSegId)),
  Idx#idx{segid = NewSegId, fd = NewFd}.

%% @doc Switch to a new log segment, append new index entry.
-spec switch_append(dirname(), index(), logid(), position(), os_sec()) -> index().
switch_append(Dir, Idx, LogId, Position, Ts) ->
  NewIdx = switch(Dir, Idx, LogId),
  append(NewIdx, LogId, Position, Ts).

%% @doc Locate {SegId, Position} for a given LogId
%% return {segid(), position()} if the given LogId is found
%% return 'false' if not in valid range
%% @end
-spec locate(dirname(), index(), logid()) -> false | location().
locate(Dir, Idx, LogId) -> to_location(read_entry(Dir, Idx, LogId)).

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
      Ms = ets:fun2ms(fun(?ENTRY(_, _, SegId, _)) -> SegId =:= SegIdToDelete end),
      _ = ets:select_delete(Tid, Ms),
      FileOp = gululog_file:delete(mk_name(Dir, SegIdToDelete), BackupDir),
      {Index, {SegIdToDelete, FileOp}};
    _ ->
      {Index, false}
  end.

%% @doc Get oldest segid from index.
-spec get_oldest_segid(index()) -> false | logid().
get_oldest_segid(#idx{tid = Tid}) ->
  case first_in_cache(Tid) of
    false                           -> false;
    ?ENTRY(LogId, _Ts, SegId, _Pos) -> LogId = SegId %% assert
  end.

%% @doc Get the first logid since the given timestamp (inclusive).
-spec first_logid_since(dirname(), index(), os_sec()) -> false | logid().
first_logid_since(Dir, #idx{tid = Tid, latest = LatestEntry}, Ts) ->
  not is_empty_cache(Tid) andalso
  find_first_logid_since(Dir, Tid, LatestEntry, Ts).

%% @doc Get latest logid from index.
%% return 'false' if it is an empty index.
%% @end
-spec get_latest_logid(index()) -> logid() | false.
get_latest_logid(#idx{latest = false})                            -> false;
get_latest_logid(#idx{latest = ?ENTRY(LogId, _Ts, _SegId, _Pos)}) -> LogId.

%% @doc Get timestamp of the latest index entry.
%% Return false in case index is empty.
%% @end
-spec get_latest_ts(index()) -> os_sec() | false.
get_latest_ts(#idx{latest = false})                            -> false;
get_latest_ts(#idx{latest = ?ENTRY(_LogId, Ts, _SegId, _Pos)}) -> Ts.

%% @doc Get timestamp of the oldest index entry in the given segment.
%% Retrun false in case no such segment.
%% @end
-spec get_seg_oldest_ts(dirname(), index(), segid()) -> false | os_sec().
get_seg_oldest_ts(Dir, Idx, SegId) ->
  case read_entry(Dir, Idx, SegId) of
    false                   -> false;
    ?ENTRY(_, Ts, SegId, _) -> Ts
  end.

%% @doc Get timestamp of the latest index entry in the given segment.
%% Return false in case no such segment.
%% @end
-spec get_seg_latest_ts(dirname(), index(), segid()) -> false | os_sec().
get_seg_latest_ts(Dir, Idx, SegId) ->
  case read_latest_in_seg(Dir, Idx, SegId) of
    false                   -> false;
    ?ENTRY(_, Ts, SegId, _) -> Ts
  end.

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
-spec delete_from_cache(index(), logid()) -> index().
delete_from_cache(#idx{tid = Tid} = Idx, LogId) ->
  case lookup_cache(Tid, LogId) of
    false ->
      false; %% either out of range, or not cached, or already deleted
    ?ENTRY(SegId, _Ts, SegId, _Pos) ->
      false; %% refuse to delete the first entry in one seg
    ?ENTRY(LogId, _Ts, _SegId, _Pos) ->
      ets:delete(Tid, LogId)
  end,
  Idx.

%% @doc Truncate from the given logid from cache and index file.
%% Return new index(), the truncated segid, and a list of deleted segids
%% @end
-spec truncate(dirname(), index(), segid(), logid(), ?undef | dirname()) ->
        {index(), [file_op()]}.
truncate(Dir, #idx{tid = Tid, fd = Fd} = Idx, SegId, LogId, BackupDir) ->
  false = is_out_of_range(Idx, LogId), %% assert
  %% Find all the Segids that are greater than the given segid -- to be deleted
  Ms = ets:fun2ms(fun(?ENTRY(I, _Ts, I, _)) when I > SegId -> I end),
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
    %% check if the given logid is the first one
    case prev_logid(Tid, LogId) of
      false ->
        [] = wildcard_reversed(Dir), %% assert
        ok = close_cache(Tid),
        init(Dir, SegId, Idx#idx.options);
      PrevLogId ->
        NewSegId  = get_segid(Tid, PrevLogId),
        NewTid    = truncate_cache(Tid, LogId),
        FileName  = mk_name(Dir, NewSegId),
        LastEntry = get_latest_entry_in_file(NewSegId, FileName),
        {Version, NewFd} = open_writer_fd(FileName),
        Idx#idx{ version = Version
               , fd      = NewFd
               , segid   = NewSegId
               , tid     = NewTid
               , latest  = LastEntry
               }
    end,
  {NewIdx, FileOpList1 ++ FileOpList2}.

%%%*_ PRIVATE FUNCTIONS ========================================================

-spec maybe_insert(cache(), entry(), options()) -> cache().
maybe_insert(Tid, ?ENTRY(SegId, _Ts, SegId, _Position) = Entry, _Options) ->
  %% always insert the latest
  true = ets:insert(Tid, Entry),
  Tid;
maybe_insert(Tid, ?ENTRY(LogId, _Ts, _SegId, _Position) = Entry, Options) ->
  ?ENTRY(LastLogId, _, _, _) = last_in_cache(Tid),
  Policy = keyget(cache_policy, Options, ?GULULOG_DEFAULT_CACHE_POLICY),
  is_cache_policy_met(LastLogId, LogId, Policy) andalso
    ets:insert(Tid, Entry),
  Tid.

-spec is_cache_policy_met(logid(), logid(), cache_policy()) -> boolean().
is_cache_policy_met(_LastLogId, _LogId, minimum)    -> false;
is_cache_policy_met(_LastLogId, _logId, all)        -> true;
is_cache_policy_met(LastLogId, LogId, {every, Nth}) -> LogId - LastLogId >= Nth.

%% @private Get position info from index entry.
-spec to_location(false | entry()               ) -> false | location().
to_location(                               false) -> false;
to_location(?ENTRY(_LogId, _Ts, SegId, Position)) -> {SegId, Position}.

%% @private Read entry from cache, read from file if deleted from cache.
-spec read_entry(dirname(), index(), logid()) -> false | entry().
read_entry(Dir, #idx{tid = Tid} = Idx, LogId) ->
  not is_out_of_range(Idx, LogId) andalso
  case lookup_cache(Tid, LogId) of
    false -> read_file_entry(Dir, get_segid(Tid, LogId), LogId);
    Entry -> Entry
  end.

%% @private Truncate cache, from the given logid (inclusive).
%% @end
-spec truncate_cache(cache(), logid()) -> cache().
truncate_cache(Tid, LogId) ->
  Ms = ets:fun2ms(fun(?ENTRY(LogIdX, _, _, _)) -> LogIdX >= LogId end),
  _  = ets:select_delete(Tid, Ms),
  Tid.

%% @private Get the latest cached entry which has timestamp before the given Ts.
-spec latest_cached_entry_before_ts(cache(), os_sec(), logid(), logid(), logid()) -> entry().
latest_cached_entry_before_ts(Tid, _Ts, LogId, _Hi, LogId) ->
  lookup_cache(Tid, LogId);
latest_cached_entry_before_ts(Tid, Ts, Lo, Hi, LogId) ->
  case lookup_cache(Tid, LogId) of
    false ->
      %% The entry is perhaps deleted from cache, continue with previous one in cache
      latest_cached_entry_before_ts(Tid, Ts, Lo, Hi, prev_logid(Tid, LogId));
    ?ENTRY(LogId, TsX, _, _) when TsX < Ts ->
      latest_cached_entry_before_ts(Tid, Ts, LogId, Hi, (LogId + Hi) bsr 1);
    ?ENTRY(LogId, TsX, _, _) when TsX >= Ts ->
      latest_cached_entry_before_ts(Tid, Ts, Lo, LogId, (Lo + LogId) bsr 1)
  end.

%% @private Position read the index file to locate the log position in segment file.
%% This function is called only when ets cache is not hit
%% @end
-spec read_file_entry(dirname(), segid(), logid()) -> entry().
read_file_entry(Dir, SegId, LogId) ->
  true = (LogId > SegId), %% assert
  Fd = open_reader_fd(Dir, SegId),
  try
    {ok, <<Version:8>>} = file:read(Fd, 1),
    read_file_entry(Version, Fd, SegId, LogId)
  after
    file:close(Fd)
  end.

%% @private Read file entry per version.
%% Assuming this function is always called with valid logid
%% @end
-spec read_file_entry(logvsn(), file:fd(), segid(), logid()) -> entry().
read_file_entry(Version, Fd, SegId, LogId) ->
  %% in-file-offset = (logid-offset * entry-size) + one byte version
  Bytes = file_entry_bytes(Version),
  Location = (LogId - SegId) * Bytes + 1,
  {ok, FileEntry} = file:pread(Fd, Location, Bytes),
  from_file_entry(1, SegId, FileEntry).

%% @private Find the first logid since the given timestamp (inclusive).
%% Assuming the cache ets table is not empty.
%% Algorithm:
%%   1. Find the latest entry (logid = N) in cache BEFORE the given timestamp T
%%   2. If logid=N+1 is found in cache
%%        N+1 is the result
%%      Oterwise
%%        M = next_logid_in_cache(N)
%%        Search from N+1 to M for log entry with timestamp >= T
%% Goal: try to use cache as much as possible,
%%       limit file:open to only ONECE for each find_first_logid_since/3 call
%% @end
-spec find_first_logid_since(dirname(), cache(), entry(), os_sec()) -> false | logid().
find_first_logid_since(Dir, Tid, LatestEntry, Ts) ->
  ?ENTRY(LogIdLo, TsLo, _, _) = first_in_cache(Tid),
  ?ENTRY(LogIdHi, TsHi, _, _) = LatestEntry,
  case Ts =< TsLo of
    true                 -> LogIdLo; %% all logs are after the given ts
    false when Ts > TsHi -> false;   %% no log since the given ts
    false ->                       %% in between
      LogIdMid = (LogIdLo + LogIdHi) bsr 1,
      %% First, run a quick binary search (in cache) to locate
      %% the latest logid which has timestamp < Ts
      ?ENTRY(LogId1, Ts1, SegId1, _Pos1) =
        latest_cached_entry_before_ts(Tid, Ts, LogIdLo, LogIdHi, LogIdMid),
      true = (Ts1 < Ts), %% assert
      %% Get the upper limit of target logid
      LogId2 = case next_in_cache(Tid, LogId1) of
                 false                                 -> LogIdHi;
                 ?ENTRY(LogId2_, _Ts2, _SegId2, _Pos2) -> LogId2_
               end,
      %% Then run another binary search (in the file) to locate the
      %% first log entry of timestamp >= Ts
      find_first_logid_since_in_file(Dir, Ts, SegId1, LogId1+1, LogId2)
  end.

%% @private when there might be skipped/deleted cache entries for the given timestamp,
%% read .idx file entries to locate the fisrt logid since the given timestamp in case.
%% @end
-spec find_first_logid_since_in_file(dirname(), os_sec(), segid(), logid(), logid()) -> logid().
find_first_logid_since_in_file(_Dir, _Ts, _SegId, LogId, LogId) ->
  %% Successive, no cache entry is deleted
  LogId;
find_first_logid_since_in_file(Dir, Ts, SegId, LogIdLo, LogIdHi) ->
  %% Entries are missing in cache, find in file
  Fd = open_reader_fd(Dir, SegId),
  try
    {ok, <<Version:8>>} = file:read(Fd, 1),
    ReadFun = fun(LogId) -> read_file_entry(Version, Fd, SegId, LogId) end,
    find_first_logid_since_in_file(ReadFun, LogIdLo, LogIdHi, Ts)
  after
    file:close(Fd)
  end.

%% @private Read .idx file entries to locate the first logid since the given timestamp.
-spec find_first_logid_since_in_file(fun((logid()) -> entry()),
                                     logid(), logid(), os_sec()) -> logid().
find_first_logid_since_in_file(_ReadFun, LogId, LogId, _Ts) ->
  LogId;
find_first_logid_since_in_file(ReadFun, LogIdLo, LogIdHi, Ts) ->
  case ReadFun((LogIdLo + LogIdHi) bsr 1) of
    ?ENTRY(LogIdMi, TsX, _SegId, _Pos) when TsX >= Ts ->
      find_first_logid_since_in_file(ReadFun, LogIdLo, LogIdMi, Ts);
    ?ENTRY(LogIdMi, TsX, _SegId, _Pos) when TsX < Ts ->
      find_first_logid_since_in_file(ReadFun, LogIdMi+1, LogIdHi, Ts)
  end.

%% @private Read the latest (last) log entry of the given segment.
-spec read_latest_in_seg(dirname(), index(), segid()) -> false | entry().
read_latest_in_seg(_Dir, #idx{segid = SegId, latest = Latest}, SegId) ->
  Latest;
read_latest_in_seg(Dir, #idx{tid = Tid, segid = LatestSegId} = Idx, SegId) ->
  not is_out_of_range(Idx, SegId) andalso
  read_entry(Dir, Idx, find_latest_logid_in_seg(Tid, SegId, LatestSegId)).

%% @private Search backward from a greater segment id
%% to find the last logid of the given segment.
%% @end
-spec find_latest_logid_in_seg(cache(), segid(), segid()) -> logid().
find_latest_logid_in_seg(Tid, SegId, SegIdX) ->
  true = (SegIdX > SegId), %% assert
  LogId = (SegId + SegIdX) bsr 1,
  find_latest_logid_in_seg(Tid, SegId, SegIdX, LogId).

-spec find_latest_logid_in_seg(cache(), segid(), segid(), logid()) -> logid().
find_latest_logid_in_seg(_Tid, _SegId, SegIdX, SegIdX) ->
  SegIdX - 1;
find_latest_logid_in_seg(Tid, SegId, SegIdX, LogId) ->
  case get_segid(Tid, LogId) of
    SegId ->
      %% test a greater logid
      NewLogId = (LogId + 1 + SegIdX) bsr 1,
      find_latest_logid_in_seg(Tid, SegId, SegIdX, NewLogId);
    SegIdY ->
      find_latest_logid_in_seg(Tid, SegId, SegIdY)
  end.

%% @private Get segid for the given logid.
-spec get_segid(cache(), logid()) -> false | segid().
get_segid(Tid, LogId) ->
  case lookup_cache(Tid, LogId) of
    false                  -> get_segid(Tid, prev_logid(Tid, LogId));
    ?ENTRY(_, _, SegId, _) -> SegId
  end.

%% @private Check if the given log ID is out of indexing range.
%% 'true' when trying to locate a 'future' log
%% or e.g. an old segment has been removed.
%% @end
-spec is_out_of_range(index(), logid()) -> boolean().
is_out_of_range(#idx{tid = Tid} = Idx, LogId) ->
  Latest = get_latest_logid(Idx),
  (Latest =:= false)        orelse %% empty table
  (Latest < LogId)          orelse %% too new
  (ets:first(Tid) > LogId).        %% too old

%% @private Create ets table to keep the index entries.
-spec init_cache_from_files(cache(), [filename()], options()) -> cache().
init_cache_from_files(Tid, [], _Options) -> Tid;
init_cache_from_files(Tid, [FileName | Rest], Options) ->
  NewTid = init_cache_from_file(FileName, Tid, Options),
  init_cache_from_files(NewTid, Rest, Options).

-spec init_cache_from_file(filename(), cache(), options()) -> ok.
init_cache_from_file(FileName, Tid, Options) ->
  SegId = gululog_name:filename_to_segid(FileName),
  Fd = open_reader_fd(FileName),
  try
    case file:read(Fd, 1) of
      eof                 -> ok;
      {ok, <<Version:8>>} -> ok = init_cache_from_file(Version, Tid, SegId, Fd, Options)
    end,
    Tid
  after
    ok = file:close(Fd)
  end.

-spec init_cache_from_file(logvsn(), cache(), segid(), file:fd(), options()) -> ok.
init_cache_from_file(Version, Tid, SegId, Fd, Options) ->
  Policy = keyget(cache_policy, Options, ?GULULOG_DEFAULT_CACHE_POLICY),
  EntryBytes = file_entry_bytes(Version),
  EntryBits = EntryBytes * 8,
  ReadFun =
    fun(ChunkSize, CurrentPos, SkipEntries) ->
      case file:read(Fd, EntryBytes * ChunkSize) of
        eof ->
          eof;
        {ok, ChunkBin} ->
          [ ets:insert(Tid, from_file_entry(Version, SegId, <<E:EntryBits>>))
            || <<E:EntryBits>> <= ChunkBin ],
          Pos = CurrentPos + size(ChunkBin),
          case SkipEntries > 0 of
            true  ->
              Pos;
            false ->
              {ok, NewPos} = file:position(Fd, Pos + EntryBytes * SkipEntries),
              NewPos
            end
          end
      end,
  ok = init_cache_from_file_entries(Policy, ReadFun, _CurrentPos = 1).

-spec init_cache_from_file_entries(cache_policy(), Fun, pos_integer()) -> ok
        when Fun :: fun((pos_integer(), pos_integer(), pos_integer()) -> eof | pos_integer()).
init_cache_from_file_entries(_, _ReadFun, eof) ->
  ok;
init_cache_from_file_entries(minimum, ReadFun, 1) ->
  _ = ReadFun(_ChunkSize = 1, _Pos = 1, _SkipEntries = 0),
  ok;
init_cache_from_file_entries(all, ReadFun, Pos) ->
  NewPos = ReadFun(?FILE_READ_CHUNK, Pos, 0),
  init_cache_from_file_entries(all, ReadFun, NewPos);
init_cache_from_file_entries({every, 1}, ReadFun, 1) ->
  init_cache_from_file_entries(all, ReadFun, 1);
init_cache_from_file_entries({every, Nth}, ReadFun, Pos) ->
  (is_integer(Nth) andalso Nth > 1) orelse erlang:error({bad_cache_policy, {every, Nth}}),
  NewPos = ReadFun(1, Pos, Nth - 1),
  init_cache_from_file_entries({every, Nth}, ReadFun, NewPos).

%% @private Read last entry from index file, return false in case empty.
-spec get_latest_entry_in_file(segid(), filename()) -> false | entry().
get_latest_entry_in_file(SegId, FileName) ->
  Fd = open_reader_fd(FileName),
  try
    {ok, <<Version:8>>} = file:pread(Fd, 0, 1),
    {ok, EofPosition}   = file:position(Fd, eof),
    EntryBytes          = file_entry_bytes(Version),
    EofPosition >= EntryBytes+1 andalso
    begin
      {ok, EntryBin} = file:pread(Fd, EofPosition - EntryBytes, EntryBytes),
      from_file_entry(Version, SegId, EntryBin)
    end
  after
    ok = file:close(Fd)
  end.

%% @private Find all the index files in the given directory
%% return all filenames in reversed order.
%% @end
-spec wildcard_reversed(dirname()) -> [filename()].
wildcard_reversed(Dir) -> gululog_name:wildcard_idx_name_reversed(Dir).

%% @private Open 'raw' mode fd for writer to 'append'.
-spec open_writer_fd(filename()) -> {logvsn(), file:fd()}.
open_writer_fd(FileName) ->
  {ok, Fd} = file:open(FileName, [write, read, raw, binary, delayed_write]),
  Version = case file:read(Fd, 1) of
              eof ->
                file:write(Fd, <<?LOGVSN:8>>),
                ?LOGVSN;
              {ok, <<Version_:8>>} ->
                Version_
            end,
  {ok, Position} = file:position(Fd, eof),
  %% Hopefully, this assertion never fails,
  %% In case it happens, add a function to truncate the corrupted tail.
  0 = (Position - 1) rem file_entry_bytes(Version), %% assert
  {Version, Fd}.

%% @private Get per-version file entry bytes.
-spec file_entry_bytes(logvsn()) -> bytecnt().
file_entry_bytes(1) -> ?FILE_ENTRY_BYTES_V1.

%% @private Open 'raw' mode fd for reader.
-spec open_reader_fd(dirname(), segid()) -> file:fd().
open_reader_fd(DirName, SegId) ->
  open_reader_fd(mk_name(DirName, SegId)).

%% @private Open 'raw' mode fd for reader.
-spec open_reader_fd(filename()) -> file:fd().
open_reader_fd(FileName) ->
  {ok, Fd} = file:open(FileName, [read, raw, binary, read_ahead]),
  Fd.

%% @private Make index file path/name
-spec mk_name(dirname(), segid()) -> filename().
mk_name(Dir, SegId) -> gululog_name:mk_idx_name(Dir, SegId).

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

%% @private Convert file binary entry to ets entry.
-spec from_file_entry(logvsn(), segid(), binary()) -> entry().
from_file_entry(1, SegId, <<Offset:32, Ts:32, Position:32>>) ->
  ?ENTRY(SegId + Offset, Ts, SegId, Position).

%%%*_ Cache help functions =====================================================

%% @private Get the first entry in cache
%% Return 'false' iff empty.
%% @end
-spec first_in_cache(cache()) -> false | entry().
first_in_cache(Tid) ->
  case ets:first(Tid) of
    ?EOT  -> false;
    LogId -> lookup_cache(Tid, LogId)
  end.

%% @private Get last entry in cache.
%% Return 'false' iff empty.
%% @end
last_in_cache(Tid) ->
  case ets:last(Tid) of
    ?EOT  -> false;
    LogId -> lookup_cache(Tid, LogId)
  end.

%% @private Get previous logid in cache.
-spec prev_logid(cache(), logid()) -> false | logid().
prev_logid(Tid, LogId) ->
  case ets:prev(Tid, LogId) of
    ?EOT      -> false;
    PrevLogId -> PrevLogId
  end.

%% @private Get next entry from cache of the given logid from cache.
%% Retrun 'false' iff empty.
%% @end
-spec next_in_cache(cache(), logid()) -> false | entry().
next_in_cache(Tid, LogId) ->
  case ets:next(Tid, LogId) of
    ?EOT      -> false;
    NextLogId -> lookup_cache(Tid, NextLogId)
  end.

%% @private Delete cache ets table.
-spec close_cache(cache()) -> ok.
close_cache(Tid) ->
  true = ets:delete(Tid),
  ok.

%% @private Lookup entry from cache.
-spec lookup_cache(cache(), logid()) -> false | entry().
lookup_cache(Tid, LogId) ->
  case ets:lookup(Tid, LogId) of
    []  -> false;
    [E] -> E
  end.

%% @private Check if the cache is empty.
-spec is_empty_cache(cache()) -> boolean().
is_empty_cache(Tid) -> ?EOT =:= ets:first(Tid).

%% @private Get value from key-value list, return default if not found
-spec keyget(Key::term(), [{Key::term(), Value::term()}], Default::term()) -> Value::term().
keyget(Key, KvList, Default) ->
  case lists:keyfind(Key, 1, KvList) of
    {Key, Value} -> Value;
    false        -> Default
  end.

idx_options(Options) ->
  [{K, V} || {K, V} <- Options, K =:= cache_policy].

%%%*_ TESTS ====================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(cache_all, [{cache_policy, all}]).
-define(cache_min, [{cache_policy, minimum}]).
-define(cache_nth(N), [{cache_policy, {every, N}}]).

cache_help_fun_test() ->
  {ok, Cwd} = file:get_cwd(),
  Dir = filename:join(Cwd, "gululog_idx-cache_help_fun_test"),
  #idx{tid = Tid} = Idx0 = init(Dir, 0, ?cache_all),
  ?assertEqual(false, first_in_cache(Tid)),
  ?assertEqual(false, last_in_cache(Tid)),
  ?assertEqual(false, prev_logid(Tid, 0)),
  ?assertEqual(false, next_in_cache(Tid, 0)),
  #idx{tid = Tid} = append(Idx0, 0, 1, gululog_dt:os_sec()),
  ?assertEqual(first_in_cache(Tid), last_in_cache(Tid)),
  ?assertEqual(false, prev_logid(Tid, 0)),
  ?assertEqual(false, next_in_cache(Tid, 0)),
  ok.

basic_find_logid_by_ts_test() ->
  {ok, Cwd} = file:get_cwd(),
  Dir = filename:join(Cwd, "gululog_idx-basic_find_logid_by_ts_test"),
  Idx0 = init(Dir, 0, ?cache_min),
  ?assertEqual(false, first_logid_since(Dir, Idx0, gululog_dt:os_sec())),
  Ts = gululog_dt:os_sec(),
  VerifyFun = fun(Idx) ->
                ?assertEqual(0, first_logid_since(Dir, Idx, Ts-1)),
                ?assertEqual(0, first_logid_since(Dir, Idx, Ts)),
                ?assertEqual(false, first_logid_since(Dir, Idx, Ts+1))
              end,
  Idx1 = append(Idx0, 0, 1, Ts),
  VerifyFun(Idx1),
  Idx2 = append(Idx1, 1, 2, Ts),
  VerifyFun(Idx2),
  Idx3 = append(Idx1, 2, 3, Ts),
  VerifyFun(Idx3),
  Idx4 = delete_from_cache(Idx3, 1),
  VerifyFun(Idx4),
  ok = flush_close(Idx4),
  ok.

seg_ts_test() ->
  {ok, Cwd} = file:get_cwd(),
  Dir = filename:join(Cwd, "gululog_idx-seg_ts_test"),
  Idx0 = init(Dir, 0, ?cache_min),
  ?assertEqual(false, get_seg_oldest_ts(Dir, Idx0, 0)),
  ?assertEqual(false, get_seg_latest_ts(Dir, Idx0, 0)),
  FunL =
    [ { fun(Idx) ->  append(Idx, 0, 1, 0) end
      , fun(Idx) ->
          ?assertEqual(0, get_seg_oldest_ts(Dir, Idx, 0)),
          ?assertEqual(0, get_seg_latest_ts(Dir, Idx, 0))
        end
      }
    , { fun(Idx) -> append(Idx, 1, 2, 1) end
      , fun(Idx) ->
          ?assertEqual(0, get_seg_oldest_ts(Dir, Idx, 0)),
          ?assertEqual(1, get_seg_latest_ts(Dir, Idx, 0))
        end
      }
    , { fun(Idx) -> switch_append(Dir, Idx, 2, 1, 42) end
      , fun(Idx) ->
          ?assertEqual(0, get_seg_oldest_ts(Dir, Idx, 0)),
          ?assertEqual(1, get_seg_latest_ts(Dir, Idx, 0)),
          ?assertEqual(42, get_seg_oldest_ts(Dir, Idx, 2)),
          ?assertEqual(42, get_seg_latest_ts(Dir, Idx, 2))
        end
      }
    , { fun(Idx) -> append(Idx, 3, 2, 52) end
      , fun(Idx) ->
          ?assertEqual(42, get_seg_oldest_ts(Dir, Idx, 2)),
          ?assertEqual(52, get_seg_latest_ts(Dir, Idx, 2))
        end
      }
    , { fun(Idx) -> switch_append(Dir, Idx, 4, 1, 62) end
      , fun(Idx) ->
          ?assertEqual(42, get_seg_oldest_ts(Dir, Idx, 2)),
          ?assertEqual(52, get_seg_latest_ts(Dir, Idx, 2)),
          ?assertEqual(62, get_seg_oldest_ts(Dir, Idx, 4))
        end
      }
    , { fun(Idx) -> switch_append(Dir, Idx, 5, 1, 72) end
      , fun(Idx) ->
          ?assertEqual(42, get_seg_oldest_ts(Dir, Idx, 2)),
          ?assertEqual(52, get_seg_latest_ts(Dir, Idx, 2)),
          ?assertEqual(62, get_seg_oldest_ts(Dir, Idx, 4)),
          ?assertEqual(72, get_seg_oldest_ts(Dir, Idx, 5))
        end
      }
    ],
  Idx = lists:foldl(
          fun({IdxOpFun, VerifyFun}, IdxIn) ->
            IdxOut = IdxOpFun(IdxIn),
            VerifyFun(IdxOut),
            IdxOut
          end, Idx0, FunL),
  %% test non-existing segments
  ?assertEqual(false, get_seg_oldest_ts(Dir, Idx, 1000)),
  ?assertEqual(false, get_seg_latest_ts(Dir, Idx, 1000)),
  ok = flush_close(Idx),
  ok.

test_dir() ->
  {ok, Dir} = file:get_cwd(),
  filename:join(Dir, "gululog_idx-gululog_idx_test_").

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
                    %% Ts = Logid for deterministic
                    append(IdxIn, LogId, Position, _Ts = LogId);
                 ({switch_append, LogId, Position}, IdxIn) ->
                    %% Ts = Logid for deterministic
                    switch_append(Dir, IdxIn, LogId, Position, _Ts = LogId)
              end, init(Dir, 0, ?cache_min), DataList),
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
          Idx0 = init(Dir, 0, ?cache_nth(1)),
          ?assertMatch(
            [ ?ENTRY(0,  _, 0, 1)
            , ?ENTRY(1,  _, 0, 10)
            , ?ENTRY(2,  _, 0, 20)
            , ?ENTRY(3,  _, 0, 30)
            , ?ENTRY(4,  _, 0, 40)
            , ?ENTRY(5,  _, 5, 1)
            , ?ENTRY(6,  _, 5, 60)
            , ?ENTRY(7,  _, 7, 1)
            , ?ENTRY(8,  _, 7, 80)
            , ?ENTRY(9,  _, 7, 90)
            , ?ENTRY(10, _, 7, 100)
            ], ets:tab2list(Idx0#idx.tid)),
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
          ?assertMatch(
            [ ?ENTRY(0, _, 0, 1)
            , ?ENTRY(1, _, 0, 10)
            , ?ENTRY(2, _, 0, 20)
            ], ets:tab2list(Idx16#idx.tid)),
          %% truncate to the very beginning
          LogId5 = 0,
          {SegId5, _} = locate(Dir, Idx16, LogId5),
          {Idx17, _}  = truncate(Dir, Idx16, SegId5, LogId5, ?undef),
          ?assertEqual(0, Idx17#idx.segid),
          ?assertEqual([], ets:tab2list(Idx17#idx.tid)),
          %% re-init
          ok = flush_close(Idx17),
          NewIdx = init(Dir, 0, ?cache_nth(3)),
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
          Idx0 = init(Dir, 0, ?cache_all),
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
          Idx0 = init(Dir, 0, ?cache_min),
          ?assertMatch({_, {0, _}}, delete_oldest_seg(Dir, Idx0, ?undef)),
          Files = lists:sort(wildcard_reversed(Dir)),
          {Idx1, Truncated} = truncate(Dir, Idx0, 5, 5, ?undef),
          FilesDeleted = lists:sort(lists:map(fun({?OP_DELETED, Fn}) -> Fn end, Truncated)),
          ?assertEqual(Files, FilesDeleted),
          ?assertEqual(5, Idx1#idx.segid)
        end
      }
    , { "delete + find logid by ts"
      , fun() ->
          Dir = test_dir(),
          Idx0 = init(Dir, 0, ?cache_all),
          TsVerifyFun =
            fun(Idx) ->
              lists:foreach(fun(_Ts = LogId) ->
                              %% LogId as Ts for deterministic
                              ?assertEqual(LogId, first_logid_since(Dir, Idx, LogId))
                            end, lists:seq(0,10))
            end,
          ok = TsVerifyFun(Idx0),
          %% delete the last entry in segmeng 0 from cache
          Idx1 = delete_from_cache(Idx0, 4),
          ok = TsVerifyFun(Idx1),
          %% delete the second last entry in segment 0 from cache
          Idx2 = delete_from_cache(Idx1, 3),
          ok = TsVerifyFun(Idx2),
          %% delete all possible entries
          Idx3 = lists:foldl(fun(LogId, IdxIn) ->
                               delete_from_cache(IdxIn, LogId)
                             end, Idx2, lists:seq(0,10)),
          ok = TsVerifyFun(Idx3)
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
