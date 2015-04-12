%% @doc LogId -> {BaseLogId, Position} index.
%% LogId: Strict monotonic non-negative positive integer ID
%% BaseLogId: The fist log ID in a log segment file, BaseLogId is used as file name.
%% Position: Log byte position (see file:position/1) in a segment file

-module(gululog_idx).

%% APIs
-export([init/1]).   %% Initialize log index from the given log file directory
-export([append/3]). %% Append a new log entry to index
-export([bump/2]). %% bump to a new segment
-export([bump_append/3]). %% bump then append
-export([locate/2]). %% Locate {SegId, Position} for a given LogId
-export([get_last_logid/1]). %% last logid in ets

-export_type([index/0]).

-include("gululog.hrl").

-record(idx, { dir   :: dirname()
             , segid :: segid()
             , fd    :: file:fd()
             , tid   :: ets:tid()
             }).

-opaque index() :: #idx{}.

-define(ETS_ENTRY(SegId, LogId, Position),
        {LogId, {SegId, Position}}).
-define(TO_FILE_ENTRY(SegId, LogId, Position),
        <<(LogId - SegId):32, Position:32>>).
-define(FROM_FILE_ENTRY(SegId, FileEntryBin),
        begin
          <<Offset:32, Position:32>> = FileEntryBin,
          ?ETS_ENTRY(SegId, SegId + Offset, Position)
        end).
-define(FILE_ENTRY_BYTES, 8). %% Number of bytes in foile per index entry.
-define(FILE_ENTRY_BITS, 64). %% Number of bits in foile per index entry.
-define(FILE_READ_CHUNK, (1 bsl 20)). %% Number of index entries per file read.

%% @doc Initialize log index in the given directory.
%% The directory is created if not exists already
%% New index file is initialized if the given directry is empty
%% @end
-spec init(dirname()) -> index() | {error, no_return()}.
init(DirName) ->
  ok = filelib:ensure_dir(filename:join(DirName, "foo")),
  IndexFiles = wildcard_reverse(DirName),
  LatestSegment = hd(IndexFiles),
  SegId = gululog_name:to_segid(LatestSegment),
  WriterFd = open_writer_fd(LatestSegment),
  Tid = ets:new(?MODULE, [ ordered_set
                         , public
                         , {read_concurrency, true} ]),
  ok = init_ets_from_index_files(Tid, IndexFiles),
  #idx{ dir   = DirName
      , segid = SegId
      , fd    = WriterFd
      , tid   = Tid
      }.

%% @doc Append a new index entry.
%% NB: There is no validation on the new LogId and Position to be appended
%% 1. LogId should be equal to SegId when appending to a new segment
%% 2. LogId should be strict monotonic. i.e. NewLogId = LastLogId + 1
%% 3. Position should be (at least MIN_LOG_SIZE) greater than the last position
%% @end
-spec append(index(), logid(), position()) -> ok | no_return().
append(#idx{segid = SegId, fd = Fd, tid = Tid}, LogId, Position) ->
  ok = file:write(Fd, ?TO_FILE_ENTRY(SegId, LogId, Position)),
  ets:insert(Tid, ?ETS_ENTRY(SegId, LogId, Position)),
  ok.

%% @doc Bump to a new log segment
-spec bump(index(), segid()) -> index().
bump(#idx{dir = Dir, fd = Fd} = Idx, NewSegId) ->
  NewFd = open_writer_fd(mk_name(Dir, NewSegId)),
  ok = file:close(Fd),
  Idx#idx{segid = NewSegId, fd = NewFd}.

%% @doc Bump to a new log segment, append new index entry.
-spec bump_append(index(), logid(), position()) -> index().
bump_append(Idx, LogId, Position) ->
  NewIdx = bump(Idx, LogId),
  ok = append(NewIdx, LogId, Position),
  NewIdx.

%% @doc Locate {SegId, Position} for a given LogId
%% return 'false' if not found
%% @end
-spec locate(index(), logid()) -> {segid(), position()} | false.
locate(#idx{tid = Tid}, LogId) ->
  case ets:lookup(Tid, LogId) of
    []                                   -> false;
    [?ETS_ENTRY(SegId, LogId, Position)] -> {SegId, Position}
  end.

%% @doc Get last logid from index.
%% return 'false' iif it is an empty index.
%% @end
-spec get_last_logid(index()) -> logid() | false.
get_last_logid(#idx{tid = Tid}) ->
  case ets:last(Tid) of
    '$end_of_table' -> false;
    LogId           -> LogId
  end.

%% INTERNAL FUNCTIONS

%% @private Create ets table to keep the index entries.
%% TODO: scarce indexing for earlier segments in case there are too many entries.
%% @end
-spec init_ets_from_index_files(ets:tid(), [filename()]) -> ok | no_return().
init_ets_from_index_files(_Tid, []) -> ok;
init_ets_from_index_files(Tid, [FileName | Rest]) ->
  SegId = gululog_name:to_segid(FileName),
  Fd = open_reader_fd(FileName),
  try
    {ok, <<Version:8>>} = file:read(Fd, 1),
    ok = init_ets_from_index_file(Version, Tid, SegId, Fd),
    init_ets_from_index_files(Tid, Rest)
  after
    file:close(Fd)
  end.

-spec init_ets_from_index_file(logvsn(), ets:tid(), segid(), file:fd()) -> ok | no_return().
init_ets_from_index_file(_Version = 1, Tid, SegId, Fd) ->
  case file:read(Fd, ?FILE_ENTRY_BYTES* ?FILE_READ_CHUNK) of
    eof ->
      ok;
    {ok, ChunkBin} ->
      [ ets:insert(Tid, ?FROM_FILE_ENTRY(SegId, Entry))
        || <<Entry:?FILE_ENTRY_BITS>> <= ChunkBin ],
      init_ets_from_index_file(1, Tid, SegId, Fd)
  end.

%% @private Find all the index files in the given directory
%% return all filenames in reversed order.
%% make one if the given directory is empty.
%% @end
-spec wildcard_reverse(dirname()) -> [filename()].
wildcard_reverse(DirName) ->
  case lists:reverse(lists:sort(filelib:wildcard("*.idx", DirName))) of
    [] -> [mk_name(DirName, 0)];
    L  -> L
  end.

%% @private Open 'raw' mode fd for writer to 'append'.
-spec open_writer_fd(filename()) -> file:fd() | no_return().
open_writer_fd(FileName) ->
  IsNew = ({error, enoent} =:= file:read_file_info(FileName)),
  {ok, Fd} = file:open(FileName, [append, raw, binary]),
  %% Write the first 1 byte version number in case it's a new file
  [ok = file:write(Fd, <<?LOGVSN:8>>) || IsNew],
  Fd.

%% @private Open 'raw' mode fd for reader.
open_reader_fd(FileName) ->
  {ok, Fd} = file:open(FileName, [read, raw, binary]),
  Fd.

%% @private Make index file path/name
-spec mk_name(dirname(), segid()) -> filename().
mk_name(Dir, LogId) -> filename:join(Dir, mk_name(LogId)).

%% @private Make index file name.
-spec mk_name(segid()) -> filename().
mk_name(SegId) -> gululog_name:basename(SegId) ++ ".idx".

