%% @doc LogId -> {SegId, Position} index.
%% LogId: Strict monotonic non-negative positive integer ID
%% SegId: The fist log ID in a log segment file, SegId is used as file name.
%% Position: Log byte position (see file:position/1) in a segment file

-module(gululog_idx).

%% APIs for writer (owner)
-export([ init/1           %% Initialize log index from the given log file directory
        , close/1          %% close the writer cursor
        , append/3         %% Append a new log entry to index
        , switch/3         %% switch to a new segment
        , switch_append/4  %% switch then append
        , delete/2         %% Delete oldest segment from index
        ]).

%% APIs for readers (public access)
-export([ locate/3         %% Locate {SegId, Position} for a given LogId
        , get_last_logid/1 %% last logid in ets
        ]).

-export_type([index/0]).

%%%*_ MACROS and SPECS =========================================================

-include("gululog_priv.hrl").

-record(idx, { version :: logvsn()
             , segid   :: segid()
             , fd      :: file:fd()
             , tid     :: ets:tid()
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
  SegId = gululog_name:to_segid(LatestSegment),
  {Version, WriterFd} = open_writer_fd(IsNew, LatestSegment),
  Tid = ets:new(?MODULE, [ ordered_set
                         , public
                         , {read_concurrency, true} ]),
  ok = init_ets_from_index_files(Tid, IndexFiles),
  #idx{ version = Version
      , segid   = SegId
      , fd      = WriterFd
      , tid     = Tid
      }.

%% @doc Close write fd, delete ets cache table.
-spec close(index()) -> ok.
close(#idx{fd = Fd, tid = Tid}) ->
  ok = file:sync(Fd),
  ok = file:close(Fd),
  true = ets:delete(Tid),
  ok.

%% @doc Append a new index entry.
%% NB: There is no validation on the new LogId and Position to be appended
%% 1. LogId should be equal to SegId when appending to a new segment
%% 2. LogId should be monotonic. i.e. NewLogId >= LastLogId + 1
%% 3. Position should be (at least MIN_LOG_SIZE) greater than the last position
%% @end
-spec append(index(), logid(), position()) -> ok | no_return().
append(#idx{ version = ?LOGVSN
           , segid   = SegId
           , fd      = Fd
           , tid     = Tid
           }, LogId, Position) ->
  ok = file:write(Fd, ?TO_FILE_ENTRY(SegId, LogId, Position)),
  ets:insert(Tid, ?ETS_ENTRY(SegId, LogId, Position)),
  ok.

%% @doc Delete oldest segment from index
%% return 'ok'
%% @end
-spec delete(dirname(), index()) -> ok.
delete(Dir, #idx{tid = Tid} = Index) ->
  case get_oldest_segid(Index) of
    false ->
      ok;
    SegId ->
      ets:select_delete(Tid, [{{'$1', {'$2', '$3'}},
                              [{'=:=', SegId, '$2'}],
                              [true]}]),
      file:delete(mk_name(Dir, SegId)),
      ok
  end.

%% @doc Switch to a new log segment
-spec switch(dirname(), index(), segid()) -> index().
switch(Dir, #idx{fd = Fd} = Idx, NewSegId) ->
  {?LOGVSN, NewFd} = open_writer_fd(_IsNew = true, mk_name(Dir, NewSegId)),
  ok = file:close(Fd),
  Idx#idx{segid = NewSegId, fd = NewFd}.

%% @doc Switch to a new log segment, append new index entry.
-spec switch_append(dirname(), index(), logid(), position()) -> index().
switch_append(Dir, Idx, LogId, Position) ->
  NewIdx = switch(Dir, Idx, LogId),
  ok = append(NewIdx, LogId, Position),
  NewIdx.

%% @doc Locate {SegId, Position} for a given LogId
%% return {segid(), position()} if the given LogId is found
%% return 'false' if not in valid range
%% @end
-spec locate(dirname(), index(), logid()) ->
        {segid(), position()} | false | no_return().
locate(Dir, #idx{tid = Tid}, LogId) ->
  case ets:lookup(Tid, LogId) of
    [] ->
      case is_out_of_range(Tid, LogId) of
        true ->
          false;
        false ->
          PrevLogId = ets:prev(Tid, LogId),
          [?ETS_ENTRY(SegId, _, _)] = ets:lookup(Tid, PrevLogId),
          scan_locate(Dir, SegId, LogId)
      end;
    [?ETS_ENTRY(SegId, LogId, Position)] ->
      {SegId, Position}
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
  Location = (LogId - SegId - 1) * ?FILE_ENTRY_BYTES_V1 + 1,
  {ok, Bin} = file:pread(Fd, Location, ?FILE_ENTRY_BYTES_V1),
  ?ETS_ENTRY(SegId, LogId, Position) = ?FROM_FILE_ENTRY_V1(SegId, Bin),
  {SegId, Position}.

%% @private Check if the given log ID is out of indexing range.
%% 'true' when trying to locate a 'future' log
%% or e.g. an old segment has been removed.
%% @end
-spec is_out_of_range(ets:tid(), logid()) -> boolean().
is_out_of_range(Tid, LogId) ->
  Last = ets:last(Tid),
  (Last =:= '$end_of_table') orelse %% empty table
  (Last < LogId)             orelse %% too new
  (ets:first(Tid) > LogId).         %% too old

%% @private Create ets table to keep the index entries.
%% TODO: less indexing for earlier segments in case there are too many entries.
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
wildcard_reverse(Dir) ->
  gululog_name:wildcard_full_path_name_reversed(Dir, ?DOT_IDX).

%% @private Open 'raw' mode fd for writer to 'append'.
-spec open_writer_fd(boolean(), filename()) -> file:fd() | no_return().
open_writer_fd(IsNew, FileName) ->
  {ok, Fd} = file:open(FileName, [write, read, raw, binary]),
  %% Write the first 1 byte version number in case it's a new file
  [ok = file:write(Fd, <<?LOGVSN:8>>) || IsNew],
  {ok, <<Version:8>>} = file:pread(Fd, 0, 1),
  _ = file:position(Fd, eof),
  {Version, Fd}.

%% @private Open 'raw' mode fd for reader.
open_reader_fd(FileName) ->
  {ok, Fd} = file:open(FileName, [read, raw, binary]),
  Fd.

%% @private Make index file path/name
-spec mk_name(dirname(), segid()) -> filename().
mk_name(Dir, SegId) -> gululog_name:from_segid(Dir, SegId) ++ ?DOT_IDX.

%% @private Get oldest segid from index
-spec get_oldest_segid(index()) -> logid() | false.
get_oldest_segid(#idx{tid = Tid}) ->
  case ets:first(Tid) of
    '$end_of_table' ->
      false;
    LogId ->
      [{LogId, {SegId, _}}] = ets:lookup(Tid, LogId),
      SegId
  end.

%%%*_ TESTS ====================================================================

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
