%% @doc Log segment read/write cursor.
%%
%% log entry binary layout
%% <<LogID:64, Timestamp:64,
%%   HeaderSize:32, BodySize:32,
%%   Header/binary, Body/binary>>

-module(gululog_cur).

-export([reader_open/2]).
-export([writer_open/3]).
-export([append/3]).
-export([close/1]).

-export_type([cursor/0]).

-include("gululog.hrl").

-record(cur, { version  :: logvsn()
             , segid    :: segid()    %% segment id
             , logid    :: logid()    %% logid for next new log entry
             , position :: position() %% position for next new log entry
             , fd       :: file:fd()  %% fd for read/write
             }).

-record(meta, { logid       :: logid()
              , timestamp   :: micro()
              , header_size :: bytecnt()
              , body_size   :: bytecnt()
              }).

-opaque cursor() :: #cur{}.
-type meta() :: #meta{}.

-define(FILE_SUFFIX, ".log").
-define(TAILER_BYTES, 4).

%% @doc Open segment file in 'raw' mode for a reader.
-spec reader_open(dirname(), segid()) -> cursor() | no_return().
reader_open(Dir, SegId) ->
  FileName = mk_name(Dir, SegId),
  {ok, Fd} = file:open(FileName, reader_modes()),
  Version = read_version(Fd),
  #cur{ version  = Version
      , segid    = SegId
      , logid    = SegId %% assume first entry have segid = logid
      , position = 1
      , fd       = Fd
      }.

%% @doc Open segment file in 'raw' mode for writer.
%% LastIndexedPosition is the position of the last logid in index file.
%% pass in LastIndexedPosition = 0 for a new segment
%% @end
-spec writer_open(dirname(), segid(), position()) -> cursor() | no_return().
writer_open(Dir, SegId, LastIndexedPosition) ->
  FileName = mk_name(Dir, SegId),
  {ok, Fd} = file:open(FileName, writer_modes()),
  case LastIndexedPosition =:= 0 of
    true ->
      %% a new segment
      ok = file:write(Fd, <<?LOGVSN:8>>),
      #cur{ version  = ?LOGVSN
          , segid    = SegId
          , logid    = SegId
          , position = 1
          , fd       = Fd
          };
    false ->
      Version = read_version(Fd),
      Meta = read_meta(Version, Fd, LastIndexedPosition),
      LastLogSize = log_size(Version, Meta),
      {ok, Position} = file:position(Fd, LastIndexedPosition + LastLogSize),
      #cur{ version  = Version
          , segid    = SegId
          , logid    = Meta#meta.logid + 1
          , position = Position
          , fd       = Fd
          }
  end.

%% @doc Close fd for either writer or reader.
-spec close(cursor()) -> ok | no_return().
close(#cur{fd = Fd}) -> ok = file:close(Fd).

%% @doc Append one log entry.
-spec append(cursor(), binary(), binary()) -> cursor().
append(#cur{ version  = Version
           , logid    = LogId
           , fd       = Fd
           , position = Position
           } = Cursor, Header, Body) ->
  Timestamp = gululog_dt:os_micro(),
  Meta = meta(Version, LogId, Timestamp, size(Header), size(Body)),
  ok = file:write(Fd, [Meta, Header, Body]),
  Cursor#cur{ logid    = LogId + 1 %% next log id
            , position = Position + log_size(Version, Meta) %% next position
            }.

%% PRIVATE FUNCTIONS

%% @private Read the first byte version number, position fd to location 1
-spec read_version(file:fd()) -> logvsn().
read_version(Fd) ->
  {ok, <<Version:8>>} = file:pread(Fd, 0, 1),
  Version.

mk_name(Dir, SegId) -> gululog_name:from_segid(Dir, SegId) ++ ?FILE_SUFFIX.

reader_modes() -> [read, raw, binary].

writer_modes() -> [write | reader_modes()].

-spec read_meta(logvsn(), file:fd(), position()) -> meta().
read_meta(1, Fd, Position) ->
  {ok, MetaBin} = file:pread(Fd, Position, meta_size(1)),
  <<LogId:64, Timestamp:64, HeaderSize:32, BodySize:32>> = MetaBin,
  #meta{ logid       = LogId
       , timestamp   = Timestamp
       , header_size = HeaderSize
       , body_size   = BodySize
       }.

-spec meta(logvsn(), logid(), micro(), bytecnt(), bytecnt()) -> binary().
meta(1, LogId, Timestamp, HeaderSize, BodySize) ->
   <<LogId:64, Timestamp:64, HeaderSize:32, BodySize:32>>.

-spec meta_size(logvsn()) -> bytecnt().
meta_size(1) -> 24.

-spec log_size(logvsn(), meta()) -> bytecnt().
log_size(1, #meta{header_size = HeaderSize, body_size = BodySize}) ->
  meta_size(1) + HeaderSize + BodySize.

