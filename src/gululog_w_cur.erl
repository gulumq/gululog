%% @doc Log segment writer cursor.
%%
%% log entry binary layout
%% <<LogID:64, Timestamp:64,
%%   HeaderSize:32, BodySize:32,
%%   Header/binary, Body/binary>>

-module(gululog_w_cur).

-export([open/3]).
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

-opaque cursor() :: #cur{}.
-type header() :: binary().
-type body() :: binary().

-define(FILE_SUFFIX, ".log").
-define(TAILER_BYTES, 4).

%% @doc Open segment file in 'raw' mode for writer.
%% LastIndexedPosition is the position of the last logid in index file.
%% pass in LastIndexedPosition = 0 for a new segment
%% @end
-spec open(dirname(), segid(), position()) -> cursor() | no_return().
open(Dir, SegId, LastIndexedPosition) ->
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
      LastLogSize = log_size(Meta),
      {ok, Position} = file:position(Fd, LastIndexedPosition + LastLogSize),
      #cur{ version  = Version
          , segid    = SegId
          , logid    = gululog_meta:logid(Meta)
          , position = Position
          , fd       = Fd
          }
  end.

%% @doc Close fd for either writer or reader.
-spec close(cursor()) -> ok | no_return().
close(#cur{fd = Fd}) -> ok = file:close(Fd).

%% @doc Append one log entry.
-spec append(cursor(), header(), body()) -> cursor().
append(#cur{ version  = Version
           , logid    = LogId
           , fd       = Fd
           , position = Position
           } = Cursor, Header, Body) ->
  Timestamp = gululog_dt:os_micro(),
  Meta = cululog_mets:new(Version, LogId, Timestamp, size(Header), size(Body)),
  ok = file:write(Fd, [Meta, Header, Body]),
  Cursor#cur{ logid    = LogId + 1 %% next log id
            , position = Position + log_size(Meta) %% next position
            }.

%% PRIVATE FUNCTIONS

%% @private Read the first byte version number, position fd to location 1
-spec read_version(file:fd()) -> logvsn().
read_version(Fd) ->
  {ok, <<Version:8>>} = file:pread(Fd, 0, 1),
  Version.

mk_name(Dir, SegId) -> gululog_name:from_segid(Dir, SegId) ++ ?FILE_SUFFIX.

writer_modes() -> [write, read, raw, binary].

-spec read_meta(logvsn(), file:fd(), position()) -> gululog_meta:meta().
read_meta(Version, Fd, Position) ->
  {ok, MetaBin} = file:pread(Fd, Position, gululog_meta:bytecnt(Version)),
  gululog_meta:decode(Version, MetaBin).

log_size(Meta) -> gululog_meta:calculate_log_size(Meta).

