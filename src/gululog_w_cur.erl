%% @doc Log segment writer cursor.
%%
%% log entry binary layout
%% [ <<Meta/binary>> %% see gululog_meta.erl
%% , <<Header/binary, Body/binary>>
%% ]

-module(gululog_w_cur).

-export([open/1]).
-export([append/4]).
-export([close/1]).

-export_type([cursor/0]).

-include("gululog_priv.hrl").

-record(wcur, { version  :: logvsn()
              , segid    :: segid()    %% segment id
              , position :: position() %% position for next new log entry
              , fd       :: file:fd()  %% fd for read/write
              }).

-opaque cursor() :: #wcur{}.
-type header() :: binary().
-type body() :: binary().

-define(TAILER_BYTES, 4).

%% @doc Open the last segment file the given directory for writer to append.
%% @end
-spec open(dirname()) -> cursor() | no_return().
open(Dir) ->
  FileName = case wildcard_reverse(Dir) of
               []    -> mk_name(Dir, 0);
               [F|_] -> F
             end,
  {ok, Fd} = file:open(FileName, [write, read, raw, binary]),
  SegId = gululog_name:to_segid(FileName),
  case SegId =:= 0 of
    true  ->
      ok = file:write(Fd, <<?LOGVSN:8>>),
      #wcur{ version  = ?LOGVSN
           , segid    = SegId
           , position = 1
           , fd       = Fd
           };
    false ->
      {ok, <<Version:8>>} = file:read(Fd, 1),
      true = (Version =< ?LOGVSN), %% assert
      {ok, Position}  = file:position(Fd, eof),
      #wcur{ version  = Version
           , segid    = SegId
           , position = Position
           , fd       = Fd
           }
  end.

%% @doc Close fd for either writer or reader.
-spec close(cursor()) -> ok | no_return().
close(#wcur{fd = Fd}) -> ok = file:close(Fd).

%% @doc Append one log entry.
-spec append(cursor(), logid(), header(), body()) -> cursor().
append(#wcur{ version  = Version
            , fd       = Fd
            , position = Position
            } = Cursor, LogId, Header, Body) ->
  Meta = gululog_meta:new(Version, LogId, size(Header), size(Body)),
  MetaBin = gululog_meta:encode(Version, Meta, [Header, Body]),
  ok = file:write(Fd, [MetaBin, Header, Body]),
  NewPosition = Position + gululog_meta:calculate_log_size(Version, Meta),
  Cursor#wcur{position = NewPosition}.

%% PRIVATE FUNCTIONS

%% @private Make a segment file name.
mk_name(Dir, SegId) ->
  gululog_name:from_segid(Dir, SegId) ++ ?SEG_SUFFIX.

%% @private Find all the index files in the given directory
%% return all filenames in reversed order.
%% @end
-spec wildcard_reverse(dirname()) -> [filename()].
wildcard_reverse(Dir) ->
    gululog_name:wildcard_full_path_name_reversed(Dir, ?SEG_SUFFIX).

