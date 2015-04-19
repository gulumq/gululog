%% @doc Log reader cursor

-module(gululog_r_cur).

-export([ open/2
        , read/2
        ]).

-export_type([cursor/0]).

-include("gululog_priv.hrl").

-type meta() :: gululog_meta:meta().
-type header() :: binary().
-type body() :: binary().
-type log() :: gululog().
-type optkey() :: sikip_body.
-type option() :: optkey() | {optkey(), term()}.
-type options() :: [option()].

-record(rcur, { version  :: logvsn()
              , segid    :: segid()
              , fd       :: file:fd()
              , meta     :: undefined | meta()
              , ptr_at   :: meta | header | body %% pointer at
              , position :: position()
              }).

-opaque cursor() :: #rcur{}.

%% @doc Open segment file in 'raw' mode for a reader.
-spec open(dirname(), segid()) -> empty | {ok, cursor()} | no_return().
open(Dir, SegId) ->
  FileName = mk_name(Dir, SegId),
  {ok, Fd} = file:open(FileName, [read, raw, binary]),
  try read_version(Fd) of
    empty ->
      file:close(Fd),
      empty;
    VSN when is_integer(VSN) andalso VSN =< ?LOGVSN ->
      #rcur{ version  = VSN
           , segid    = SegId
           , fd       = Fd
           , meta     = undefined
           , ptr_at   = meta
           , position = 1
           }
  catch C : E ->
    file:close(Fd),
    erlang:raise(C, E, erlang:get_stacktrace())
  end.

%% @doc Read one log including head and body.
-spec read(cursor(), options()) -> {cursor(), log()}.
read(#rcur{version = Version} = Cursor0, Options) ->
  #rcur{meta = Meta} = Cursor1 = read_meta(Cursor0),
  {Cursor2, Header} = read_header(Cursor1),
  {Cursor,  Body} = maybe_read_body(Cursor2, Options),
  ok = gululog_meta:assert_data_integrity(Version, Meta, [Header, Body]),
  {Cursor, #gululog{ logid     = gululog_meta:logid(Meta)
                   , logged_on = gululog_meta:logged_on(Meta)
                   , header    = Header
                   , body      = Body
                   }}.

%%% PRIVATE FUNCTIONS

%% @private Read log meta data.
-spec read_meta(cursor()) -> cursor().
read_meta(#rcur{ version  = Version
               , fd       = Fd
               , ptr_at   = meta
               , meta     = undefined
               , position = Position
               } = Cursor0) ->
  Bytes = gululog_meta:bytecnt(Version),
  {ok, MetaBin} = file:read(Fd, Bytes),
  Meta = gululog_meta:decode(Version, MetaBin),
  Cursor0#rcur{ ptr_at   = header
              , meta     = Meta
              , position = Position + Bytes
              }.

%% @private Read log header.
-spec read_header(cursor()) -> {cursor(), header()}.
read_header(#rcur{ fd       = Fd
                 , ptr_at   = header
                 , meta     = Meta
                 , position = Position
                 } = Cursor0) ->
  Bytes = gululog_meta:header_size(Meta),
  {ok, Header} = file:read(Fd, Bytes),
  Cursor = Cursor0#rcur{ ptr_at   = body
                       , position = Position + Bytes
                       },
  {Cursor, Header}.

%% @private Read log body, return the new cursor and the log body binary
%% in case skip_body is given in read options, undefined is returned
%% the fd is positioned to the beginning of the nex log
%% @end
-spec maybe_read_body(cursor(), options()) ->
        {cursor(), undefined | body()} | no_return().
maybe_read_body(#rcur{ fd       = Fd
                     , ptr_at   = header
                     , meta     = Meta
                     , position = Position
                     } = Cursor0, Options) ->
  Bytes = gululog_meta:body_size(Meta),
  Body =
    case proplists:get_bool(skip_body, Options) of
      true ->
        {ok, _} = file:position(Fd, Position + Bytes),
        undefined;
      false ->
        {ok, Body_} = file:read(Fd, Bytes),
        Body_
    end,
  Cursor = Cursor0#rcur{ ptr_at   = meta
                       , position = Position + Bytes
                       },
  {Cursor, Body}.

%% @private Read the first byte version number, position fd to location 1
%% Return 'empty' in case the file is empty or contains only a version byte
%% @end
-spec read_version(file:fd()) -> empty | logvsn() | no_return().
read_version(Fd) ->
  case file:read(Fd, 1) of
    eof                      -> empty;
    {ok, <<Version:8>>}      -> Version;
    {error, Reason}          -> erlang:error(Reason)
  end.

mk_name(Dir, SegId) ->
  gululog_name:from_segid(Dir, SegId) ++ ?SEG_SUFFIX.

