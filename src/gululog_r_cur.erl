%% @doc Log reader cursor

-module(gululog_r_cur).

-export([ open/2
        , read/1
        , read/2
        , close/1
        , current_position/1
        , reposition/2
        ]).

-export_type([cursor/0]).

%%%*_ MACROS and SPECS =========================================================

-include("gululog_priv.hrl").

-type meta() :: gululog_meta:meta().
-type log() :: gululog().
-type optkey() :: sikip_body.
-type option() :: optkey() | {optkey(), term()}.
-type options() :: [option()].

-record(rcur, { version  :: logvsn()
              , segid    :: segid()
              , fd       :: file:fd()
              , meta     :: ?undef | meta()
              , ptr_at   :: meta | header | body %% pointer at
              , position :: position()
              }).

-opaque cursor() :: #rcur{}.

%%%*_ API FUNCTIONS ============================================================

%% @doc Open segment file in 'raw' mode for a reader.
-spec open(dirname(), segid()) -> empty | cursor() | no_return().
open(Dir, SegId) ->
  FileName = mk_name(Dir, SegId),
  {ok, Fd} = file:open(FileName, [read, raw, binary, read_ahead]),
  try read_version(Fd) of
    empty ->
      ok = file:close(Fd),
      empty;
    VSN when is_integer(VSN) andalso VSN =< ?LOGVSN ->
      #rcur{ version  = VSN
           , segid    = SegId
           , fd       = Fd
           , meta     = ?undef
           , ptr_at   = meta
           , position = 1
           }
  catch C : E ->
    ok = file:close(Fd),
    erlang:raise(C, E, erlang:get_stacktrace())
  end.

%% @doc Close reader cursor.
-spec close(cursor()) -> ok.
close(#rcur{fd = Fd}) -> file:close(Fd).

%% @doc Read one log including head and body.
-spec read(cursor()) -> {cursor(), log()} | eof.
read(Cur) -> read(Cur, []).

%% @doc Read one log including head and maybe body.
-spec read(cursor(), options()) -> {cursor(), log()} | eof.
read(#rcur{version = Version} = Cursor0, Options) ->
  case read_meta(Cursor0) of
    eof ->
      eof;
    #rcur{meta = Meta} = Cursor1 ->
      {Cursor2, Header} = read_header(Cursor1),
      {Cursor,  Body} = maybe_read_body(Cursor2, Options),
      ok = gululog_meta:assert_data_integrity(Version, Meta, Header, Body),
      Log = #gululog{ logid  = gululog_meta:logid(Meta)
                    , header = Header
                    , body   = Body
                    },
      {Cursor, Log}
  end.

%% @doc Reposition the cursor position.
-spec reposition(cursor(), position()) -> cursor().
reposition(#rcur{fd = Fd} = Cur, Position) ->
  {ok, Position} = file:position(Fd, Position),
  Cur#rcur{ meta     = ?undef
          , ptr_at   = meta
          , position = Position
          }.

%% @doc Get current fd pointer position.
-spec current_position(cursor()) -> position().
current_position(#rcur{position = Position}) -> Position.

%%%*_ PRIVATE FUNCTIONS ========================================================

%% @private Read log meta data.
-spec read_meta(cursor()) -> cursor() | eof.
read_meta(#rcur{ version  = Version
               , fd       = Fd
               , ptr_at   = meta
               , meta     = ?undef
               , position = Position
               } = Cursor0) ->
  Bytes = gululog_meta:bytecnt(Version),
  case file:read(Fd, Bytes) of
    {ok, MetaBin} ->
      Meta = gululog_meta:decode(Version, MetaBin),
      Cursor0#rcur{ ptr_at   = header
                  , meta     = Meta
                  , position = Position + Bytes
                  };
    eof ->
      eof
  end.

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
%% the fd is positioned to the beginning of the next log
%% @end
-spec maybe_read_body(cursor(), options()) ->
        {cursor(), ?undef | body()}.
maybe_read_body(#rcur{ fd       = Fd
                     , ptr_at   = body
                     , meta     = Meta
                     , position = Position
                     } = Cursor0, Options) ->
  Bytes = gululog_meta:body_size(Meta),
  Body =
    case proplists:get_bool(skip_body, Options) of
      true ->
        {ok, _} = file:position(Fd, Position + Bytes),
        ?undef;
      false ->
        {ok, Body_} = file:read(Fd, Bytes),
        Body_
    end,
  Cursor = Cursor0#rcur{ ptr_at   = meta
                       , meta     = ?undef
                       , position = Position + Bytes
                       },
  {Cursor, Body}.

%% @private Read the first byte version number, position fd to location 1
%% Return 'empty' in case the file is empty or contains only a version byte
%% @end
-spec read_version(file:fd()) -> empty | logvsn().
read_version(Fd) ->
  case file:read(Fd, 1) of
    eof                                         -> empty;
    {ok, <<Version:8>>} when Version =< ?LOGVSN -> Version
  end.

mk_name(Dir, SegId) -> gululog_name:mk_seg_name(Dir, SegId).

%%%*_ TESTS ====================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

open_fail_test() ->
  {ok, Cwd} = file:get_cwd(),
  SegFile = gululog_name:mk_seg_name(Cwd, 999),
  ok = file:write_file(SegFile, <<255>>),
  ?assertException(error, {case_clause, _}, open(Cwd, 999)).

-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
