
-module(gululog_r_cur).

-export([ open/2
        , read/1
        , read_meta/1
        , read_header/1
        , read_body/1
        ]).

-export_type([cursor/0]).

-include("gululog.hrl").

-define(FILE_SUFFIX, ".log").

-type op() :: {position, position()} | read_meta | read_header | read_body.
-type header() :: binary().
-type body() :: binary().
-type meta() :: gululog_meta:meta().
-type cur_result() :: ok | eof | meta() | header() | body().
-opaque cursor() :: fun((op()) -> {cursor(), cur_result()} | no_return()).
-type log() :: {logid(), micro(), header(), body()}.

%% @doc Open segment file in 'raw' mode for a reader.
-spec open(dirname(), segid()) -> cursor() | no_return().
open(Dir, SegId) ->
  FileName = mk_name(Dir, SegId),
  {ok, Fd} = file:open(FileName, [read, raw, binary]),
  Version = read_version(Fd),
  cursor(read_meta, Fd, Version).

%% @doc Read one log including head and body.
-spec read(cursor()) -> {cursor(), log()}.
read(Cursor0) ->
  {Cursor1, Meta}   = read_meta(Cursor0),
  {Cursor2, Header} = read_header(Cursor1),
  {Cursor,  Body}   = read_body(Cursor2),
  LogId = gululog_meta:logid(Meta),
  Ts    = gululog_meta:timestamp(Meta),
  {Cursor, {LogId, Ts, Header, Body}}.

-spec read_meta(cursor()) -> {cursor(), meta()} | eof.
read_meta(Cursor) -> Cursor(read_meta).

-spec read_header(cursor()) -> {cursor(), header()}.
read_header(Cursor) -> Cursor(read_header).

-spec read_body(cursor()) -> {cursor(), body()}.
read_body(Cursor) -> Cursor(read_body).

%% INTERNAL FUNCTIONS

-spec cursor(op(), file:fd(), term()) -> cursor().
cursor(Op, Fd, Arg) ->
  fun({position, Position}) ->
    case Op =:= read_meta of
      true ->
        {ok, Position} = file:position(Fd, Position),
        {cursor(read_meta, Fd, Arg), ok};
      false ->
        erlang:throw({bad_op, [ {unexpected, position}
                              , {arg, Arg}
                              ]})
    end;
    (Op_) ->
    case Op_ =:= Op of
      true  ->
        try
          read_and_move(Op, Fd, Arg)
        catch C : E ->
          _ = file:close(Fd),
          erlang:throw({C, E, erlang:get_stacktrace()})
        end;
      false ->
        _ = file:close(Fd),
        erlang:throw({bad_op, [ {expected, Op}
                              , {got, Op_}
                              , {arg, Arg}]})
    end
  end.

-spec read_and_move(op(), file:fd(), logvsn() | meta()) ->
        eof | {cursor(), cur_result()} | no_return().
read_and_move(meta, Fd, Version) ->
  case file:read(Fd, gululog_meta:bytecnt(Version)) of
    eof ->
      %% we assume here is the only place to deal with eof.
      %% Meaning: a reader should never endup catching up to the
      %% position where a writer is actively writting to
      ok = file:close(Fd),
      eof;
    {ok, MetaBin} ->
      Meta = gululog_meta:decode(MetaBin),
      {cursor(header, Fd, Meta), Meta}
  end;
read_and_move(header, Fd, Meta) ->
  {ok, Header} = file:read(Fd, gululog_meta:header_size(Meta)),
  {cursor(body, Fd, Meta), Header};
read_and_move(body, Fd, Meta) ->
  {ok, Body} = file:read(Fd, gululog_meta:body_size(Meta)),
  {cursor(meta, Fd, gululog_meta:version(Meta)), Body}.

mk_name(Dir, SegId) -> gululog_name:from_segid(Dir, SegId) ++ ?FILE_SUFFIX.

%% @private Read the first byte version number, position fd to location 1
-spec read_version(file:fd()) -> logvsn().
read_version(Fd) ->
  {ok, <<Version:8>>} = file:pread(Fd, 0, 1),
  Version.


