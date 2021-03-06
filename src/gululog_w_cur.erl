%% @doc Log segment writer cursor.
%% log entry binary layout
%% `[ <<Meta/binary>>, <<Header/binary, Body/binary>> ]'
%% See gululog_meta.erl for details about Meta
%% @end

-module(gululog_w_cur).

-export([ open/2
        , append/4
        , flush/1
        , flush_close/1
        , switch/3
        , switch_append/5
        , handle_corrupted_tail/2
        , next_log_position/1
        , truncate/5
        , delete_seg/4
        ]).

-export_type([cursor/0]).

%%%*_ MACROS and SPECS =========================================================

-include("gululog_priv.hrl").

-record(wcur, { version  :: logvsn()
              , segid    :: segid()    %% segment id
              , position :: position() %% position for next new log entry
              , fd       :: file:fd()  %% fd for read/write
              }).

-opaque cursor() :: #wcur{}.

-define(INIT_POSITION, 1). %% position of the fist log entry in .seg file

%%%*_ API FUNCTIONS ============================================================

%% @doc Get the position (byte offset) in segment file for the next log to be appended.
-spec next_log_position(cursor()) -> position().
next_log_position(#wcur{position = Position}) -> Position.

%% @doc Open the last segment file the given directory for writer to append.
-spec open(dirname(), segid()) -> cursor().
open(Dir, OldestSegId) ->
  ok = filelib:ensure_dir(filename:join(Dir, "foo")),
  case wildcard_reverse(Dir) of
    []         -> open_new_seg(Dir, OldestSegId);
    [File | _] -> open_existing_seg(File)
  end.

%% @doc Flush os disk cache.
-spec flush(cursor()) -> ok.
flush(#wcur{fd = Fd}) ->
  ok = file:sync(Fd).

%% @doc Flush os disk cache, close fd.
-spec flush_close(cursor()) -> ok.
flush_close(#wcur{fd = Fd}) ->
  ok = file:sync(Fd),
  ok = file:close(Fd).

%% @doc Append one log entry.
-spec append(cursor(), logid(), header(), body()) -> cursor().
append(#wcur{ version  = Version
            , fd       = Fd
            , segid    = SegId
            , position = Position
            } = Cursor, LogId, Header, Body) ->
  Version = ?LOGVSN, %% assert
  case Position =:= ?INIT_POSITION of
    true  -> [erlang:error({unexpected_fist_logid, SegId, LogId}) || SegId =/= LogId]; %% assert
    false -> [erlang:error({unexpected_logid, SegId, LogId}) || LogId =< SegId] %% assert
  end,
  Meta = gululog_meta:new(Version, LogId, size(Header), size(Body)),
  MetaBin = gululog_meta:encode(Version, Meta, Header, Body),
  ok = file:write(Fd, [MetaBin, Header, Body]),
  NewPosition = Position + gululog_meta:calculate_log_size(Version, Meta),
  Cursor#wcur{position = NewPosition}.

%% @doc Switch to a new segment file.
-spec switch(dirname(), cursor(), logid()) -> cursor().
switch(Dir, OldCursor, NextLogId) ->
  NewSegId = NextLogId,
  ok = flush_close(OldCursor),
  open_new_seg(Dir, NewSegId).

%% @doc Switch to a new segment file and append a new log to it.
-spec switch_append(dirname(), cursor(), logid(), header(), body()) -> cursor().
switch_append(Dir, OldCursor, LogId, Header, Body) ->
  NewCursor = switch(Dir, OldCursor, LogId),
  append(NewCursor, LogId, Header, Body).

%% @doc Handle corrupted tail for write cursor.
-spec handle_corrupted_tail(cursor(), logid()) -> cursor().
handle_corrupted_tail(#wcur{fd = Fd, version = Version} = Cur, LogId) ->
  {ok, _} = file:position(Fd, ?INIT_POSITION),
  ok = seek_and_truncate_corrupted_w_cur(Fd, ?INIT_POSITION, Version, LogId),
  Cur.

%% @doc Truncate after given logid from segment file
%% Return new writer cur and delete segment files
%% @end
-spec truncate(dirname(), cursor(), segid(), position(), ?undef | dirname()) ->
        {cursor(), [file_op()]}.
truncate(Dir, Cur, SegId, Position, BackupDir) ->
  ok = flush_close(Cur),
  {KeepFiles0, TruncateFiles} =
    lists:partition(fun(FileName) ->
                      gululog_name:filename_to_segid(FileName) < SegId
                    end, wildcard_reverse(Dir)),
  {KeepFiles, TruncateFile, DeleteFiles} =
    case Position =:= ?INIT_POSITION of
      true  ->
        %% the .seg file for the given segid should be deleted
        %% rather than truncated
        {KeepFiles0, ?undef, TruncateFiles};
      false ->
        [TruncateFile0 | DeleteFiles0] = lists:reverse(TruncateFiles),
        {[TruncateFile0 | KeepFiles0], TruncateFile0, lists:reverse(DeleteFiles0)}
    end,
  DeleteResult = truncate_delete_do(DeleteFiles, BackupDir),
  TruncateResult = truncate_truncate_do(TruncateFile, Position, BackupDir),
  NewCur =
    case KeepFiles of
      []         -> open_new_seg(Dir, SegId);
      [File | _] -> open_existing_seg(File)
    end,
  {NewCur, DeleteResult ++ TruncateResult}.

%% @doc Delete segment file for the given segid.
%% Return new curosr() and the delete file with OP tag
%% File is backedup if backup dir is given.
%% @end
-spec delete_seg(dirname(), cursor(), segid(), ?undef | dirname()) ->
        {cursor(), file_op()}.
delete_seg(Dir, #wcur{segid = CurrentSegId} = Cur, SegId, BackupDir) ->
  true = (CurrentSegId =/= SegId), %% assert
  {Cur, gululog_file:delete(mk_name(Dir, SegId), BackupDir)}.

%%%*_ PRIVATE FUNCTIONS ========================================================

%% @private Help function to open the wrter cursor.
-spec open_existing_seg(filename()) -> cursor().
open_existing_seg(FileName) ->
  SegId = gululog_name:filename_to_segid(FileName),
  {ok, Fd} = file:open(FileName, [write, read, raw, binary, delayed_write]),
  {ok, <<Version:8>>} = file:read(Fd, ?INIT_POSITION),
  true = (Version =< ?LOGVSN), %% assert
  %% In case Version < ?LOGVSN, the caller should
  %% switch to the next segment immediately by calling switch/3
  %% or switch_append/5. We don't try to switch here
  %% because otherwise we'll have to read the latest LogID from
  %% the tail of THIS file --- while this information is easily
  %% accessible from index which should be owned by the caller
  %% of this function
  {ok, Position}  = file:position(Fd, eof),
  #wcur{ version  = Version
       , segid    = SegId
       , position = Position
       , fd       = Fd
       }.

%% @private Open a new segment file for writer.
open_new_seg(Dir, SegId) ->
  FileName = mk_name(Dir, SegId),
  {ok, Fd} = file:open(FileName, [write, read, raw, binary, delayed_write]),
  ok = file:write(Fd, <<?LOGVSN:8>>),
  #wcur{ version  = ?LOGVSN
       , segid    = SegId
       , position = ?INIT_POSITION
       , fd       = Fd
       }.

%% @private Seek write cursor in order to truncate corrupted tail.
seek_and_truncate_corrupted_w_cur(Fd, StartPos, Version, LogId) ->
  MetaSize      = gululog_meta:bytecnt(Version),
  {ok, MetaBin} = file:pread(Fd, StartPos, MetaSize),
  Meta          = gululog_meta:decode(Version, MetaBin),
  MetaLogId     = gululog_meta:logid(Meta),
  HeaderSize    = gululog_meta:header_size(Meta),
  BodySize      = gululog_meta:body_size(Meta),
  NextStartPos  = StartPos + MetaSize + HeaderSize + BodySize,
  if
    MetaLogId == LogId ->
      file:position(Fd, NextStartPos),
      file:truncate(Fd),
      ok;
    true ->
      seek_and_truncate_corrupted_w_cur(Fd, NextStartPos, Version, LogId)
  end.

%% @private Make a segment file name.
mk_name(Dir, SegId) -> gululog_name:mk_seg_name(Dir, SegId).

%% @private Find all the index files in the given directory
%% return all filenames in reversed order.
%% @end
-spec wildcard_reverse(dirname()) -> [filename()].
wildcard_reverse(Dir) -> gululog_name:wildcard_seg_name_reversed(Dir).

%% @private Truncate segment file.
-spec truncate_truncate_do(filename(), position(), dirname()) -> [file_op()].
truncate_truncate_do(?undef, _Position, _BackupDir) -> [];
truncate_truncate_do(SegFile, Position, BackupDir) ->
  gululog_file:maybe_truncate(SegFile, Position, BackupDir).

%% @private Delete segment files.
%% It is very important to delete files in reversed order in order to keep
%% data integrity (in case this function crashes in the middle for example)
%% @end
-spec truncate_delete_do([filename()], ?undef | dirname()) -> [file_op()].
truncate_delete_do(DeleteListReversed, BackupDir) ->
  lists:map(fun(FileName) ->
              gululog_file:delete(FileName, BackupDir)
            end, DeleteListReversed).

%%%*_ TESTS ====================================================================

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
