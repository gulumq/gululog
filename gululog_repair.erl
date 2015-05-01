scan_backward(Version, SegId, Fd, Position) ->
  case Position =< min_log_size(Version) of
    true ->
      %% all logs are corrupted, re-init
      init_segment(SegId, Fd);
    false ->
      TrailerSize = gululog_trailer:bytecnt(Version),
      case file:pread(Fd, Position - TrailerSize, TrailerSize) of
        {ok, TrailerBin} ->
          Trailer = gululog_trailer:decode(TrailerBin),
          StartPosition = gululog_trailer:start_position(Trailer),
          MetaSize = gululog_meta:bytecnt(Version),
          case Position - StartPosition < 
          file:pread(Fd, StartPosition, MetaSize


      Version = read_and_validate_version(Fd),
      {LogId, Position} = move_to_eof(Fd, LastIndexedPosition),
      #wcur{ version  = Version
           , segid    = SegId
           , logid    = gululog_meta:logid(Meta)
           , position = Position
           , fd       = Fd
           }
  end.

%% @private Read the first byte version number, position fd to location 1
%% raise an error exception if invalid
%% @end
-spec read_and_validate_version(file:fd()) -> logvsn().
read_and_validate_version(Fd) ->
  {ok, <<Version:8>>} = file:pread(Fd, 0, 1),
  case is_valid_version(Version) of
    true  -> Version;
    false -> erlang:error({bad_version, Version})
  end.

min_log_size(Version) ->
  gululog_meta:bytecnt(Version) + gululog_trailer:bytecnt(Version).

%% @private Read info about where to start for the next log position
%% and position the fd to EOF for append.
%% NB! The EOF may not be a real EOF, it's the last indexed log.
%% @end
-spec move_to_eof(logvsn(), file:fd(), position()) -> {logid(), position()}.
move_to_eof(Version, Fd, Position) ->
  {ok, MetaBin} = file:pread(Fd, Position, gululog_meta:bytecnt(Version)),
  gululog_meta:decode(Version, MetaBin).


