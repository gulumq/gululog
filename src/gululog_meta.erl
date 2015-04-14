%% @doc Log meta bytes.

-module(gululog_meta).

-export([ encode/5
        , decode/2
        , bytecnt/1
        , calculate_log_size/1
        , logid/1
        , timestamp/1
        , header_size/1
        , body_size/1
        ]).

-export_type([ meta/0
             ]).

-include("gululog.hrl").

-record(meta, { version     :: logvsn()
              , logid       :: logid()
              , timestamp   :: micro()
              , header_size :: bytecnt()
              , body_size   :: bytecnt()
              }).

-opaque meta() :: #meta{}.

-spec version(meta()) -> logvsn().
version(#meta{version = Version}) -> Version.

-spec logid(meta()) -> logid().
logid(#meta{logid = LogId}) -> LogId.

-spec timestamp(meta()) -> micro().
timestamp(#meta{timestamp = Ts}) -> Ts.

-spec header_size(meta()) -> bytecnt().
header_size(#meta{header_size = Hs}) -> Hs.

-spec body_size(meta()) -> bytecnt().
body_size(#meta{body_size = Bs}) -> Bs.

-spec encode(logvsn(), logid(), micro(), bytecnt(), bytecnt()) -> binary().
encode(1, LogId, Timestamp, HeaderSize, BodySize) ->
   <<LogId:64, Timestamp:64, HeaderSize:32, BodySize:32>>.

-spec decode(logvsn(), binary()) -> meta().
decode(1, <<LogId:64, Timestamp:64, HeaderSize:32, BodySize:32>>) ->
  #meta{ version     = 1
       , logid       = LogId
       , timestamp   = Timestamp
       , header_size = HeaderSize
       , body_size   = BodySize
       }.

-spec bytecnt(logvsn()) -> bytecnt().
bytecnt(1) -> 24.

-spec calculate_log_size(meta()) -> bytecnt().
calculate_log_size(#meta{ version     = Version
                        , header_size = HeaderSize
                        , body_size   = BodySize
                        }) ->
  bytecnt(Version) + HeaderSize + BodySize.

