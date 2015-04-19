%% @doc Log meta bytes.
%% binary layout
%% <<MetaCRC:32, DataCRC:32, Meta/binary>>
%% Where Meta = <<LogId:64, Timestamp:32, HeaderSize:32, BodySize:32>>

-module(gululog_meta).

-export([ encode/3
        , decode/2
        , bytecnt/1
        , calculate_log_size/2
        , logid/1
        , logged_on/1
        , header_size/1
        , body_size/1
        , now_ts/0
        , new/4
        , assert_data_integrity/3
        ]).

-export_type([ meta/0
             ]).

-include("gululog_priv.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(meta, { logid        :: logid()
              , logged_on    :: ts()
              , header_size  :: bytecnt()
              , body_size    :: bytecnt()
              , data_crc = 0 :: pos_integer()
              }).

-opaque meta() :: #meta{}.
-type ts() :: os_sec().

-spec now_ts() -> ts().
now_ts() -> gululog_dt:os_sec().

-spec logid(meta()) -> logid().
logid(#meta{logid = LogId}) -> LogId.

-spec logged_on(meta()) -> ts().
logged_on(#meta{logged_on = Ts}) -> Ts.

-spec header_size(meta()) -> bytecnt().
header_size(#meta{header_size = Hs}) -> Hs.

-spec body_size(meta()) -> bytecnt().
body_size(#meta{body_size = Bs}) -> Bs.

%% @doc Create a new log meta info set.
%% Take current os time as timestamp.
%% @end
-spec new(logvsn(), logid(), bytecnt(), bytecnt()) -> meta().
new(1, LogId, HeaderSize, BodySize) ->
  #meta{ logid       = LogId
       , logged_on   = gululog_dt:os_sec()
       , header_size = HeaderSize
       , body_size   = BodySize
       }.

%% @doc Encode meta info into binary.
-spec encode(logvsn(), meta(), iodata()) -> binary().
encode(1, #meta{ logid       = LogId
               , logged_on   = Timestamp
               , header_size = HeaderSize
               , body_size   = BodySize
               }, LogData) ->
   MetaBin = <<LogId:64, Timestamp:32, HeaderSize:32, BodySize:32>>,
   MetaCRC = erlang:crc32(MetaBin),
   DataCRC = erlang:crc32(LogData),
   <<MetaCRC:32, DataCRC:32, MetaBin/binary>>.

%% @doc Decode meta bytes into opaque meta()
%% raise a 'throw' exception in case unexpected size or corrupted.
%% @end
-spec decode(logvsn(), binary()) -> meta() | no_return().
decode(Version = 1, <<MetaCRC:32, DataCRC:32, MetaBin/binary>>) ->
  [erlang:throw(bad_size)  || erlang:size(MetaBin) =/= (bytecnt(Version) - 8)],
  [erlang:throw(corrupted_meta) || erlang:crc32(MetaBin) =/= MetaCRC],
  <<LogId:64, Timestamp:32, HeaderSize:32, BodySize:32>> = MetaBin,
  #meta{ logid       = LogId
       , logged_on   = Timestamp
       , header_size = HeaderSize
       , body_size   = BodySize
       , data_crc    = DataCRC
       }.

%% @doc Assert data integrity. 'throw' exception in case not.
-spec assert_data_integrity(logvsn(), meta(), iodata()) -> ok | no_return().
assert_data_integrity(1, #meta{data_crc = CRC}, Data) ->
  [erlang:throw(corrupted_data) || CRC =/= erlang:crc32(Data)],
  ok.

%% @doc Per-version meta size in file.
-spec bytecnt(logvsn()) -> bytecnt().
bytecnt(1) -> 28.

%% @doc Calculate the entire log entry size.
-spec calculate_log_size(logvsn(), meta()) -> bytecnt().
calculate_log_size(Version, #meta{ header_size = HeaderSize
                                 , body_size   = BodySize
                                 }) ->
  bytecnt(Version) + HeaderSize + BodySize.

%%% TESTs

v1_size_test() ->
  Meta = new(1, 1, 0, 1),
  Bin = encode(1, Meta, <<>>),
  ?assertEqual({ok, Meta}, decode(1, Bin)),
  Size = bytecnt(1),
  ?assertEqual(Size, size(Bin)).

