%% @doc Log meta bytes.
%% Binary layout:
%% `<<MetaCRC:32, HeaderCRC:32, BodyCRC:32, Meta/binary>>'
%% Where Meta = `<<LogId:64, HeaderSize:32, BodySize:32>>'
%% @end

-module(gululog_meta).

-export([ encode/4
        , decode/2
        , bytecnt/1
        , calculate_log_size/2
        , logid/1
        , header_size/1
        , body_size/1
        , new/4
        , assert_data_integrity/4
        ]).

-export_type([ meta/0
             ]).

%%%*_ MACROS and SPECS =========================================================

-include("gululog_priv.hrl").

-record(meta, { logid       :: logid()
              , header_size :: bytecnt()
              , body_size   :: bytecnt()
              , header_crc  :: pos_integer()
              , body_crc    :: pos_integer()
              }).

-opaque meta() :: #meta{}.

-spec logid(meta()) -> logid().
logid(#meta{logid = LogId}) -> LogId.

-spec header_size(meta()) -> bytecnt().
header_size(#meta{header_size = Hs}) -> Hs.

-spec body_size(meta()) -> bytecnt().
body_size(#meta{body_size = Bs}) -> Bs.

%%%*_ API FUNCTIONS ============================================================

%% @doc Create a new log meta info set.
-spec new(logvsn(), logid(), bytecnt(), bytecnt()) -> meta().
new(1, LogId, HeaderSize, BodySize) ->
  #meta{ logid       = LogId
       , header_size = HeaderSize
       , body_size   = BodySize
       }.

%% @doc Encode meta info into binary.
-spec encode(logvsn(), meta(), header(), body()) -> binary().
encode(1, #meta{ logid       = LogId
               , header_size = HeaderSize
               , body_size   = BodySize
               }, Header, Body) ->
   MetaBin = <<LogId:64, HeaderSize:32, BodySize:32>>,
   MetaCRC = erlang:crc32(MetaBin),
   HeaderCRC = erlang:crc32(Header),
   BodyCRC = erlang:crc32(Body),
   <<MetaCRC:32, HeaderCRC:32, BodyCRC:32, MetaBin/binary>>.

%% @doc Decode meta bytes into opaque meta()
%% raise a 'throw' exception in case unexpected size or corrupted.
%% @end
-spec decode(logvsn(), binary()) -> meta() | no_return().
decode(Version = 1, <<MetaCRC:32, HeaderCRC:32, BodyCRC:32, MetaBin/binary>>) ->
  ExpectedMetaSize = bytecnt(Version) - (_CRCBytes = 12),
  [erlang:throw(bad_meta_size)  || erlang:size(MetaBin) =/= ExpectedMetaSize],
  [erlang:throw(corrupted_meta) || erlang:crc32(MetaBin) =/= MetaCRC],
  <<LogId:64, HeaderSize:32, BodySize:32>> = MetaBin,
  #meta{ logid       = LogId
       , header_size = HeaderSize
       , body_size   = BodySize
       , header_crc  = HeaderCRC
       , body_crc    = BodyCRC
       }.

%% @doc Assert data integrity. 'throw' exception in case not.
-spec assert_data_integrity(logvsn(), meta(), header(), body()) -> ok | no_return().
assert_data_integrity(1, #meta{ header_crc = HeaderCRC
                              , body_crc   = BodyCRC
                              }, Header, Body) ->
  [erlang:throw(corrupted_header) || HeaderCRC =/= erlang:crc32(Header)],
  [erlang:throw(corrupted_body) || Body =/= ?undef andalso BodyCRC=/= erlang:crc32(Body)],
  ok.

%% @doc Per-version meta size in file.
%% For reader, must support all versions.
%% @end
-spec bytecnt(logvsn()) -> bytecnt().
bytecnt(1) -> 28.

%% @doc Calculate the entire log entry size.
-spec calculate_log_size(logvsn(), meta()) -> bytecnt().
calculate_log_size(Version, #meta{ header_size = HeaderSize
                                 , body_size   = BodySize
                                 }) ->
  bytecnt(Version) + HeaderSize + BodySize.

%%%*_ PRIVATE FUNCTIONS ========================================================

%%%*_ TESTS ====================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

v1_size_test() ->
  Meta = new(1, 1, 0, 1),
  Bin = encode(1, Meta, <<>>, <<>>),
  ?assertEqual(Meta#meta{header_crc = 0, body_crc = 0}, decode(1, Bin)),
  Size = bytecnt(1),
  ?assertEqual(Size, size(Bin)).

-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
