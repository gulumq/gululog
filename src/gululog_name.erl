%% @doc Log file name

-module(gululog_name).

-export([ mk_idx_name/2
        , mk_seg_name/2
        , filename_to_segid/1
        , filename_to_type/1
        , wildcard_idx_name_reversed/1
        , wildcard_seg_name_reversed/1
        ]).

%%%*_ MACROS and SPECS =========================================================

-include("gululog_priv.hrl").

-define(SEGID_LEN, 20). %% number of digits (1 bsl 64 - 1) in segment ID

%%%*_ API FUNCTIONS ============================================================

%% @doc Make index full path-name from segment ID.
-spec mk_idx_name(dirname(), segid()) -> filename().
mk_idx_name(DirName, SegId) when is_integer(SegId) ->
  filename:join(DirName, basename(SegId)) ++ ?DOT_IDX.

%% @doc Make segment full path-name from segment ID.
-spec mk_seg_name(dirname(), segid()) -> filename().
mk_seg_name(DirName, SegId) when is_integer(SegId) ->
  filename:join(DirName, basename(SegId)) ++ ?DOT_SEG.

%% @doc Convert filename back to segment ID.
-spec filename_to_segid(filename()) -> segid().
filename_to_segid(FileName) when is_list(FileName) ->
  [Basename | _Suffix] = string:tokens(filename:basename(FileName), "."),
  list_to_integer(Basename).

%% @doc Get file type (index or segment) from file name.
-spec filename_to_type(filename()) -> ?FILE_TYPE_IDX | ?FILE_TYPE_SEG.
filename_to_type(FileName) when is_list(FileName) ->
  case lists:reverse(string:tokens(FileName, ".")) of
    ["idx" | _] -> ?FILE_TYPE_IDX;
    ["seg" | _] -> ?FILE_TYPE_SEG
  end.

%% @doc Get all index file names in the given directory in reversed order.
-spec wildcard_idx_name_reversed(dirname()) -> [filename()].
wildcard_idx_name_reversed(Dir) ->
  wildcard_full_path_name_reversed(Dir, ?DOT_IDX).

%% @doc Get all segment file names in the given directory in reversed order.
-spec wildcard_seg_name_reversed(dirname()) -> [filename()].
wildcard_seg_name_reversed(Dir) ->
  wildcard_full_path_name_reversed(Dir, ?DOT_SEG).

%%%*_ PRIVATE FUNCTIONS ========================================================

%% @private Wildcard match in the given directory, retrun the full
%% path name of the files with given .suffix in reversed order
%% @end
-spec wildcard_full_path_name_reversed(dirname(), string()) -> [filename()].
wildcard_full_path_name_reversed(Dir, DotSuffix) ->
  lists:map(fun(FileName) -> filename:join(Dir, FileName) end,
            lists:reverse(lists:sort(filelib:wildcard("*" ++ DotSuffix, Dir)))).


-spec basename(segid()) -> filename().
basename(SegId) ->
  Name = integer_to_list(SegId),
  pad0(Name, ?SEGID_LEN - length(Name)).

-spec pad0(filename(), non_neg_integer()) -> filename().
pad0(Name, 0) -> Name;
pad0(Name, N) when N > 0 -> pad0([$0 | Name], N-1).

%%%*_ TESTS ====================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

basename_test() ->
  ?assertEqual("00000000000000000000", basename(0)),
  ?assertEqual("01234567890123456789", basename(1234567890123456789)),
  ?assertEqual("18446744073709551615", basename(1 bsl 64 - 1)),
  ok.

to_segid_test() ->
  ?assertEqual(0, filename_to_segid("00000000000000000000.idx")),
  ?assertEqual(1234567890123456789, filename_to_segid("01234567890123456789")),
  ?assertEqual(18446744073709551615, filename_to_segid("/foo/18446744073709551615.seg")),
  ok.

from_segid_test() ->
  ?assertEqual("./topic/00000000000000000000.idx", mk_idx_name("./topic", 0)),
  ?assertEqual("topic/01234567890123456789.seg", mk_seg_name("topic", 1234567890123456789)),
  ?assertEqual("/topic/18446744073709551615.seg", mk_seg_name("/topic/", 1 bsl 64 - 1)),
  ok.

-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
