%% @doc Log file name

-module(gululog_name).

-export([from_segid/2]).
-export([to_segid/1]).
-export([wildcard_full_path_name_reversed/2]).

-include("gululog_priv.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(SEGID_LEN, 20). %% number of digits (1 bsl 64 - 1) in segment ID

%% @doc Make log (segment/index) file name from segment ID.
-spec from_segid(dirname(), segid()) -> filename().
from_segid(DirName, SegId) when is_integer(SegId) ->
  filename:join(DirName, basename(SegId)).

%% @doc Convert filename back to segment ID.
-spec to_segid(filename()) -> segid().
to_segid(FileName) when is_list(FileName) ->
  [Basename | _] = string:tokens(filename:basename(FileName), "."),
  list_to_integer(Basename).

%% @doc Wildcard match in the given directory, retrun the full
%% path name of the files with given .suffix in reversed order
%% @end
-spec wildcard_full_path_name_reversed(dirname(), string()) -> [filename()].
wildcard_full_path_name_reversed(Dir, DotSuffix) ->
  lists:map(fun(FileName) -> filename:join(Dir, FileName) end,
            lists:reverse(lists:sort(filelib:wildcard("*" ++ DotSuffix, Dir)))).

%% INTERNAL FUNCTIONS

-spec basename(segid()) -> filename().
basename(SegId) ->
  Name = integer_to_list(SegId),
  pad0(Name, ?SEGID_LEN - length(Name)).

-spec pad0(filename(), non_neg_integer()) -> filename().
pad0(Name, 0) -> Name;
pad0(Name, N) -> pad0([$0 | Name], N-1).

%% TESTS

basename_test() ->
  ?assertEqual("00000000000000000000", basename(0)),
  ?assertEqual("01234567890123456789", basename(1234567890123456789)),
  ?assertEqual("18446744073709551615", basename(1 bsl 64 - 1)),
  ok.

to_segid_test() ->
  ?assertEqual(0, to_segid("00000000000000000000.idx")),
  ?assertEqual(1234567890123456789, to_segid("01234567890123456789")),
  ?assertEqual(18446744073709551615, to_segid("/foo/18446744073709551615.log")),
  ok.
