%% @doc Date-time wrapper

-module(gululog_dt).

-export([ os_micro/0
        , to_utc_str/1
        , from_utc_str/1
        ]).

-include("gululog.hrl").

-define(MEGA, 1000000).

-spec os_micro() -> micro().
os_micro() ->
  {Mega, Sec, Micro} = timestamp(),
  ((Mega * ?MEGA) + Sec) * ?MEGA + Micro.

-spec to_utc_str(micro()) -> string().
to_utc_str(MicroSec) ->
  {{Year, Month, Day}, {Hour, Minute, Second}} =
    calendar:now_to_universal_time(to_now(MicroSec)),
  lists:flatten(
    io_lib:format("~.4w-~2.2.0w-~2.2.0w ~2.2.0w:~2.2.0w:~2.2.0w.~w",
                  [Year, Month, Day, Hour, Minute, Second, MicroSec rem ?MEGA])).

-spec from_utc_str(string()) -> micro().
from_utc_str(Str) ->
  [Year, Month, Day, Hour, Minute, Second | MaybeMicro] =
    [list_to_integer(Num) || Num <- string:tokens(Str, "-:. ")],
  Gsec = calendar:datetime_to_gregorian_seconds({{Year, Month, Day},
                                                 {Hour, Minute, Second}}),
  MicroPart = case MaybeMicro of
                []      -> 0;
                [Micro] -> Micro
              end,
  (Gsec - 62167219200) * ?MEGA + MicroPart.

%% INTERNAL FUNCTIONS

timestamp() -> os:timestamp().

to_now(MicroSec) ->
  {MicroSec div ?MEGA div ?MEGA,
   MicroSec div ?MEGA rem ?MEGA,
   MicroSec rem ?MEGA}.


