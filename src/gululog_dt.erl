%% @doc Date-time wrapper

-module(gululog_dt).

-export([ os_micro/0
        , os_sec/0
        , micro_to_utc_str/1
        , sec_to_utc_str/1
        , sec_to_utc_str_compact/1
        , utc_str_to_micro/1
        , utc_str_to_sec/1
        ]).

%%%*_ MACROS and SPECS =========================================================

-include("gululog_priv.hrl").

-define(MEGA, 1000000).
-define(EPOCH, 62167219200).

%%%*_ API FUNCTIONS ============================================================

-spec os_micro() -> os_micro().
os_micro() ->
  {Mega, Sec, Micro} = timestamp(),
  ((Mega * ?MEGA) + Sec) * ?MEGA + Micro.

-spec os_sec() -> os_sec().
os_sec() ->
  {Mega, Sec, _Micro} = timestamp(),
  ((Mega * ?MEGA) + Sec).

-spec micro_to_utc_str(os_sec()) -> string().
micro_to_utc_str(Micro) ->
  Sec = Micro div ?MEGA,
  Mic = Micro rem ?MEGA,
  sec_to_utc_str(Sec) ++ lists:flatten(io_lib:format(".~6.6.0w", [Mic])).

-spec sec_to_utc_str(os_sec()) -> string().
sec_to_utc_str(Sec) ->
  {{Year, Month, Day}, {Hour, Minute, Second}} =
    calendar:now_to_universal_time(sec_to_now(Sec)),
  lists:flatten(
    io_lib:format("~.4w-~2.2.0w-~2.2.0w ~2.2.0w:~2.2.0w:~2.2.0w",
                  [Year, Month, Day, Hour, Minute, Second])).

-spec sec_to_utc_str_compact(os_sec()) -> string().
sec_to_utc_str_compact(Sec) ->
  {{Year, Month, Day}, {Hour, Minute, Second}} =
    calendar:now_to_universal_time(sec_to_now(Sec)),
  lists:flatten(
    io_lib:format("~.4w~2.2.0w~2.2.0w~2.2.0w~2.2.0w~2.2.0w",
                  [Year, Month, Day, Hour, Minute, Second])).

-spec utc_str_to_sec(string()) -> os_sec().
utc_str_to_sec(Str) ->
  utc_str_to_micro(Str) div ?MEGA.

-spec utc_str_to_micro(string()) -> os_micro().
utc_str_to_micro(Str) ->
  [Year, Month, Day, Hour, Minute, Second | MaybeMicro] =
    [list_to_integer(Num) || Num <- string:tokens(Str, "-:. ")],
  Gsec = calendar:datetime_to_gregorian_seconds({{Year, Month, Day},
                                                 {Hour, Minute, Second}}),
  MicroPart = case MaybeMicro of
                []      -> 0;
                [Micro] -> Micro
              end,
  (Gsec - ?EPOCH) * ?MEGA + MicroPart.

%%%*_ PRIVATE FUNCTIONS ========================================================

timestamp() -> os:timestamp().

sec_to_now(Sec) ->
  micro_to_now(Sec * ?MEGA).

micro_to_now(MicroSec) ->
  {MicroSec div ?MEGA div ?MEGA,
   MicroSec div ?MEGA rem ?MEGA,
   MicroSec rem ?MEGA}.

%%%*_ TESTS ====================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

gululog_dt_test_() ->
  [ {"micro_to_utc_str/1 & utc_str_to_micro/1",
      fun() ->
         Micro         = os_micro(),
         MicroStr      = micro_to_utc_str(Micro),
         MicroStrMicro = utc_str_to_micro(MicroStr),
         ?assertEqual(Micro, MicroStrMicro)
       end}
  , {"sec_to_utc_str/1 & utc_str_to_sec/1",
       fun() ->
         Sec       = os_sec(),
         SecStr    = sec_to_utc_str(Sec),
         SecStrSec = utc_str_to_sec(SecStr),
         ?assertEqual(Sec, SecStrSec)
       end}
  , {"sec_to_utc_str_compact/1",
       fun() ->
         Sec = utc_str_to_sec("2015-01-01 01:00:59"),
         ?assertEqual("20150101010059", sec_to_utc_str_compact(Sec))
       end}
  ].

-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
