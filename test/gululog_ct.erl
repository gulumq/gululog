
-module(gululog_ct).

-export([run/0]).
-export([run/1]).

run() -> run(".").

run(LogDir) ->
  io:format("logging to : ~p\n", [LogDir]),
  ct:run_test([ {dir, filename:join([code:lib_dir(guluhub), "ebin"])}
              , {logdir, LogDir}
              ]).

