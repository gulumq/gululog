%% @doc Common help functions for test suites.

-module(gululog_test_lib).

-export([ cleanup/1
        ]).

%%%*_ MACROS and SPECS =========================================================

-include("../src/gululog_priv.hrl").

%%%*_ API FUNCTIONS ============================================================

cleanup(Dir) ->
  Files = filelib:wildcard("*" ++ ?DOT_SEG, Dir) ++
          filelib:wildcard("*" ++ ?DOT_IDX, Dir),
  lists:foreach(fun(File) ->
                  ok = file:delete(filename:join(Dir, File))
                end, Files).

%%%*_ PRIVATE FUNCTIONS ========================================================

%%%*_ TESTS ====================================================================

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
