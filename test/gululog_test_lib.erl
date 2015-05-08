%% @doc Common help functions for test suites.

-module(gululog_test_lib).

-export([ cleanup/1
        ]).

%%%*_ MACROS and SPECS =========================================================

-include("../src/gululog_priv.hrl").

%%%*_ API FUNCTIONS ============================================================

%% @doc Recursively clean up the given directory, sub-directories are deleted.
cleanup(Dir) ->
  Files = gululog_name:wildcard_idx_name_reversed(Dir) ++
          gululog_name:wildcard_seg_name_reversed(Dir),
  BackupDirs = filelib:wildcard("backup*", Dir),
  ok = lists:foreach(fun(File) ->
                        ok = file:delete(File)
                     end, Files),
  ok = lists:foreach(fun(BackupDir0) ->
                        BackupDir = filename:join(Dir, BackupDir0),
                        ok = cleanup(BackupDir),
                        ok = file:del_dir(BackupDir)
                     end, BackupDirs).


%%%*_ PRIVATE FUNCTIONS ========================================================

%%%*_ TESTS ====================================================================

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
