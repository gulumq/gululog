#!/bin/bash

# find all modules
ERL_FILES="$(find ebin/*.beam -type f -print0 | xargs -0 -I{} basename {} .beam)"
MODULES="$(echo $ERL_FILES | tr ' ' ',')"

# Erlang code find modules having test/0 exported
CODE="[io:format([io_lib:print(Module), $ ]) || Module <- [$MODULES], lists:member({test, 0}, Module:module_info(exports))]"

# Get all modules having test/0 exported
UT_MODULES="$(erl -pa ebin -noshell -eval "$CODE" -s init stop)"

cp test/gululog_ut_SUITE.erl.in test/gululog_ut_SUITE.erl

# generate a test case in gululog_ut_SUITE.erl
for i in $UT_MODULES; do
  echo -e "t_$i(_Config) -> ?assertEqual(ok, $i:test()).\n" >> test/gululog_ut_SUITE.erl
done

