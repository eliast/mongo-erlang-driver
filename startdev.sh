#!/bin/sh
cd `dirname $0`
# For the following to work make sure that reloader is in the code
# path (e.g., use code:load_abs/1 or code:add_pathsz/1 in ~/.erlang).
# You can obtain it from mochiweb's sources.
exec erl -pa $PWD/ebin $PWD/deps/*/ebin -boot start_sasl +K true -config src/mongodb -sname mongo -run reloader start $@
