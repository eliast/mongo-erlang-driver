#!/bin/sh
cd `dirname $0`
# For the following to work make sure that reloader is in your local code
# path (use code:load_abs/1 in ~/.erlang). We don't bundle it with the sources.
exec erl -pa $PWD/ebin $PWD/deps/*/ebin -boot start_sasl +K true -config src/mongodb -sname mongo -run reloader start $@
