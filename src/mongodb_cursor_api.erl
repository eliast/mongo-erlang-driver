%%%-------------------------------------------------------------------
%%% File:      mongodb_cursor_api.erl
%%% @author    Elias Torres <elias@torrez.us>
%%%-------------------------------------------------------------------
-module(mongodb_cursor_api, [CursorPid]).
-author('elias@torrez.us').

-include("mongodb.hrl").

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

next() ->
  next(self()).

next(From) ->
  mongodb_cursor:call_(CursorPid, {next}, From).

to_list() ->
  to_list(self()).

to_list(From) ->
  exhaust_cursor(From, next(From), []).

count() ->
  count(self()).

count(From) ->
  mongodb_cursor:call_(CursorPid, {count}, From).

close() ->
  close(self()).

close(From) ->
  mongodb_cursor:call_(CursorPid, {close}, From).

exhaust_cursor(_From, none, Accum) ->
  lists:reverse(Accum);
exhaust_cursor(From, Next, Accum) ->
  exhaust_cursor(From, next(From), [Next|Accum]).

