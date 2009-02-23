%%%----------------------------------------------------------------------
%%% File    : mongodb.erl
%%% Author  : Elias Torres <elias@torrez.us>
%%% Purpose : Erlang MongoDB Driver
%%%----------------------------------------------------------------------

-module(mongodb_database, [PoolId, DbName]).
-author('elias@torrez.us').

-include("mongodb.hrl").

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

collection(Name) ->
  mongodb_collection:new(PoolId, DbName, Name).
