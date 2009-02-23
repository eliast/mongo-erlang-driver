%%%----------------------------------------------------------------------
%%% File    : mongodb_util.erl
%%% Author  : Elias Torres <elias@torrez.us>
%%% Purpose : Erlang MongoDB Driver
%%%----------------------------------------------------------------------

-module(mongodb_util).
-author('elias@torrez.us').

-include("mongodb.hrl").

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-import(random).
 
object_id() ->
    object_id(random:uniform(math:pow(2, 48)) - 1, random:uniform(math:pow(2, 12)) - 1, random:uniform(math:pow(2, 32)) - 1, random:uniform(math:pow(2, 30)) - 1).
    
object_id(R1, R2, R3, _R4) ->
    {oid, <<R1:48, 4:4, R2:12, R3:32>>}.