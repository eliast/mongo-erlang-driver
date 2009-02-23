%%%----------------------------------------------------------------------
%%% File    : mongodb_tests.erl
%%% Author  : Elias Torres <elias@torrez.us>
%%% Purpose : Erlang MongoDB Driver
%%%----------------------------------------------------------------------

-module(mongodb_tests).
-author('elias@torrez.us').

-include("mongodb.hrl").

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

setup_collection() ->
  error_logger:tty(false),
  {ok, _Pid} = mongodb:start(mypool),
  Db = mongodb:database(mypool, "pymongo_test"),
  TestCollection = Db:collection("test"),
  ok = TestCollection:remove({obj,[]}),
  TestCollection.
  
teardown_collection(_Collection) ->
  mongodb:stop().

collection_test_() ->
    {foreach,
     fun setup_collection/0,
     fun teardown_collection/1,
     [
      fun(Collection) -> ?_test(collection_insert(Collection)) end,
      fun(Collection) -> ?_test(collection_remove(Collection)) end,
      fun(Collection) -> ?_test(collection_save(Collection)) end,
      fun(Collection) -> ?_test(collection_find(Collection)) end,
      fun(Collection) -> ?_test(collection_find_one(Collection)) end
     ]
    }.

collection_insert(Collection) ->
  {ok, _} = Collection:insert({obj,[{"a", 1}]}).
  
collection_remove(Collection) ->
  ok = Collection:remove({obj,[]}).
  
collection_save(Collection) ->
  {ok, {oid, _}} = Collection:save({obj,[{"a", 1}]}).

collection_find(Collection) ->
  {ok, _} = Collection:save({obj,[{"a", 1}]}),
  {ok, _} = Collection:save({obj,[{"a", 1}]}),
  Cursor = Collection:find({obj, [{"a", 1}]}),
  Cursor:next(),
  Cursor:next(),
  ?assertEqual(none, Cursor:next()).
  
collection_find_one(Collection) ->
  {ok, _} = Collection:save({obj,[{"a", 1}]}),
  {ok, _} = Collection:save({obj,[{"a", 1}]}),
  Cursor = Collection:find_one({obj, [{"a", 1}]}),
  Results = Cursor:to_list(),
  ?assert(length(Results) =:= 1).