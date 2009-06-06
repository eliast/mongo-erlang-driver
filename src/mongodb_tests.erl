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

-define(STRING, [350,1103]). %% "Şя"

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
      fun(Collection) -> ?_test(collection_find_one(Collection)) end,
      fun(Collection) -> ?_test(collection_unicode(Collection)) end
     ]
    }.

collection_insert(Collection) ->
  {ok, _} = Collection:insert({obj,[{"a", ?STRING}]}).

collection_remove(Collection) ->
  ok = Collection:remove({obj,[]}).

collection_save(Collection) ->
  {ok, {oid, _}} = Collection:save({obj,[{"a", ?STRING}]}).

collection_find(Collection) ->
  {ok, _} = Collection:save({obj,[{"a", ?STRING}]}),
  {ok, _} = Collection:save({obj,[{"a", ?STRING}]}),
  Cursor = Collection:find({obj, [{"a", ?STRING}]}),
  Cursor:next(),
  Cursor:next(),
  ?assertEqual(none, Cursor:next()).

collection_find_one(Collection) ->
  {ok, _} = Collection:save({obj,[{"a", ?STRING}]}),
  {ok, _} = Collection:save({obj,[{"a", ?STRING}]}),
  Cursor = Collection:find_one({obj, [{"a", ?STRING}]}),
  Results = Cursor:to_list(),
  ?assert(length(Results) =:= 1).

test_unicode(Collection, Key, Spec, String) ->
    {ok, {obj, [{"_id", _Id}|_T]}} = Collection:insert(Spec),
    Cursor = Collection:find_one(Spec),
    Results = Cursor:to_list(),
    {obj, [{"_id", _}, {Key, StringFound}]} = hd(Results),
    ?assertEqual(StringFound, String).

collection_unicode(Collection) ->
    test_unicode(Collection, "e", {obj,[{"e", ?STRING}]}, ?STRING).
