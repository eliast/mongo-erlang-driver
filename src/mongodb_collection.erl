%%%----------------------------------------------------------------------
%%% File    : mongodb_collection.erl
%%% Author  : Elias Torres <elias@torrez.us>
%%% Purpose : Erlang MongoDB Driver
%%%----------------------------------------------------------------------

-module(mongodb_collection, [PoolId, DbName, ColName]).
-author('elias@torrez.us').

-include("mongodb.hrl").

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

full_name() ->
  lists:concat([DbName, ".", ColName]).

insert({obj, PropList}) ->
  FullName = mongodb_bson:encode_cstring(full_name()),
  Modified = {obj, [{"_id", mongodb_util:object_id()} | PropList]},
  EncodedDocument = mongodb_bson:encode(Modified),
  case mongodb:send_message(PoolId, 2002, <<0:32, FullName/binary, EncodedDocument/binary>>) of
    ok ->
      {ok, Modified};
    Error ->
      Error
  end.

update(Spec, Document) ->
  FullName = mongodb_bson:encode_cstring(full_name()),
  EncodedSpec = mongodb_bson:encode(Spec),
  EncodedDocument = mongodb_bson:encode(Document),
  mongodb:send_message(PoolId, 2001, <<0:32, FullName/binary, EncodedSpec/binary, EncodedDocument/binary>>).

save({obj, PropList} = Document) ->
  case proplists:is_defined("_id", PropList) of
    false ->
      case insert(Document) of
        {ok, {obj, InsertedProperties}} ->
          {ok, proplists:get_value("_id", InsertedProperties)};
        Error ->
          Error
      end;
    true ->
      Id = proplists:get_value("_id", PropList),
      case update({obj, [{"_id", Id}]}, Document) of
        ok ->
          {ok, Id};
        Error ->
          Error
      end
  end.

remove({obj, PropList}) ->
  FullName = mongodb_bson:encode_cstring(full_name()),
  EncodedSpec = mongodb_bson:encode({obj, PropList}),
  mongodb:send_message(PoolId, 2006, <<0:32, FullName/binary, 0:32, EncodedSpec/binary>>).

find_one({oid, Id}) ->
  find_one({obj, [{"_id", {oid, Id}}]});

find_one(Spec) ->
  mongodb_cursor:start(#cursor{pool=PoolId, collection=full_name(), spec=Spec, limit=1}).

find({oid, Id}) ->
  find({obj, [{"_id", {oid, Id}}]});

find(Spec) ->
  mongodb_cursor:start(#cursor{pool=PoolId, collection=full_name(), spec=Spec}).
