%%%-------------------------------------------------------------------
%%% File:      mongodb_cursor.erl
%%% @author    Elias Torres <elias@torrez.us>
%%%-------------------------------------------------------------------
-module(mongodb_cursor).
-author('elias@torrez.us').

-include("mongodb.hrl").

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

%%====================================================================
%% Public Static API
%%====================================================================

start(State) ->
    start(fun spawn/1, State).

start_link(State) ->
    start(fun spawn_link/1, State).

%% @private
start(SpawnMethod, State) ->
    Pid = SpawnMethod(fun() -> init(State) end),
    mongodb_cursor_api:new(Pid).
  
%%====================================================================
%% Private stuff
%%====================================================================

call_(ConnHandler, Message, From) when is_pid(From) -> % caller is just a regular process
    Self = self(),
    ConnHandler ! {handle_call, Self, Message},
    receive
        {handle_call, ConnHandler, R} -> R;
        Unknown -> {error, Unknown}
    after ?TIMEOUT ->
        {error, timeout}
    end;    
call_(ConnHandler, Message, From) -> % caller is a gen_server process
    Self = self(),
    ConnHandler ! {handle_call, Self, Message},
    receive
        {handle_call, ConnHandler, R} -> gen_server:reply(From, R);
        Unknown -> gen_server:reply(From, {error, Unknown})
    after ?TIMEOUT ->
        gen_server:reply(From, {error, timeout})
    end.

cast_(ConnHandler, Message) ->
    ConnHandler ! {handle_cast, Message}.

init(State) ->
    loop(State).
    
loop(State) ->
    Self = self(),
    NewState = receive
        {handle_call, From, {close}} ->
            reply_(State, From, {handle_call, Self, ok});
        {handle_call, From, {next}} ->
            handle_next(State, From);
        {handle_call, From, _} ->
            reply_(State, From, {handle_call, Self, ok});
        {handle_cast, _} ->
            State;
        _ ->
            void
    end,
    loop(NewState).

handle_next(#cursor{documents=[Next|Rest]} = State, From) ->
  reply_(State#cursor{documents=force_list(Rest)}, From, {handle_call, self(), Next});
  
handle_next(#cursor{documents=Next} = State, From) when not is_list(Next) ->
  reply_(State#cursor{documents=[]}, From, {handle_call, self(), Next});

handle_next(#cursor{cid=CursorId, killed=Killed} = State, From) when CursorId =:= 0; Killed =:= true ->
  reply_(State, From, {handle_call, self(), none});

handle_next(State, From) ->
  try
    {ok, RequestId, CursorId, Results} = refresh(State),
    {Next, Rest} = case Results of
      [Next2 | Rest2] ->
        {Next2, force_list(Rest2)};
      Next2 ->
        {Next2, []}
    end,
    reply_(State#cursor{documents=Rest, id=RequestId, cid=CursorId}, From, {handle_call, self(), Next})
  catch
    _:Any ->
      ?debugVal(Any),
      reply_(State, From, {error, Any})
  end.

handle_response(_State, Response) ->
  {{_Length, RequestId, _ResponseTo, _OpCode}, Rest} = extract_header(Response),
  case Rest of
    <<0:32/little-signed, CursorId:64/little-signed, _StartingFrom:32/little-signed, _Returned:32/little-signed, Data/binary>> ->
      {ok, RequestId, CursorId, mongodb_bson:decode(Data)};
    <<Failure:32/little-signed, _Rest2/binary>> ->
      throw({failed, RequestId, Failure})
  end.  

force_list(Item) when is_list(Item) ->
  Item;
force_list(Item) ->
  [Item].
  
refresh(#cursor{pool=PoolId, collection=Collection, spec=Spec, skip=Skip, limit=Limit, fields=Fields, id=Id, cid=CursorId} = State) ->
  FullName = mongodb_bson:encode_cstring(Collection),
  if
    Id =:= undefined ->
      EncodedSpec = mongodb_bson:encode(Spec),
      Data = <<0:32, FullName/binary, Skip:32/little-signed, Limit:32/little-signed, EncodedSpec/binary>>,
      DataWithFields = if
        Fields =:= undefined ->
          Data;
        true ->
          EncodedFields = mongodb_bson:encode(Fields),
          <<Data/binary, EncodedFields/binary>>
      end,
      case mongodb:send_message(PoolId, 2004, DataWithFields) of
        {ok, Reply} ->
          handle_response(State, Reply);
        Error ->
          ?debugHere,
          Error
      end;
    true ->
      ?debugVal(CursorId),
      todo
  end.

extract_header(<<Length:32/little-signed, RequestId:32/little-signed, 
  ResponseTo:32/little-signed, OpCode:32/little-signed, Rest/binary>> = _Response) ->
  {{Length, RequestId, ResponseTo, OpCode}, Rest}. 

%% send reply back to method calling process
reply_(State, DestPid, Reply) ->
    DestPid ! Reply,
    State.