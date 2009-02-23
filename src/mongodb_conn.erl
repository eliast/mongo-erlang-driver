%%%-------------------------------------------------------------------
%%% File:      mongodb_conn.erl
%%% @author    Elias Torres <elias@torrez.us>
%%%-------------------------------------------------------------------
-module(mongodb_conn).
-author('elias@torrez.us').

-export([
        start/0, start/2, start_link/0, start_link/2, stop/1, stop/2, send_message/3, send_message/4
    ]).

-record(state, {host, port, socket, req_id=1}).

-include("mongodb.hrl").

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Public API
%%====================================================================

%% @doc start a connection handler (assumes default host and port)
start() ->
    start(?MD_DEFAULT_HOST, ?MD_DEFAULT_PORT).
%% @doc start a connection handler
start(Host, Port) when is_list(Host) andalso is_integer(Port) ->
    start(fun spawn/1, Host, Port).

%% @doc start a connection handler (assumes default host and port)
start_link() ->
    start_link(?MD_DEFAULT_HOST, ?MD_DEFAULT_PORT).
%% @doc start a connection handler
start_link(Host, Port) when is_list(Host) andalso is_integer(Port) ->
    start(fun spawn_link/1, Host, Port).

%% @private
start(SpawnMethod, Host, Port) ->
    Self = self(),
    Pid = SpawnMethod(fun() -> init([Host, Port, Self]) end),
    receive
        {init, Pid, ok} -> {ok, Pid};
        {init, Pid, {error, Reason}} -> {error, Reason};
        Unknown -> {error, Unknown}
    after ?TIMEOUT ->
        {error, timeout}
    end.

%% @doc stop the connection handler
stop(ConnHandler) -> 
  stop(ConnHandler, self()).
  
stop(ConnHandler, From) -> 
  call_(ConnHandler, terminate, From).

send_message(ConnHandler, Operation, Message) ->
  send_message(ConnHandler, Operation, Message, self()).
  
send_message(ConnHandler, Operation, Message, From) ->
  call_(ConnHandler, {send, Operation, Message}, From).
  
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

init([Host, Port, Parent]) ->
    Self = self(),
    case gen_tcp:connect(Host, Port, ?TCP_OPTS) of
        {ok, Sock} ->
            State = #state{host=Host, port=Port, socket=Sock},
            Parent ! {init, Self, ok},
            loop(State);
        {error, Reason} ->
            Parent ! {init, Self, {error, Reason}}
    end.

loop(State) ->
    Self = self(),
    #state{socket=Sock} = State,
    NewState = receive
        {handle_call, From, {terminate, []}} ->
            reply_(State, From, {handle_call, Self, gen_tcp:close(Sock)});
        {handle_call, From, {send, Operation, Message}} ->
            Length = byte_size(Message),
            ReqId = State#state.req_id,
            Packaged = <<(Length+16):32/little-signed, ReqId:32/little-signed, 0:32, Operation:32/little-signed, Message/binary>>,
            gen_tcp:send(Sock, Packaged),
            if
              Operation =:= 2004 ->
                reply_(State, From, {handle_call, Self, recv_(Sock, fun recv_reply/2)});
              true ->
                reply_(State, From, {handle_call, Self, no_recv(Sock)})
            end;
        {handle_call, From, _} ->
            reply_(State, From, {handle_call, Self, {error, invalid_call}});
        {handle_cast, _} ->
            State;
        _ ->
            void
    end,
    IncrementRequest = NewState#state.req_id+1,
    loop(NewState#state{req_id=IncrementRequest}).

%% send reply back to method calling process
reply_(State, DestPid, Reply) ->
    NewState = case Reply of
        {_, _, {error, conn_closed}} -> reconnect(State);
        {_, _, {error, conn_error}} -> reconnect(State);
        _ -> State
    end,
    DestPid ! Reply,
    NewState.

reconnect(#state{host=Host, port=Port, req_id=RequestId}) ->
    {ok, SockNew} = gen_tcp:connect(Host, Port, ?TCP_OPTS),
    #state{host=Host, port=Port, socket=SockNew, req_id=RequestId}.

%% receive response from the server via the socket
recv_(Sock, CustomHandler) ->
    receive
      {tcp_closed, Sock} -> {error, conn_closed};
      {tcp_error, Sock, _Reason} -> {error, conn_error};
      Reply -> CustomHandler(Sock, Reply)
    after ?TIMEOUT -> 
      {error, timeout}
    end.

no_recv(Sock) ->
  receive
      {tcp_closed, Sock} -> {error, conn_closed};
      {tcp_error, Sock, _Reason} -> {error, conn_error}
  after 0 ->
      ok
  end.

recv_reply(Sock, Reply) ->
  case Reply of
    {tcp, _, <<Length:32/little-signed, Rest/binary>>} ->
      {Value, <<>>} = recv_until(Sock, Rest, Length-4),
      {ok, <<Length:32/little-signed, Value/binary>>}
  end.

%%====================================================================
%% Utils
%%====================================================================

recv_until(Sock, Bin, ReqLength) when byte_size(Bin) < ReqLength ->
  receive
    {tcp, Sock, Data} ->
      Combined = <<Bin/binary, Data/binary>>,
      recv_until(Sock, Combined, ReqLength);
   	{error, closed} ->
      connection_closed
  after ?TIMEOUT -> 
    timeout
  end;    
recv_until(_Sock, Bin, ReqLength) when byte_size(Bin) =:= ReqLength ->
    {Bin, <<>>};
recv_until(_Sock, Bin, ReqLength) when byte_size(Bin) > ReqLength ->
    <<Required:ReqLength/binary, Rest/binary>> = Bin,
    {Required, Rest}.