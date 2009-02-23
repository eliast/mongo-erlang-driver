%%%----------------------------------------------------------------------
%%% File    : mongodb.erl
%%% Author  : Elias Torres <elias@torrez.us>
%%% Purpose : Erlang MongoDB Driver
%%%----------------------------------------------------------------------

-module(mongodb).
-author('elias@torrez.us').

-behavior(gen_server).

-define(SERVER, ?MODULE).

-include("mongodb.hrl").

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

%% API functions

%% gen_server callbacks
-export([init/1, 
         handle_call/3,
         handle_cast/2, 
         handle_info/2,
         code_change/3,
         terminate/2]).
         
-record(connid, {
   lf, % count of how many outstanding calls are assigned to this connection
   pid % pid of the process handling the connection
}).
-record(connection, {
   pid, % pid of the process handling the connection
   host,
   port
}).
-record(pool, {
   id, % PoolId
   connections % all connections within this pool
}).
-record(state, {
   pools, % mapping PoolId -> pool tuple
   conns % mapping ConnPid -> pool tuple
}).
                
%%====================================================================
%% Public API
%%====================================================================

%% @doc
%% Start the tora gen_server and create a default pool with the given PoolId <br/>
%% and add one connection to this pool (connection assumes default hostname and port).
%% @end
start(PoolId) when is_atom(PoolId) ->
    start(PoolId, ?MD_DEFAULT_HOST, ?MD_DEFAULT_PORT).
%% @doc
%% Start the tora gen_server and create a default pool with the given PoolId <br/>
%% and add one connection to this pool (connection uses the supplied hostname and port).
%% @end
start(PoolId, Host, Port) when is_atom(PoolId) andalso is_list(Host) andalso is_integer(Port) ->
    gen_server:start({local, ?SERVER}, ?MODULE, [PoolId, Host, Port], []).

start_link(PoolId) when is_atom(PoolId) ->
    start_link(PoolId, ?MD_DEFAULT_HOST, ?MD_DEFAULT_PORT).
start_link(PoolId, Host, Port) when is_atom(PoolId) andalso is_list(Host) andalso is_integer(Port) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [PoolId, Host, Port], []).

%% @doc spawn a new connection handler and add it to an existing pool
add_to_pool(PoolId) when is_atom(PoolId) ->
    gen_server:call(?SERVER, {PoolId, add_to_pool}).

%% @doc spawn a new connection handler with the given (host, port) and add it to the pool
add_to_pool(PoolId, Host, Port) when is_atom(PoolId) andalso is_list(Host) andalso is_integer(Port) ->
    gen_server:call(?SERVER, {PoolId, add_to_pool, {Host, Port}}).

%% @doc return the number of connections currently present in an existing pool
pool_size(PoolId) when is_atom(PoolId) ->
    gen_server:call(?SERVER, {PoolId, pool_size}).

%% @doc return the list of Pids of the connection handlers in an existing pool
pool_connections(PoolId) when is_atom(PoolId) ->
    gen_server:call(?SERVER, {PoolId, pool_connections}).

%% @doc create a new pool (also automatically creates 1 connection to default host and port)
pool_create(PoolId) ->
    pool_create(PoolId, ?MD_DEFAULT_HOST, ?MD_DEFAULT_PORT).
%% @doc create a new pool (also automatically creates 1 connection to supplied host and port)
pool_create(PoolId, Host, Port) when is_atom(PoolId) ->
    gen_server:call(?SERVER, {PoolId, pool_create, {Host, Port}}).

%% @doc stop all connections in an existing pool
stop(PoolId) when is_atom(PoolId) ->
    gen_server:call(?SERVER, {PoolId, terminate}).

stop() ->
  try
    gen_server:call(?SERVER, stop)
  catch
    exit:{normal,_} ->
      ok;
    _:_Any ->
      {error, _Any}
  end.
        
%%%----------------------------------------------------------------------
%%% API
%%%----------------------------------------------------------------------

database(PoolId, Name) ->
  mongodb_database:new(PoolId, Name).
  
send_message(PoolId, Operation, Message) ->
  gen_server:call(?MODULE, {PoolId, {send_message, Operation, Message}}).

%%%----------------------------------------------------------------------
%%% Callback functions from gen_server
%%%----------------------------------------------------------------------
%%====================================================================
%% gen_server callbacks
%%====================================================================
init([PoolId, Host, Port]) ->
    process_flag(trap_exit, true),
    Pools = gb_trees:empty(),
    Conns = gb_trees:empty(),
    {ok, create_pool(PoolId, Host, Port, #state{pools=Pools, conns=Conns})}.

handle_call({PoolId, terminate}, _From, #state{pools=Pools, conns=Conns}) ->
    ?debugVal({PoolId, terminate}),
    ?debugVal([Pools, Conns]),
    {Pools1, Conns1} = stop_pool(PoolId, Pools, Conns),
    ?debugVal(#state{pools=Pools1, conns=Conns1}),
    {reply, ok, #state{pools=Pools1, conns=Conns1}};

handle_call({PoolId, add_to_pool}, _From, #state{pools=Pools, conns=Conns}) ->
    {value, Pool} = gb_trees:lookup(PoolId, Pools),
    #pool{id=PoolId, connections=Connections} = Pool,
    % use the same (host, port) from the first connection created
    {_, #connection{host=Host, port=Port}} = gb_trees:smallest(Connections),
    {ConnPid, Pool1} = add_conn(Pool, Host, Port),
    Pools1 = gb_trees:enter(PoolId, Pool1, Pools),
    Conns1 = gb_trees:enter(ConnPid, Pool1, Conns),
    {reply, ok, #state{pools=Pools1, conns=Conns1}};
handle_call({PoolId, add_to_pool, {Host, Port}}, _From, #state{pools=Pools, conns=Conns}) ->
    {value, Pool} = gb_trees:lookup(PoolId, Pools),
    {ConnPid, Pool1} = add_conn(Pool, Host, Port),
    Pools1 = gb_trees:enter(PoolId, Pool1, Pools),
    Conns1 = gb_trees:enter(ConnPid, Pool1, Conns),
    {reply, ok, #state{pools=Pools1, conns=Conns1}};    

handle_call({PoolId, pool_size}, _From, #state{pools=Pools, conns=Conns}) ->
    {value, #pool{connections=Connections}} = gb_trees:lookup(PoolId, Pools),
    PoolSize = gb_trees:size(Connections),
    {reply, PoolSize, #state{pools=Pools, conns=Conns}};

handle_call({PoolId, pool_connections}, _From, #state{pools=Pools, conns=Conns}) ->
    {value, #pool{connections=Connections}} = gb_trees:lookup(PoolId, Pools),
    ConnPids = [ConnPid || #connid{pid=ConnPid} <- gb_trees:keys(Connections)],
    {reply, ConnPids, #state{pools=Pools, conns=Conns}};

handle_call({PoolId, pool_create, {Host, Port}}, _From, State) ->
    {reply, ok, create_pool(PoolId, Host, Port, State)};
    
%% match all remaining API calls
handle_call({PoolId, {send_message, Operation, Message}}, From, #state{pools=Pools, conns=Conns})  ->
    Pools1 = with_connection(
        PoolId, Pools,
        fun(ConnHandler) ->
            InvokeArgs = [ConnHandler, Operation, Message, From],
            erlang:apply(mongodb_conn, send_message, InvokeArgs)
        end
    ),
    {noreply, #state{pools=Pools1, conns=Conns}};

handle_call(stop, _From, #state{pools=Pools, conns=Conns} =State) ->
  {Pools3, Conns3} = lists:foldl(
      fun(Pool, {Pools2, Conns2}) -> 
          #pool{id=PoolId} = Pool,
          stop_pool(PoolId, Pools2, Conns2)
      end,
      {Pools, Conns},
      gb_trees:values(Pools)),
  {stop, normal, #state{pools=Pools3, conns=Conns3}};

handle_call(_Request, _From, State) ->
    io:format("Ignored message received: ~p~n", [_Request]),
    {reply, ignored, State}.

handle_cast({PoolId, {send_message, Operation, Method}}, #state{pools=Pools, conns=Conns}) ->
    Pools1 = with_connection(
        PoolId, Pools,
        fun(ConnHandler) ->
            erlang:apply(mongodb_conn, send_message, [ConnHandler, Operation, Method])
        end
    ),
    {noreply, #state{pools=Pools1, conns=Conns}};
        
handle_cast(_Msg, State) ->
  ?debugVal([unknown_cast, _Msg]),
  {noreply, State}.

handle_info({'EXIT', FromPid, _Reason}, #state{pools=Pools, conns=Conns}) ->
    case gb_trees:lookup(FromPid, Conns) of
        {value, #pool{id=PoolId, connections=Connections}} ->
            %io:format(user, "removing connection whose pid = ~p from connections=~p~n", [FromPid, Connections]),
            Connections1 = remove_connection_by_pid(Connections, FromPid),
            Pool1 = #pool{id=PoolId, connections=Connections1},
            Pools1 = gb_trees:enter(PoolId, Pool1, Pools),
            Conns1 = gb_trees:delete(FromPid, Conns),
            {noreply, #state{pools=Pools1, conns=Conns1}};
        _ ->
            {noreply, #state{pools=Pools, conns=Conns}}
    end;

handle_info(_Info, State) ->
    %?debugVal(_Info),
    {noreply, State}.

terminate(_Reason, _State) ->
    %?debugVal(_Reason),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%----------------------------------------------------------------------
%%% Internal functions
%%%----------------------------------------------------------------------

%%====================================================================
%% Private stuff
%%====================================================================
create_pool(PoolId, Host, Port, #state{pools=Pools, conns=Conns}) ->
    Connections = gb_trees:empty(),
    Pool = #pool{id=PoolId, connections=Connections},
    {ConnPid, Pool1} = add_conn(Pool, Host, Port),
    Pools1 = gb_trees:enter(PoolId, Pool1, Pools),
    Conns1 = gb_trees:enter(ConnPid, Pool1, Conns),
    #state{pools=Pools1, conns=Conns1}.

add_conn(Pool, Host, Port) ->
    #pool{id=PoolId, connections=Connections} = Pool,
    {ok, ConnPid} = mongodb_conn:start_link(Host, Port),
    Connection = #connection{pid=ConnPid, host=Host, port=Port},
    Connections1 = gb_trees:enter(#connid{lf=0, pid=ConnPid}, Connection, Connections),
    {ConnPid, #pool{id=PoolId, connections=Connections1}}.

with_connection(PoolId, Pools, F) ->
    % get a connnection handler
    {value, #pool{id=PoolId, connections=Connections}} = gb_trees:lookup(PoolId, Pools),
    {#connid{lf=Usage, pid=ConnPid}, Connection, Connections1} = gb_trees:take_smallest(Connections),
    
    % assign call to connection handler
    spawn(fun() -> F(ConnPid) end),
    
    % update lf for the connection handler
    Usage1 = Usage + 1,
    Connections2 = gb_trees:enter(#connid{lf=Usage1, pid=ConnPid}, Connection, Connections1),
    Pool1 = #pool{id=PoolId, connections=Connections2},
    gb_trees:enter(PoolId, Pool1, Pools).

remove_connection_by_pid(Connections, ConnPid) ->
    remove_connection_by_pid(gb_trees:keys(Connections), Connections, ConnPid).
remove_connection_by_pid([#connid{lf=LF, pid=ConnPid}|_Rest], Connections, ConnPid) ->
    gb_trees:delete(#connid{lf=LF, pid=ConnPid}, Connections);
remove_connection_by_pid([_H|Rest], Connections, ConnPid) ->
    remove_connection_by_pid(Rest, Connections, ConnPid);
remove_connection_by_pid([], Connections, _ConnPid) ->
    Connections.

stop_pool(PoolId, Pools, Conns) ->    
    {value, #pool{id=PoolId, connections=Connections}} = gb_trees:lookup(PoolId, Pools),
    spawn(fun() ->
            lists:foreach(
                fun(Conn) -> 
                    #connection{pid=ConnPid} = Conn,
                    mongodb_conn:stop(ConnPid)
                end,
                gb_trees:values(Connections))
        end),
    Pools1 = gb_trees:delete(PoolId, Pools),
    Conns1 = lists:foldl(
                fun(Conn, Acc) ->
                    #connection{pid=ConnPid} = Conn,
                    gb_trees:delete(ConnPid, Acc)
                end,
                Conns,
                gb_trees:values(Connections)),
    {Pools1, Conns1}.