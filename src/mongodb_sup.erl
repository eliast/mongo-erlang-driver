%%%----------------------------------------------------------------
%%% @author  Elias Torres <elias@torrez.us>
%%% @doc
%%% @end
%%%----------------------------------------------------------------
-module(mongodb_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    supervisor:start_link(mongodb_sup, []).
    
%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @spec init(Args) -> {ok, {SupFlags, [ChildSpec]}} |
%%                     ignore |
%%                     {error, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->

    MongoDbMain = {mongodb, {mongodb,start_link,[]},
        permanent, 10, worker, [mongodb]},

    {ok,{{one_for_one, 3, 10},[MongoDbMain]}}.
    
%%%===================================================================
%%% Internal functions
%%%===================================================================
