%%%-------------------------------------------------------------------
%% @doc cass_test top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(cass_test_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
         start_msgr/2, start_msgr/3, start_msgr/4]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

start_msgr(Channel, Rate) ->
    start_msgr(Channel, Rate, marina).

start_msgr(Channel, Rate, Driver) ->
    supervisor:start_child(?SERVER, [Channel, Rate, Driver]).

start_msgr(Channel, Rate, Driver, Limit) ->
    supervisor:start_child(?SERVER, [Channel, Rate, Driver, Limit]).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init(_Args) ->
    SupFlags = #{strategy => simple_one_for_one,
                 intensity => 0,
                 period => 1},

    ChildSpecs = [#{id => call,
                    start => {msgr, start_link, []},
                    shutdown => brutal_kill,
                    restart => temporary}],

    {ok, {SupFlags, ChildSpecs}}.

%%====================================================================
%% Internal functions
%%====================================================================
