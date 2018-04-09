-module(msgr).

-include_lib("marina/include/marina.hrl").
-include_lib("cqerl/include/cqerl.hrl").

-behaviour(gen_server).

%% API
-export([
         start_link/3, start_link/4
        ]).

-compile(export_all).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state,
        {
          channel :: binary(),
          rate :: pos_integer(),
          latest :: pos_integer(),
          driver = marina :: atom(),
          limit = inf :: pos_integer() | inf,
          start = erlang:monotonic_time(milli_seconds)
        }).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Channel, Rate, Driver) ->
    start_link(Channel, Rate, Driver, inf).

start_link(Channel, Rate, Driver, Limit) ->
    gen_server:start_link(?MODULE, [Channel, Rate, Driver, Limit], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Channel, Rate, Driver, Limit]) ->
    Interval = calc_interval(Rate),
    %% check if the conversation exists
    case convo_exists(Driver, Channel) of
        {true, Latest} ->
            ok;
        false ->
            Latest = make_convo(Driver)
    end,
    %% fetch the latest
    {ok, #state{channel = Channel,
                rate = Rate,
                driver = Driver,
                latest = Latest,
                limit = Limit}, Interval}.

handle_call(_Request, _From, State) ->
    lager:warning("unexpected call ~p from ~p", [_Request, _From]),
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    lager:warning("unexpected cast ~p", [_Msg]),
    {noreply, State}.

handle_info(timeout, #state{limit = 0,
                            channel = Channel,
                            start = Start} = State) ->
    lager:info("channel ~p took ~pms",
               [Channel, erlang:monotonic_time(milli_seconds) - Start]),
    {stop, normal, State};
handle_info(timeout, #state{latest = Latest0,
                            driver = Driver,
                            channel = Channel,
                            limit = Limit0} = State) ->
    %% keep track of time better for more stable rates

    %% make this %age configurable
    case rand:uniform(10) of
        N when N =< 11 ->
            %% do a write
            %% TODO grab timings
            {ok, Latest} = write_msg(Driver, Channel, Latest0);
        _ ->
            %% do a read
            %%_ = read_msgs(Driver, Channel),
            Latest = Latest0
    end,
    {noreply, State#state{latest = Latest, limit = dec_limit(Limit0)},
     calc_interval(State#state.rate)};
handle_info(_Info, State) ->
    lager:warning("unexpected message ~p", [_Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

calc_interval(Rate) ->
    %% rates past 1000 are effectively: go as fast as we can go
    trunc(1000 / Rate).

dec_limit(inf) ->
    inf;
dec_limit(N) ->
    N - 1.


%%send_message(marina, Convo, Text,
convo_exists(marina, ID) ->
    case marina:query(<<"select latest, name from test.conversation where id = ?">>,
                      [marina_types:encode_int(ID)],
                      ?CONSISTENCY_ONE, [{skip_metadata, true}], 1000) of
        {ok, #result{rows = [[LatestBin, Name]]}} ->
            <<Latest:32/signed-integer>> = LatestBin,
            lager:info("got existing convo with name ~p and latest ~p",
                       [Name, Latest]),
            {true, Latest};
        Err ->
            lager:info("got something else: ~p", [Err]),
            false
    end;
convo_exists(cqerl, ID) ->
    {ok, Client} = cqerl:get_client({}),
    Ret =
    case cqerl:run_query(
           Client,
           #cql_query{
              statement = <<"select latest, name from test.conversation where id = ?">>,
              values = [{id, ID}]}) of
        {ok, Result} ->
            case cqerl:all_rows(Result) of
                [[{latest, Latest}, {name, Name}]] ->
                    lager:info("got existing convo with name ~p and latest ~p",
                               [Name, Latest]),
                    {true, Latest};
                Err ->
                    lager:info("got something else: ~p", [Err]),
                    false
            end
    end,
    cqerl:close_client(Client),
    Ret.

make_convo(marina) ->
    L = fun Loop(_, _, 0) ->
                {error, too_many_retries};
            Loop(Highest0, New, Tries) ->
                case marina:query(<<"update test.convos set highest = ? "
                                    "where space = 'test' if highest = ?">>,
                                  [marina_types:encode_int(New),
                                   marina_types:encode_int(Highest0)],
                                  ?CONSISTENCY_ONE, [], 1000) of
                    {ok, #result{rows = [[<<0>>, HighestBin]]}} ->
                        <<Highest:32/signed-integer>> = HighestBin,
                        lager:info("cas retry, highest = ~p", [Highest]),
                        Loop(Highest, Highest + 1, Tries - 1);
                    {ok, #result{rows = [[<<1>>]]}} ->
                        {ok, New}
                end
        end,
    %% this will always retry at least once, but I am lazy
    case L(-1, -1, 20) of
        {ok, CID} ->
            case marina:query(<<"insert into test.conversation (id, latest) "
                                "values (?, ?)">>,
                              [marina_types:encode_int(CID),
                               marina_types:encode_int(0)],
                              ?CONSISTENCY_ONE, [], 1000) of
                {ok, undefined} ->
                    %% i guess undefined is the correct response for inserts?
                    0
            end;
        {error, Reason} -> {error, Reason}
    end;
make_convo(cqerl) ->
    {ok, Client} = cqerl:get_client({}),
    L = fun Loop(_, _, 0) ->
                {error, too_many_retries};
            Loop(Highest0, New, Tries) ->
                case cqerl:run_query(
                       Client,
                       #cql_query{
                          statement = <<"update test.convos set highest = :highest1 "
                                        "where space = 'test' if highest = :highest">>,
                          values = [{highest1, New}, {highest, Highest0}]}) of
                    {ok, Result} ->
                        case cqerl:all_rows(Result) of
                            [[{'[applied]', false},{highest, Highest}]] ->
                                lager:info("cas retry, highest = ~p", [Highest]),
                                Loop(Highest, Highest + 1, Tries - 1);
                            [[{'[applied]', true}]] ->
                                {ok, New}
                        end
                end
        end,
    %% this will always retry at least once, but I am lazy
    Ret =
    case L(-1, -1, 20) of
        {ok, CID} ->
            case cqerl:run_query(
                   Client,
                   #cql_query{
                      statement = <<"insert into test.conversation (id, latest) "
                                    "values (?, ?)">>,
                      values = [{id, CID}, {latest, 0}]}) of
                {ok, _Response} ->
                    0
            end;
        {error, Reason} -> {error, Reason}
    end,
    cqerl:close_client(Client),
    Ret.

-define(DUMMY,
        <<"here is some dummy message text to make this example slightly less"
          " trivial and stupid.  on the other hand it won't really be long enough"
          " because I am too lazy to keep typing for long">>).

write_msg(marina, Channel, Latest) ->
    L = fun Loop(_, 0) ->
                {error, too_many_retries};
            Loop(L0, Tries) ->
                case marina:query(<<"update test.conversation set latest = ? "
                                    "where id = ? if latest = ?">>,
                                  [marina_types:encode_int(L0 + 1),
                                   marina_types:encode_int(Channel),
                                   marina_types:encode_int(L0)],
                                  ?CONSISTENCY_ONE, [], 1000) of
                    {ok, #result{rows = [[<<0>>, LBin]]}} ->
                        <<L:32/signed-integer>> = LBin,
                        lager:info("cas retry, latest = ~p", [L]),
                        Loop(L, Tries - 1);
                    {ok, #result{rows = [[<<0>>]]}} ->
                        Loop(L0, Tries - 1);
                    {ok, #result{rows = [[<<1>>]]}} ->
                        {ok, L0 + 1}
                end
        end,
    %% this will always retry at least once, but I am lazy
    case L(Latest, 50) of
        {ok, ID} ->
            case marina:query(<<"insert into test.message (convo, msg_id, message) "
                                "values (?, ?, ?)">>,
                              [marina_types:encode_int(Channel),
                               marina_types:encode_int(ID),
                               ?DUMMY],
                              ?CONSISTENCY_ONE, [], 1000) of
                {ok, undefined} ->
                    %% i guess undefined is the correct response for inserts?
                    {ok, ID}
            end;
        {error, Reason} -> {error, Reason}
    end;
write_msg(cqerl, Channel, Latest) ->
    {ok, Client} = cqerl:get_client({}),
    L = fun Loop(_, 0) ->
                {error, too_many_retries};
            Loop(L0, Tries) ->
            case cqerl:run_query(
                   Client,
                   #cql_query{
                      statement = <<"update test.conversation set latest = :latest1 "
                                    "where id = ? if latest = :latest">>,
                      values = [{latest1, L0 + 1}, {id, Channel}, {latest, L0}]}) of
                {ok, Result} ->
                    case cqerl:all_rows(Result) of
                        [[{'[applied]', false},{latest, L}]] ->
                            lager:info("cas retry, latest = ~p", [L]),
                            Loop(L, Tries - 1);
                        [[{'[applied]', true}]] ->
                            {ok, L0 + 1}
                    end
            end
        end,
    %% this will always retry at least once, but I am lazy
    Ret =
        case L(Latest, 20) of
            {ok, ID} ->
                case cqerl:run_query(
                       Client,
                       #cql_query{
                          statement = <<"insert into test.message (convo, msg_id, message) "
                                        "values (?, ?, ?)">>,
                          values = [{convo, Channel}, {msg_id, ID}, {message, ?DUMMY}]}) of
                    {ok, _Response} ->
                        {ok, ID}
                end;
            {error, Reason} -> {error, Reason}
        end,
    cqerl:close_client(Client),
    Ret.

