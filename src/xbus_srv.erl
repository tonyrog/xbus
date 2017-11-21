%%%-------------------------------------------------------------------
%%% @author Tony Rogvall <tony@rogvall.se>
%%% @copyright (C) 2017, Tony Rogvall
%%% @doc
%%%    Administartiv process for xbus
%%% @end
%%% Created : 25 Apr 2017 by Tony Rogvall <tony@rogvall.se>
%%%-------------------------------------------------------------------
-module(xbus_srv).

-behaviour(gen_server).

-define(XBUS_SUBS,   xbus_subs).
-define(XBUS_RETAIN, xbus_retain).
-define(XBUS_ACK,    xbus_ack).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state,
	{
	  mref,  %% table(set):  Monitor => TopicPattern
	  tref   %% table(bag):  {TopicPattern,Pid} => Monitor
	}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    tree_db_bin:new(?XBUS_SUBS),
    tree_db_bin:new(?XBUS_RETAIN),
    ets:new(?XBUS_ACK, [public, named_table]),
    MRef = ets:new(xbus_mref, [set]),
    TRef = ets:new(xbus_tref, [bag]),
    init_persistent(application:get_env(xbus, persistent)),
    %% Always "retain" meta information if declared
    case application:get_env(xbus, topics) of
	{ok,List} ->
	    lists:foreach(
	      fun({Topic,Meta}) ->
		      xbus:pub_meta(Topic, Meta)
	      end, List);
	undefined ->
	    ok
    end,
    {ok, #state{ mref = MRef, tref = TRef }}.

init_persistent({ok,true}) ->
    case application:get_env(xbus, file) of
	{ok,File} ->
	    dets:open_file(?XBUS_RETAIN, [{access,read_write},{file,File}]),
	    dets:to_ets(?XBUS_RETAIN, ?XBUS_RETAIN);
	undefined ->
	    dets:open_file(?XBUS_RETAIN, [{access,read_write}]),
	    dets:to_ets(?XBUS_RETAIN, ?XBUS_RETAIN)
    end;
init_persistent(_) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({monitor,TopicPattern,Pid}, State) ->
    Mon = erlang:monitor(process, Pid),
    ets:insert(State#state.mref, {Mon, {TopicPattern,Pid}}),
    ets:insert(State#state.tref, {{TopicPattern,Pid}, Mon}),
    {noreply, State};
handle_cast({demonitor,TopicPattern,Pid}, State) ->
    lists:foreach(
      fun({_, Mon}) ->
	      erlang:demonitor(Mon,[flush]),
	      ets:delete(State#state.mref, Mon),
	      ets:delete_object(State#state.tref, {{TopicPattern,Pid},Mon})
      end, ets:lookup(State#state.tref, {TopicPattern,Pid})),
    ets:delete(?XBUS_ACK, {TopicPattern,Pid}),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------

handle_info({'DOWN',Mon,process,Pid,_Reason}, State) ->
    %% io:format("process ~p died, cleaning up, reason ~p\n", [Pid,_Reason]),
    case ets:lookup(State#state.mref, Mon) of
	[{_, {TopicPattern,Pid}}] ->
	    %% clean up subscription
	    tree_db_bin:unsubscribe(?XBUS_SUBS, TopicPattern, Pid),
	    ets:delete_object(State#state.tref, {{TopicPattern,Pid},Mon}),
	    ets:delete(State#state.mref, Mon),
	    ets:delete(?XBUS_ACK, {TopicPattern,Pid});
	[] ->
	    %% already deleted
	    ok
    end,
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
