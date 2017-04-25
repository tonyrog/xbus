%%% @author Tony Rogvall <tony@rogvall.se>
%%% @copyright (C) 2017, Tony Rogvall
%%% @doc
%%%    xbus api
%%% @end
%%% Created : 25 Apr 2017 by Tony Rogvall <tony@rogvall.se>

-module(xbus).
-export([start/0]).
-export([pub/2, pub/3]).
-export([pub_meta/2]).
-export([sub/1, sub/2]).
-export([sub_meta/1]).
-export([unsub/1]).
-export([read/1]).
-export([read_meta/1]).
-export([read_meta/2]).
-export([read_meta/3]).

-export([show_topics/0]).
-export([show_retained/0]).

-define(XBUS_SUBS,   xbus_subs).
-define(XBUS_RETAIN, xbus_retain).
-define(XBUS_SRV,    xbus_srv).

-type key()         :: binary().
-type pattern_key() :: binary().

%%
%% Start the application
%%
-spec start() -> {ok,[atom()]}.

start() ->
    application:ensure_all_started(xbus).

%%
%% publish/broadcast a value to all subscribers
%%
-spec pub(Topic::key(), Value::term()) -> boolean().

pub(Topic, Value) when is_binary(Topic) ->
    pub_(Topic,Value,true).

pub(Topic,Value,Retain) when is_binary(Topic), is_boolean(Retain) ->
    pub_(Topic,Value,Retain).

pub_(Topic,Value,Retain) ->
    retain(Topic,Value,Retain),
    tree_db_bin:fold_subscriptions(
      fun(TopicPattern,Pid,_TimeStamp,Acc) ->
	      Pid ! {xbus,tree_db_bin:external_key(TopicPattern),Topic,Value},
	      Acc
      end, true, ?XBUS_SUBS, Topic).

%%
%% retain a value
%%
-spec retain(Topic::key(), Value::term(), Retain::boolean()) -> boolean().

retain(Topic,Value,true) ->
    tree_db_bin:insert(?XBUS_RETAIN, {Topic,Value});    
retain(_Topic,_Value,false) ->
    true.

%%
%% publish/retain meta informatin about a topic
%%
-spec pub_meta(Topic::key(), Data::[{atom(),term()}]) -> boolean().

pub_meta(Topic, Data) when is_binary(Topic) ->
    pub_(<<"{META}.", Topic/binary>>, Data, true).

%% subscribe to a topic, Topic may contain components with * or ?
%% also match any retained values
-spec sub(TopicPattern::pattern_key()) -> boolean().
sub(TopicPattern) when is_binary(TopicPattern) ->
    sub_(TopicPattern,true).

-spec sub(TopicPattern::pattern_key(),Retained::boolean()) -> boolean().

sub(TopicPattern,Retained) when is_binary(TopicPattern), is_boolean(Retained) ->
    sub_(TopicPattern,Retained).
    
sub_(TopicPattern,Retained) ->
    retained(TopicPattern,Retained),
    true = tree_db_bin:subscribe(?XBUS_SUBS, TopicPattern, self()),
    gen_server:cast(?XBUS_SRV, {monitor,TopicPattern, self()}),
    true.

sub_meta(TopicPattern) when is_binary(TopicPattern) ->
    sub_(<<"{META}.",TopicPattern/binary>>, true).

%% Filter retained values with TopicPattern to get the current values
retained(TopicPattern,true) ->
    tree_db_bin:fold_matching(
      ?XBUS_RETAIN, TopicPattern,
      fun({Topic,Value},_Acc) ->
	      self() ! {xbus,TopicPattern,
			tree_db_bin:external_key(Topic),
			Value},
	      true
      end, true);
retained(_TopicPattern,false) ->
    true.
%%
%% remove subscription
%%
-spec unsub(TopicPattern::pattern_key()) -> boolean().

unsub(TopicPattern) when is_binary(TopicPattern) ->
    case tree_db_bin:unsubscribe(?XBUS_SUBS, TopicPattern, self()) of
	true ->
	    gen_server:cast(?XBUS_SRV, {demonitor,TopicPattern, self()}),
	    true;
	false ->
	    false
    end.

%%
%% Read retain values
%%
-spec read(Topic::key()) -> [{key(),term()}].

read(Topic) ->
    tree_db_bin:lookup(?XBUS_RETAIN, Topic).

-spec read_meta(Topic::key()) -> [{atom(),term()}].
read_meta(Topic) ->
    case tree_db_bin:lookup(?XBUS_RETAIN, <<"{META}.",Topic/binary>>) of
	[] -> [];
	[{_,Meta}] -> Meta
    end.

-spec read_meta(Topic::key(),Key::atom()) -> term() | undefined.
read_meta(Topic,Key) ->
    read_meta(Topic,Key,undefined).

-spec read_meta(Topic::key(),Key::atom(),Default::term()) -> term().
read_meta(Topic,Key,Default) ->
    Meta = read_meta(Topic),
    proplists:get_value(Key, Meta, Default).

%% show local topics
show_topics() ->
    tree_db_bin:fold_matching(
      ?XBUS_RETAIN, <<"{META}.*">>,
      fun({[<<"{META}">>|Topic],Meta},_Acc) ->
	      io:format("~p  meta=~p\n", [tree_db_bin:external_key(Topic),
					  Meta])
      end, ok).

show_retained() ->
    tree_db_bin:fold_matching(
      ?XBUS_RETAIN, <<"{META}.*">>,
      fun({[<<"{META}">>|Topic],Meta},_Acc) ->
	      Topic1 = tree_db_bin:external_key(Topic),
	      case read(Topic1) of
		  [] -> ok;
		  [{_,Value}] ->
		      Unit = proplists:get_value(unit,Meta,""),
		      io:format("~p  ~w~s\n", [Topic1,Value,Unit])
	      end
      end, ok).
