%%% @author Tony Rogvall <tony@rogvall.se>
%%% @copyright (C) 2017, Tony Rogvall
%%% @doc
%%%    xbus api
%%% @end
%%% Created : 25 Apr 2017 by Tony Rogvall <tony@rogvall.se>

-module(xbus).

-export([start/0]).
-export([pub/2, pub/3]).
-export([sub/1, sub/2]).
-export([unsub/1]).

-export([pub_meta/2]).
-export([sub_meta/1]).
-export([unsub_meta/1]).

-export([write/2, write/3]).
-export([write_p/2, write_p/3]).

-export([read/1, read_value/1, read_value_ts/1]).
-export([read_p/1, read_p_value/1, read_p_value_ts/1]).
-export([read_n/3]).
-export([read_meta/1,read_meta/2,read_meta/3]).

-export([timestamp/0]).
-export([show_topics/0]).
-export([show_retained/0]).
-export([show_q/1]).

-define(XBUS_SUBS,   xbus_subs).
-define(XBUS_RETAIN, xbus_retain).
-define(XBUS_SRV,    xbus_srv).

-type key()         :: binary() | string().
-type pattern_key() :: binary().
-type timestamp()   :: integer().

-define(timestamp(), tree_db_bin:timestamp()).
%%
%% Start the application
%%
-spec start() -> {ok,[atom()]}.

start() ->
    application:ensure_all_started(xbus).

%%
%% publish/broadcast a value with timestamp to all subscribers
%%
-spec pub(Topic::key(), Value::term()) -> boolean().

pub(Topic, Value) when is_binary(Topic) ->
    pub_(Topic, Value, ?timestamp());
pub(Topic, Value) when is_list(Topic) ->
    pub_(list_to_binary(Topic), Value, ?timestamp()).


-spec pub(Topic::key(), Value::term(), Timestamp::timestamp()) -> boolean().

pub(Topic, Value, TimeStamp) when is_binary(Topic), is_integer(TimeStamp) ->
    pub_(Topic, Value, TimeStamp);
pub(Topic, Value, TimeStamp) when is_list(Topic), is_integer(TimeStamp) ->
    pub_(list_to_binary(Topic), Value, TimeStamp).

pub_(Topic, Value, TimeStamp) ->
    retain_(Topic, Value, TimeStamp),
    tree_db_bin:fold_subscriptions(
      fun(TopicPattern,Pid,_PatternTimeStamp,Acc) ->
	      Pid ! {xbus,tree_db_bin:external_key(TopicPattern),
		     #{ topic=>Topic,
			value=>Value,
			timestamp=>TimeStamp}},
	      Acc
      end, true, ?XBUS_SUBS, Topic).

%%
%% retain a queue of values
%% a.b.c     will get the original value
%% a.b.c.#   contains current retain position
%% 
retain_(Topic, Value, TimeStamp) ->
    Meta = read_meta(Topic),
    case retain(Meta) of
	0 ->
	    true;
	1 ->
	    case persistent(Meta) of
		true ->
		    write_p(Topic,Value,TimeStamp);
		false -> 
		    true
	    end,
	    write(Topic,Value,TimeStamp);
	R when R > 0 ->
	    TopicC = <<Topic/binary,".#">>,
	    I = tree_db_bin:update_counter(?XBUS_RETAIN, TopicC, 1),
	    TopicI = join(Topic,I),
	    case persistent(Meta) of
		true ->
		    write_p(TopicC, I),
		    write_p(TopicI,Value,TimeStamp),
		    write_p(Topic,Value,TimeStamp);
		false ->
		    true
	    end,
	    write(TopicI,Value,TimeStamp),
	    write(Topic,Value,TimeStamp)
    end.

%%
%% publish/retain meta informatin about a topic
%% FIXME: code must be added to handle update
%%        of retain value. The easiest whould be
%%        to delete all data and restart, but that
%%        would be boring for persistent data... 
%%
-spec pub_meta(Topic::key(), Data::[{atom(),term()}]) -> boolean().

pub_meta(Topic, Data) when is_binary(Topic) ->
    pub_meta_(Topic, Data);
pub_meta(Topic, Data) when is_list(Topic) ->
    pub_meta_(list_to_binary(Topic), Data).

pub_meta_(Topic, Data) when is_binary(Topic) ->
    MetaTopic = <<"{META}.", Topic/binary>>,
    TimeStamp = ?timestamp(),
    case retain(Data) of
	0 ->
	    ok;
	_ ->
	    TopicC = <<Topic/binary,".#">>,
	    write(TopicC, -1, TimeStamp),
	    case persistent(Data) of
		true ->
		    write_p(TopicC, -1, TimeStamp),
		    write_p(MetaTopic, Data, TimeStamp);
		false ->
		    true
	    end
    end,
    pub_(MetaTopic, Data, TimeStamp).

%% subscribe to a topic, Topic may contain components with * or ?
%% also match any retained values
-spec sub(TopicPattern::pattern_key()) -> boolean().
sub(TopicPattern) when is_binary(TopicPattern) ->
    sub_(TopicPattern,true);
sub(TopicPattern) when is_list(TopicPattern) ->
    sub_(list_to_binary(TopicPattern),true).

-spec sub(TopicPattern::pattern_key(),Retained::boolean()) -> boolean().

sub(TopicPattern,Retained) when is_binary(TopicPattern), is_boolean(Retained) ->
    sub_(TopicPattern,Retained);
sub(TopicPattern,Retained) when is_list(TopicPattern), is_boolean(Retained) ->
    sub_(list_to_binary(TopicPattern),Retained).
    
sub_(TopicPattern,Retained) ->
    retained(TopicPattern,Retained),
    true = tree_db_bin:subscribe(?XBUS_SUBS, TopicPattern, self()),
    gen_server:cast(?XBUS_SRV, {monitor,TopicPattern, self()}),
    true.

sub_meta(TopicPattern) when is_binary(TopicPattern) ->
    sub_(<<"{META}.",TopicPattern/binary>>, true);
sub_meta(TopicPattern) when is_list(TopicPattern) ->
    sub_(list_to_binary(["{META}.",TopicPattern]), true).

%% Filter retained values with TopicPattern to get the current values
retained(TopicPattern,true) ->
    tree_db_bin:fold_matching(
      ?XBUS_RETAIN, TopicPattern,
      fun({Topic,Value},_Acc) ->
	      %% FIXME: make fold_matching return timetamp as well
	      {_Value,TimeStamp} = tree_db_bin:get_ts(?XBUS_RETAIN,Topic),
	      self() ! {xbus,TopicPattern,
			#{ topic=>tree_db_bin:external_key(Topic),
			   value=>Value,
			   timestamp=>TimeStamp}},
	      true
      end, true);
retained(_TopicPattern,false) ->
    true.
%%
%% remove subscription
%%
-spec unsub(TopicPattern::pattern_key()) -> boolean().

unsub(TopicPattern) when is_binary(TopicPattern) ->
    unsub_(TopicPattern);
unsub(TopicPattern) when is_list(TopicPattern) ->
    unsub_(list_to_binary(TopicPattern)).

unsub_(TopicPattern) ->
    case tree_db_bin:unsubscribe(?XBUS_SUBS, TopicPattern, self()) of
	true ->
	    gen_server:cast(?XBUS_SRV, {demonitor,TopicPattern, self()}),
	    true;
	false ->
	    false
    end.

%%
%% remove subscription on meta information
%%
-spec unsub_meta(TopicPattern::pattern_key()) -> boolean().

unsub_meta(TopicPattern) when is_binary(TopicPattern) ->
    unsub_(<<"{META}.",TopicPattern/binary>>);
unsub_meta(TopicPattern) when is_list(TopicPattern) ->
    unsub_(list_to_binary(["{META}.",TopicPattern])).

%%
%% Raw write retained values
%%
-spec write(Topic::key(), Value::term()) -> true.

write(Topic, Value) ->
    write(Topic, Value, ?timestamp()).
    
-spec write(Topic::key(), Value::term(),TimeStamp::integer()) -> true.

write(Topic, Value, TimeStamp) when is_integer(TimeStamp) ->
    tree_db_bin:put(?XBUS_RETAIN,Topic,Value,TimeStamp).

%%
%% write persistent retained values
%%
-spec write_p(Topic::key(), Value::term()) -> true.

write_p(Topic, Value) ->
    write_p(Topic, Value, ?timestamp()).
    
-spec write_p(Topic::key(), Value::term(),TimeStamp::integer()) -> true.

write_p(Topic, Value, TimeStamp) when is_integer(TimeStamp) ->
    Element = {tree_db_bin:internal_key(Topic),Value,TimeStamp},
    dets:insert(?XBUS_RETAIN, Element).

%%
%% Read retain values
%%
-spec read_p(Topic::key()) -> [{key(),term()}].

read_p(Topic) ->
    [{Key,Value,_}]=dets:lookup(?XBUS_RETAIN, tree_db_bin:internal_key(Topic)),
    [{tree_db_bin:external_key(Key),Value}].

-spec read_p_value(Topic::key()) -> term().

read_p_value(Topic) ->
    [{_,Value,_}] = dets:lookup(?XBUS_RETAIN, tree_db_bin:internal_key(Topic)),
    Value.

-spec read_p_value_ts(Topic::key()) -> {term(), timestamp()}.

read_p_value_ts(Topic) ->
    [{_,Value,Ts}] = dets:lookup(?XBUS_RETAIN, tree_db_bin:internal_key(Topic)),
    {Value,Ts}.

%%
%% Read retain values
%%
-spec read(Topic::key()) -> [{key(),term()}].

read(Topic) ->
    tree_db_bin:lookup(?XBUS_RETAIN, Topic).

-spec read_value(Topic::key()) -> term().

read_value(Topic) ->
    tree_db_bin:get(?XBUS_RETAIN, Topic).

-spec read_value_ts(Topic::key()) -> {term(), timestamp()}.

read_value_ts(Topic) ->
    tree_db_bin:get_ts(?XBUS_RETAIN, Topic).

%%
%% Read n elements >= timestamp()
%%
read_n(Topic, N, TimeStamp) when is_binary(Topic),
				 is_integer(N), N>0,
				 is_integer(TimeStamp) ->
    case tree_db_bin:lookup_ts(?XBUS_RETAIN, <<Topic/binary,".#">>) of
	[] -> 
	    [];
	[{_,Pos,_}] ->
	    Meta = read_meta(Topic),
	    case retain(Meta) of
		0 -> ok;
		R when Pos < R ->
		    %% available values are 0..Pos
		    %% binary search for Timetamp in range 0..Pos
		    BPos = bin_search_pos(Topic, TimeStamp, 0, Pos, R),
		    read_n_(Topic, N, TimeStamp, BPos, R);
		R when Pos >= R ->
		    %% values are in Pos+1..Pos+R-1 (mod R)
		    BPos = bin_search_pos(Topic, TimeStamp, Pos+1, Pos+R-1, R),
		    read_n_(Topic, N, TimeStamp, BPos, R)
	    end
    end.

read_n_(Topic, N, TimeStamp, {true,I}, R) ->
    read_n__(Topic, N, TimeStamp, I, R, []);
read_n_(Topic, N, TimeStamp, {false,I}, R) ->
    read_n__(Topic, N, TimeStamp, I, R, []).

read_n__(_Topic, 0, _TimeStamp, _Pos, _R, Acc) ->
    lists:reverse(Acc);
read_n__(Topic, I, TimeStamp, Pos, R, Acc) ->
    case tree_db_bin:lookup_ts(?XBUS_RETAIN, join(Topic,Pos rem R)) of
	[{Key,Value,TimeStamp1}] when TimeStamp1 >= TimeStamp ->
	    read_n__(Topic,I-1,TimeStamp,Pos+1,R,[{Key,Value,TimeStamp1}|Acc]);
	[_] ->
	    lists:reverse(Acc)
    end.

%%
%% Search for Element with Timestamp >= TimeStamp 
%%

bin_search_pos(_Topic, _TimeStamp, Low, High, N) when Low > High ->
    {false,Low rem N};
bin_search_pos(Topic, TimeStamp, Low, High, N) ->
    Mid = (Low + High) div 2,
    [{_,_,TimeStamp1}] = tree_db_bin:lookup_ts(?XBUS_RETAIN, 
					       join(Topic,Mid rem N)),
    if TimeStamp < TimeStamp1 ->
	    bin_search_pos(Topic, TimeStamp, Low, Mid-1, N);
       TimeStamp > TimeStamp1 ->
	    bin_search_pos(Topic, TimeStamp, Mid+1, High, N);
       true ->
	    {true,Mid rem N}
    end.
    

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
    meta_value(Key, Meta, Default).

persistent(Meta) ->
    meta_value(persistent, Meta, false).

retain(Meta) ->
    meta_value(retain, Meta, 1).

meta_value(Key, Meta, Default) when is_list(Meta) ->
    proplists:get_value(Key, Meta, Default);
meta_value(Key, Meta, Default) when is_map(Meta) ->
    maps:get(Key, Meta, Default).

%% show local topics
show_topics() ->
    tree_db_bin:fold_matching(
      ?XBUS_RETAIN, <<"{META}.*">>,
      fun({[<<"{META}">>|Topic],Meta},_Acc) ->
	      io:format("~p  meta=~p\n", [tree_db_bin:external_key(Topic),
					  Meta])
      end, ok).

%% Show retained data that has meta data, fixme show all data
show_retained() ->
    tree_db_bin:fold_matching(
      ?XBUS_RETAIN, <<"{META}.*">>,
      fun({[<<"{META}">>|Topic],Meta},_Acc) ->
	      Topic1 = tree_db_bin:external_key(Topic),
	      case read(Topic1) of
		  [] -> ok;
		  [{_,Value}] ->
		      Unit = meta_value(unit,Meta,""),
		      io:format("~p  ~w~s\n", [Topic1,Value,Unit])
	      end
      end, ok).

join(Topic, I) when is_binary(Topic), is_integer(I), I>0 ->
    <<Topic/binary,$.,(integer_to_binary(I))/binary>>.

timestamp() ->
    ?timestamp().

%% system date
-define(UNIX_SECONDS, 62167219200).

%% Timestamps are in micro seconds (pretend in unix time for now)
datetime_us_from_timestamp(Timestamp) ->
    Seconds = (Timestamp div 1000000) + ?UNIX_SECONDS,
    Us = (Timestamp rem 1000000),
    {D,T} = calendar:gregorian_seconds_to_datetime(Seconds),
    {D,T,Us}.

-define(DATE_FORMAT, "~4..0w-~2..0w-~2..0w ~2..0w:~2..0w:~2..0w.~w UTC").

%%
%% Show queue values
%%

show_q(Topic) ->
    case read(<<Topic/binary,".#">>) of
	[] -> ok;
	[{_,Pos}] ->
	    Meta = read_meta(Topic),
	    case retain(Meta) of
		0 ->
		    ok;
		1 ->
		    show_value(Topic);
		R when Pos < R ->
		    lists:foreach(
		      fun(J) ->
			      show_value(Topic, J)
		      end, lists:seq(0, Pos));
		R when Pos >= R ->
		    lists:foreach(
		      fun(J) ->
			      show_value(Topic, J)
		      end, lists:seq(0, R-1))
	    end
    end.

show_value(Topic) ->
    {Value,Ts} = read_value_ts(Topic),
    {{Y,M,D},{TH,TM,TS},Micro} = datetime_us_from_timestamp(Ts),
    io:format("~s = ~w [" ?DATE_FORMAT "]\n",
	      [Topic, Value,
	       Y,M,D,TH,TM,TS,Micro]).

show_value(Topic, I) ->
    {Value,Ts} = read_value_ts(join(Topic,I)),
    {{Y,M,D},{TH,TM,TS},Micro} = datetime_us_from_timestamp(Ts),
    io:format("~s.~w = ~w [" ?DATE_FORMAT "]\n",
	      [Topic, I, Value,
	       Y,M,D,TH,TM,TS,Micro]).
