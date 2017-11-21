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
-export([sub_ack/1, sub_ack/2]).
-export([ack/1, ack/2]).
-export([unsub/1]).

-export([pub_meta/2]).
-export([sub_meta/1]).
-export([unsub_meta/1]).

-export([write/2, write/3]).
-export([write_p/2, write_p/3]).

-export([read/1]).
-export([read_p/1]).
-export([read_n/3]).
-export([read_meta/1]).

-export([timestamp/0]).
-export([show_topics/0]).
-export([show_retained/0]).
-export([show_q/1]).
-export([datetime_us_from_timestamp/1]).
-export([send_retained/2]).

-define(XBUS_SUBS,   xbus_subs).
-define(XBUS_RETAIN, xbus_retain).
-define(XBUS_ACK,    xbus_ack).
-define(XBUS_SRV,    xbus_srv).

%% system date
-define(UNIX_SECONDS, 62167219200).

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
	      XTopicPattern = tree_db_bin:external_key(TopicPattern),
	      case is_ack(Pid,XTopicPattern, TimeStamp) of
		  true ->
		      Pid ! {xbus,XTopicPattern,
			     #{ topic=>Topic,
				value=>Value,
				timestamp=>TimeStamp}};
		  false ->
		      ignore
	      end,
	      Acc
      end, true, ?XBUS_SUBS, Topic).

%%
%% retain a queue of values
%% a.b.c     will get the original value
%% a.b.c.#   contains current retain position
%%
retain_(MetaTopic= <<"{META}.",_/binary>>, Value, TimeStamp) ->
    write(MetaTopic,Value,TimeStamp);    
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
	    TopicI = join(Topic,I rem R),
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

pub_meta(Topic, Meta) when is_binary(Topic) ->
    pub_meta_(Topic, Meta);
pub_meta(Topic, Meta) when is_list(Topic) ->
    pub_meta_(list_to_binary(Topic), Meta).

pub_meta_(Topic, Meta) when is_binary(Topic) ->
    MetaTopic = <<"{META}.", Topic/binary>>,
    TimeStamp = ?timestamp(),
    case retain(Meta) of
	0 ->
	    ok;
	_ ->
	    TopicC = <<Topic/binary,".#">>,
	    write(TopicC, -1, TimeStamp),
	    case persistent(Meta) of
		true ->
		    write_p(TopicC, -1, TimeStamp),
		    write_p(MetaTopic, Meta, TimeStamp);
		false ->
		    true
	    end
    end,
    pub_(MetaTopic, Meta, TimeStamp).

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

%% subscribe to a topic, Topic may contain components with * or ?
%% also match any retained values, require ack before next value is sent
-spec sub_ack(TopicPattern::pattern_key()) -> boolean().
sub_ack(TopicPattern) when is_binary(TopicPattern) ->
    sub_ack_(TopicPattern,true);
sub_ack(TopicPattern) when is_list(TopicPattern) ->
    sub_ack_(list_to_binary(TopicPattern),true).

-spec sub_ack(TopicPattern::pattern_key(),Retained::boolean()) -> boolean().

sub_ack(TopicPattern,Retained) when is_binary(TopicPattern),
				    is_boolean(Retained) ->
    sub_ack_(TopicPattern,Retained);
sub_ack(TopicPattern,Retained) when is_list(TopicPattern),
				    is_boolean(Retained) ->
    sub_ack_(list_to_binary(TopicPattern),Retained).

sub_ack_(TopicPattern,Retained) ->
    ets:insert(?XBUS_ACK, {{TopicPattern,self()},0,timestamp()}),
    sub_(TopicPattern,Retained).

sub_(TopicPattern,Retained) ->
    retained_(self(),TopicPattern,Retained),
    true = tree_db_bin:subscribe(?XBUS_SUBS, TopicPattern, self()),
    gen_server:cast(?XBUS_SRV, {monitor,TopicPattern, self()}),
    true.

sub_meta(TopicPattern) when is_binary(TopicPattern) ->
    sub_(<<"{META}.",TopicPattern/binary>>, true);
sub_meta(TopicPattern) when is_list(TopicPattern) ->
    sub_(list_to_binary(["{META}.",TopicPattern]), true).

%% Filter retained values with TopicPattern to get the current values
retained_(Pid,TopicPattern,true) ->
    case ets:lookup(?XBUS_ACK, {TopicPattern,Pid}) of
	[] ->
	    send_retained(Pid,TopicPattern);
	[{_,0,_}] ->
	    send_retained(Pid,TopicPattern);
	[_] ->
	    true
    end;
retained_(_Pid,_TopicPattern,false) ->
    true.

send_retained(Pid,TopicPattern) ->
    tree_db_bin:fold_matching_ts(
      ?XBUS_RETAIN, TopicPattern,
      fun({Topic,Value,TimeStamp},_Acc) ->
	      Pid ! {xbus,TopicPattern,
		     #{ topic=>tree_db_bin:external_key(Topic),
			value=>Value,
			timestamp=>TimeStamp}},
	      true
      end, true).

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

%% Check if topic is acknowledged {Key, AckCount, PubCount}.
is_ack(Pid, TopicPattern, TimeStamp) ->
    try ets:update_counter(?XBUS_ACK,{TopicPattern,Pid},{2,1}) of
	1 -> 
	    ets:update_counter(?XBUS_ACK,{TopicPattern,Pid},
			       {3,0,0,TimeStamp}),
	    true;
	_ -> false
    catch
	_:_ -> true
    end.

%% acknowledge topic and send retained values if needed
ack(TopicPattern) when is_binary(TopicPattern) ->
    ack_(self(),TopicPattern);
ack(TopicPattern) when is_list(TopicPattern) ->
    ack_(self(),list_to_binary(TopicPattern)).

ack(Pid,TopicPattern)  when is_pid(Pid), is_binary(TopicPattern) ->
    ack_(Pid,TopicPattern);
ack(Pid,TopicPattern)  when is_pid(Pid), is_list(TopicPattern) ->
    ack_(Pid,list_to_binary(TopicPattern)).

ack_(Pid, TopicPattern) ->
    try ets:update_counter(?XBUS_ACK,{TopicPattern,Pid},[{2,0},{2,0,0,0}]) of
	[0,_] -> ok;
	[_Pub,_] ->
	    send_retained(Pid,TopicPattern),
	    ok
    catch
	_:_ -> ok
    end.

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
-spec read_p(Topic::key()) -> [{key(),term(),timestamp()}].

read_p(Topic) ->
    [{tree_db_bin:external_key(K),V,T} ||
	{K,V,T} <- dets:lookup(?XBUS_RETAIN, tree_db_bin:internal_key(Topic))].

%%
%% Read retain values
%%
-spec read(Topic::key()) -> [{key(),term(),timestamp()}].

read(Topic) ->
    [{tree_db_bin:external_key(K),V,T} ||
	{K,V,T} <- tree_db_bin:lookup_ts(?XBUS_RETAIN, Topic)].

%%
%% Read n elements >= timestamp()
%%
read_n(Topic, N, TimeStamp) when is_list(Topic), is_integer(N), N>0 ->
    read_n_(list_to_binary(Topic), N, cvt_timestamp(TimeStamp));
read_n(Topic, N, TimeStamp) when is_binary(Topic), is_integer(N), N>0 ->
    read_n_(Topic, N, cvt_timestamp(TimeStamp)).

read_n_(Topic, N, TimeStamp) ->
    case read(<<Topic/binary,".#">>) of
	[] ->
	    [];
	[{_,Pos,_}] ->
	    Meta = read_meta(Topic),
	    case retain(Meta) of
		0 ->
		    [];
		R when Pos < R ->
		    %% available values are 0..Pos
		    %% binary search for Timetamp in range 0..Pos
		    BPos = bin_search_pos(Topic, TimeStamp, 0, Pos, R),
		    read_loop_n_(Topic, N, TimeStamp, BPos, R);
		R when Pos >= R ->
		    %% values are in Pos+1..Pos+R-1 (mod R)
		    BPos = bin_search_pos(Topic, TimeStamp, Pos+1, Pos+R, R),
		    read_loop_n_(Topic, N, TimeStamp, BPos, R)
	    end
    end.

read_loop_n_(Topic, N, TimeStamp, {true,I}, R) ->
    read_loop_n__(Topic, N, TimeStamp, I, R, []);
read_loop_n_(Topic, N, TimeStamp, {false,I}, R) ->
    read_loop_n__(Topic, N, TimeStamp, I, R, []).

read_loop_n__(_Topic, 0, _TimeStamp, _Pos, _R, Acc) ->
    lists:reverse(Acc);
read_loop_n__(Topic, I, TimeStamp, Pos, R, Acc) ->
    case read(join(Topic,Pos rem R)) of
	[E={_Key,_Value,TimeStamp1}] when TimeStamp1 >= TimeStamp ->
	    read_loop_n__(Topic,I-1,TimeStamp,Pos+1,R,[E|Acc]);
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
    [{_,_,TimeStamp1}] = read(join(Topic,Mid rem N)),
    if TimeStamp < TimeStamp1 ->
	    bin_search_pos(Topic, TimeStamp, Low, Mid-1, N);
       TimeStamp > TimeStamp1 ->
	    bin_search_pos(Topic, TimeStamp, Mid+1, High, N);
       true ->
	    {true,Mid rem N}
    end.

-spec read_meta(Topic::key()) -> [{atom(),term()}].
read_meta(Topic) when is_list(Topic) ->
    read_meta(list_to_binary(Topic));
read_meta(Topic) ->
    case read(<<"{META}.",Topic/binary>>) of
	[] -> [];
	[{_,Meta,_Ts}] -> Meta
    end.

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
		  [{_,Value,_Ts}] ->
		      Unit = meta_value(unit,Meta,""),
		      io:format("~p  ~w~s\n", [Topic1,Value,Unit])
	      end
      end, ok).

join(Topic, I) when is_binary(Topic), is_integer(I), I>=0 ->
    <<Topic/binary,$.,(integer_to_binary(I))/binary>>.

cvt_timestamp(TimeStamp={_Date,_Time}) ->
    S = calendar:datetime_to_gregorian_seconds(TimeStamp),
    (S-?UNIX_SECONDS)*1000000;
cvt_timestamp({Date,Time,Us}) ->
    S = calendar:datetime_to_gregorian_seconds({Date,Time}),
    (S-?UNIX_SECONDS)*1000000+Us;
cvt_timestamp(TimeStamp) when is_integer(TimeStamp), TimeStamp>=0 ->
    TimeStamp.

timestamp() ->
    ?timestamp().

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
show_q(Topic) when is_list(Topic) ->
    show_q(list_to_binary(Topic));
show_q(Topic) ->
    case read(<<Topic/binary,".#">>) of
	[] -> ok;
	[{_,Pos,_Ts}] ->
	    Meta = read_meta(Topic),
	    case retain(Meta) of
		0 ->
		    ok;
		1 ->
		    show_value(Topic);
		R when Pos < R ->
		    show_values_(Topic, 0, Pos, R);
		R when Pos >= R ->
		    show_values_(Topic, Pos+1, Pos+R, R)
	    end
    end.

show_values_(Topic, J, N, R) ->
    if J =< N ->
	    show_value(Topic, J rem R),
	    show_values_(Topic, J+1, N, R);
       true ->
	    ok
    end.

show_value(Topic) ->
    [{_,Value,Ts}] = read(Topic),
    {{Y,M,D},{TH,TM,TS},Micro} = datetime_us_from_timestamp(Ts),
    io:format("~s = ~w [" ?DATE_FORMAT "]\n",
	      [Topic, Value,
	       Y,M,D,TH,TM,TS,Micro]).

show_value(Topic, I) ->
    [{_,Value,Ts}] = read(join(Topic,I)),
    {{Y,M,D},{TH,TM,TS},Micro} = datetime_us_from_timestamp(Ts),
    io:format("~s.~w = ~w [" ?DATE_FORMAT "]\n",
	      [Topic, I, Value,
	       Y,M,D,TH,TM,TS,Micro]).
