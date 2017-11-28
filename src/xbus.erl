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

-export([read_p/1]).
-export([read_n/2]).
-export([read/1]).
-export([read_meta/1]).

-export([timestamp/0]).
-export([topics/0, topics/1, show_topics/0, show_topics/1]).
-export([retained/0, retained/1, show_retained/0, show_retained/1]).
-export([show_q/1]).
-export([datetime_us_from_timestamp/1]).
-export([send_retained/2]).

-define(XBUS_SUBS,   xbus_subs).
-define(XBUS_RETAIN, xbus_retain).
-define(XBUS_ACK,    xbus_ack).
-define(XBUS_SRV,    xbus_srv).

-define(META_PREFIX,   "$M").
-define(DATA_PREFIX,   "$D").
-define(RETAIN_PREFIX, "$R").
-define(INSERT_PREFIX, "$I").

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
    DataTopic = data_key(Topic),
    retain_(DataTopic, Value, TimeStamp),
    tree_db_bin:fold_subscriptions(
      fun([<<?DATA_PREFIX>>|Pattern],Pid,_PatternTimeStamp,Acc) ->
	      TopicPattern = tree_db_bin:external_key(Pattern),
	      case must_transmit(Pid, data_key(TopicPattern)) of
		  true ->
		      Pid ! {xbus,TopicPattern,
			     #{ topic=>Topic,
				value=>Value,
				timestamp=>TimeStamp}};
		  false ->
		      ignore
	      end,
	      Acc
      end, true, ?XBUS_SUBS, DataTopic).

%%
%% retain a queue of values
%% $V.a.b.c     the original value
%% $P.a.b.c     contains current retain position
%%
retain_(DataTopic = <<?DATA_PREFIX".",Topic/binary>>, Value, TimeStamp) ->
    Meta = read_meta(Topic),
    case retain_(Meta) of
	0 ->
	    true;
	1 ->
	    case persistent_(Meta) of
		true ->
		    write_p(DataTopic,Value,TimeStamp);
		false ->
		    true
	    end,
	    write(DataTopic,Value,TimeStamp),
	    inc_retain(self(),DataTopic);
	R when R > 0 ->
	    TopicC = insert_key(Topic),
	    I = tree_db_bin:update_counter(?XBUS_RETAIN, TopicC, 1),
	    TopicI = retain_key(Topic, I rem R),
	    case persistent_(Meta) of
		true ->
		    write_p(TopicC, I),
		    write_p(TopicI,Value,TimeStamp),
		    write_p(DataTopic,Value,TimeStamp);
		false ->
		    true
	    end,
	    write(TopicI,Value,TimeStamp),
	    write(DataTopic,Value,TimeStamp),
	    inc_retain(self(),DataTopic)
    end.

%%
%% publish/retain meta informatin about a topic
%% FIXME: code must be added to handle update
%%        of retain value. The easiest whould be
%%        to delete all data and restart, but that
%%        would be boring for persistent data...
%%
-spec pub_meta(Topic::key(), Data::[{atom(),term()}]) -> boolean().

pub_meta(Topic, MetaData) when is_binary(Topic) ->
    pub_meta_(Topic, MetaData);
pub_meta(Topic, MetaData) when is_list(Topic) ->
    pub_meta_(list_to_binary(Topic), MetaData).

pub_meta_(Topic, MetaData) when is_binary(Topic) ->
    MetaTopic = meta_key(Topic),
    TimeStamp = ?timestamp(),
    case retain_(MetaData) of
	0 ->
	    ok;
	_ ->
	    TopicC = insert_key(Topic),
	    write(TopicC, -1, TimeStamp),  %% initialize
	    case persistent_(MetaData) of
		true ->
		    write_p(TopicC, -1, TimeStamp),
		    write_p(MetaTopic, MetaData, TimeStamp);
		false ->
		    true
	    end
    end,
    write(MetaTopic,MetaData,TimeStamp),
    tree_db_bin:fold_subscriptions(
      fun([<<?META_PREFIX>>|TopicPattern],Pid,_PatternTimeStamp,Acc) ->
	      XTopicPattern = tree_db_bin:external_key(TopicPattern),
	      Pid ! {xbus_meta,XTopicPattern,
		     #{ topic => Topic,
			value=>MetaData,
			timestamp=>TimeStamp}},
	      Acc
      end, true, ?XBUS_SUBS, MetaTopic),
    true.


%% subscribe to a topic, Topic may contain components with * or ?
%% also match any retained values
-spec sub(TopicPattern::pattern_key()) -> boolean().
sub(TopicPattern) when is_binary(TopicPattern) ->
    sub_data_(TopicPattern,true);
sub(TopicPattern) when is_list(TopicPattern) ->
    sub_data_(list_to_binary(TopicPattern),true).

-spec sub(TopicPattern::pattern_key(),Retained::boolean()) -> boolean().
sub(TopicPattern,Retained) when is_binary(TopicPattern), is_boolean(Retained) ->
    sub_data_(TopicPattern,Retained);
sub(TopicPattern,Retained) when is_list(TopicPattern), is_boolean(Retained) ->
    sub_data_(list_to_binary(TopicPattern),Retained).

sub_data_(TopicPattern, Retained) ->
    sub_(data_key(TopicPattern), Retained).

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
    DataPattern = data_key(TopicPattern),
    ets:insert(?XBUS_ACK, {{DataPattern,self()},0,0}),
    sub_(DataPattern, Retained).

sub_(TopicPattern,Retained) ->
    retained_(self(),TopicPattern,Retained),
    true = tree_db_bin:subscribe(?XBUS_SUBS, TopicPattern, self()),
    gen_server:cast(?XBUS_SRV, {monitor,TopicPattern, self()}),
    true.

%% Filter retained values with TopicPattern to get the current values
retained_(Pid,TopicPattern,true) ->
    case may_transmit(Pid,TopicPattern) of
	true ->
	    send_retained(Pid,TopicPattern);
	false ->
	    true
    end;
retained_(_Pid,_TopicPattern,false) ->
    true.


%%
%% subscribe to meta information
%%
-spec sub_meta(TopicPattern::pattern_key()) -> boolean().

sub_meta(TopicPattern) when is_binary(TopicPattern) ->
    sub_(meta_key(TopicPattern), true);
sub_meta(TopicPattern) when is_list(TopicPattern) ->
    sub_(meta_key(list_to_binary(TopicPattern)), true).


send_retained(Pid,TopicPattern) ->
    tree_db_bin:fold_matching_ts(
      ?XBUS_RETAIN, TopicPattern,
      fun({[<<?DATA_PREFIX>>|Topic],Value,TimeStamp},Acc) ->
	      <<?DATA_PREFIX".",Pattern/binary>> = TopicPattern,
	      case must_transmit(Pid, TopicPattern) of
		  true ->
		      Pid ! {xbus,Pattern,
			     #{ topic=>tree_db_bin:external_key(Topic),
				value=>Value,
				timestamp=>TimeStamp}};
		  false ->
		      ignore
	      end,
	      Acc;
	 ({[<<?META_PREFIX>>|Topic],Value,TimeStamp},_Acc) ->
	      <<?META_PREFIX".",Pattern/binary>> = TopicPattern,
	      Pid ! {xbus_meta,Pattern,
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
    unsub_data_(TopicPattern);
unsub(TopicPattern) when is_list(TopicPattern) ->
    unsub_data_(list_to_binary(TopicPattern)).

unsub_data_(TopicPattern) ->
    unsub_(data_key(TopicPattern)).

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
    unsub_(meta_key(TopicPattern));
unsub_meta(TopicPattern) when is_list(TopicPattern) ->
    unsub_(meta_key(list_to_binary(TopicPattern))).


%% acknowledge topic and send retained values if needed
ack(TopicPattern) when is_binary(TopicPattern) ->
    ack_(self(),data_key(TopicPattern));
ack(TopicPattern) when is_list(TopicPattern) ->
    ack_(self(),data_key(list_to_binary(TopicPattern))).

ack(Pid,TopicPattern)  when is_pid(Pid), is_binary(TopicPattern) ->
    ack_(Pid,data_key(TopicPattern));
ack(Pid,TopicPattern)  when is_pid(Pid), is_list(TopicPattern) ->
    ack_(Pid,data_key(list_to_binary(TopicPattern))).

%% acknowledge transmit,
ack_(Pid, DataPattern) ->
    try ets:update_counter(?XBUS_ACK,{DataPattern,Pid},
			   [{2,0},{3,0},{2,0,0,0},{3,0,0,0}]) of
	[N,T,_,_] when N > T; N > 1, N >= T -> %% 
	    send_retained(Pid,DataPattern);
	[_,_,_,_] ->
	    ok
    catch
	_:_ -> ok
    end.

%% number of retained data values since last transmit
inc_retain(Pid, TopicPattern) ->
    catch ets:update_counter(?XBUS_ACK, {TopicPattern,Pid},{2,1}).

must_transmit(Pid, TopicPattern) ->
    try ets:update_counter(?XBUS_ACK,{TopicPattern,Pid},{3,1}) of
	1 -> true;  %% was = 0
	_ -> false
    catch
	_:_ -> true
    end.

may_transmit(Pid, TopicPattern) ->
    case ets:lookup(?XBUS_ACK, {TopicPattern,Pid}) of
	[] -> true;
	[{_,_R,0}] -> true;
	[_] -> false
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
	{[<<?DATA_PREFIX>>|K],V,T} <- 
	    dets:lookup(?XBUS_RETAIN, 
			tree_db_bin:internal_key(data_key(Topic)))].

%%
%% Read prefixed value
%%
-spec prefix_read_(Prefix::key(),Topic::key()) ->
			  [{key(),term(),timestamp()}].

prefix_read_(Prefix0,Topic0) ->
    Prefix = iolist_to_binary(Prefix0),
    Topic  = iolist_to_binary(Topic0),
    [{tree_db_bin:external_key(K),V,T} ||
	{[_|K],V,T} <- 
	    tree_db_bin:lookup_ts(?XBUS_RETAIN,join_key(Prefix,Topic))].

%%
%% Read a retained value
%%
read_retained_(Topic0, Pos) when is_integer(Pos) ->
    Topic = join_key(iolist_to_binary(Topic0),Pos),
    prefix_read_(<<?RETAIN_PREFIX>>, Topic).
    
%%
%% Read the n latest elements
%%
read_n(Topic, N) when is_binary(Topic) ->
    read_n_(Topic, N);
read_n(Topic, N) when is_list(Topic) ->
    read_n_(list_to_binary(Topic), N).

read_n_(Topic, N) ->
    case prefix_read_(?INSERT_PREFIX,Topic) of
	[] -> [];
	[{_,-1,_}] -> [];
	[{_,Pos,_}] ->
	    Meta = read_meta(Topic),
	    case retain_(Meta) of
		0 ->
		    [];
		R ->
		    read_n__(Topic,Pos,R,min(R,N))
	    end
    end.

read_n__(_Topic,_Pos,_R,0) ->
    [];
read_n__(Topic,Pos,R,I) ->
    case read_retained_(Topic,(Pos+R) rem R) of
	[] -> [];
	[Val] -> [Val|read_n__(Topic,Pos-1,R,I-1)]
    end.

-spec read(Topic::key()) -> [{atom(),term(),timestamp()}].

%% read data value
read(Topic) ->
    prefix_read_(<<?DATA_PREFIX>>,Topic).

-spec read_meta(Topic::key()) -> [{atom(),term()}].
read_meta(Topic) when is_list(Topic) ->
    read_meta(list_to_binary(Topic));
read_meta(Topic) ->
    case prefix_read_(?META_PREFIX,Topic) of
	[] -> [];
	[{_,Meta,_Ts}] -> Meta
    end.

persistent_(Meta) ->
    meta_value_(persistent, Meta, false).

retain_(Meta) ->
    meta_value_(retain, Meta, 1).

meta_value_(Key, Meta, Default) when is_list(Meta) ->
    proplists:get_value(Key, Meta, Default);
meta_value_(Key, Meta, Default) when is_map(Meta) ->
    maps:get(Key, Meta, Default).

%% Get all topcs
topics() ->
    topics("*").

topics(TopicPattern) ->
    BinPattern = iolist_to_binary(TopicPattern),
    tree_db_bin:fold_matching(
      ?XBUS_RETAIN, meta_key(BinPattern),
      fun({[<<?META_PREFIX>>|Topic],Meta},Acc) ->
	      [{tree_db_bin:external_key(Topic),Meta}|Acc]
      end, []).

%% show local topics
show_topics() -> 
    show_topics("*").
    
show_topics(TopicPattern) ->
    lists:foreach(
      fun({Topic,Meta}) ->
	      io:format("~p  meta=~p\n", [Topic,Meta])
      end, topics(TopicPattern)).

%% Get a list of retained values
retained() ->
    retained("*").

retained(TopicPattern) ->
    %% fixme: use foldmatch in order to avoid building long topcs lists
    lists:foldl(
      fun({Topic,Meta},Acc) ->
	      case prefix_read_(?DATA_PREFIX,Topic) of
		  [] ->
		      Acc;
		  [{_,Value,Ts}] ->
		      [{Topic,Value,[{timestamp,Ts}|Meta]}|Acc]
	      end
      end, [], topics(TopicPattern)).

%% Show retained data that has meta data, fixme show all data

show_retained() ->
    show_retained("*").

show_retained(TopicPattern) ->
    %% fixme: make retained a fold/map function!
    lists:foreach(
      fun({Topic,Value,Info}) ->
	      Unit = meta_value_(unit,Info,""),
	      io:format("~p  ~w~s\n", [Topic,Value,Unit])
      end, retained(TopicPattern)).

%%
%% Timestamp used in read_n ... may have format:
%% {{Year,Mon,Day},{Hour,Min,Sec}}
%% {{Year,Mon,Day},{Hour,Min,Sec},MicroSec}
%% absolute MicroSeconds since 1970
%% relative -MicroSeconds since now
%%

cvt_timestamp(TimeStamp={_Date,_Time}) ->
    S = calendar:datetime_to_gregorian_seconds(TimeStamp),
    (S-?UNIX_SECONDS)*1000000;
cvt_timestamp({Date,Time,Us}) ->
    S = calendar:datetime_to_gregorian_seconds({Date,Time}),
    (S-?UNIX_SECONDS)*1000000+Us;
cvt_timestamp(TimeStamp) when is_integer(TimeStamp), TimeStamp>=0 ->
    TimeStamp;
cvt_timestamp(TimeStamp) when is_integer(TimeStamp), TimeStamp<0 ->
    timestamp() + TimeStamp.

data_key(Topic = <<"$",_binary>>) ->
    erlang:error({key_already_prefixed,Topic});
data_key(Topic) ->
    <<?DATA_PREFIX".", Topic/binary>>.

meta_key(Topic = <<"$",_binary>>) ->
    erlang:error({key_already_prefixed,Topic});
meta_key(Topic) ->
    <<?META_PREFIX".", Topic/binary>>.

insert_key(Topic = <<"$",_binary>>) ->
    erlang:error({key_already_prefixed,Topic});
insert_key(Topic) ->
    <<?INSERT_PREFIX".", Topic/binary>>.

retain_key(Topic,I) when is_binary(Topic), is_integer(I), I>=0 ->
    <<?RETAIN_PREFIX,$.,Topic/binary,$.,(integer_to_binary(I))/binary>>.


join_key(A, B) when is_binary(A), is_binary(B) ->
    <<A/binary,$.,B/binary>>;
join_key(A, B) when is_binary(A), is_integer(B) ->
    <<A/binary,$.,(integer_to_binary(B))/binary>>.


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
    case prefix_read_(?INSERT_PREFIX, Topic) of
	[] -> ok;
	[{_,Pos,_Ts}] ->
	    Meta = read_meta(Topic),
	    case retain_(Meta) of
		0 ->
		    ok;
		1 ->
		    show_value(Topic);
		R ->
		    show_values_(Topic, Pos rem R, R, R)
	    end
    end.

show_values_(_Topic, _Pos, _R, 0) ->
    ok;
show_values_(Topic, Pos, R, I) ->
    case read_retained_(Topic,(Pos+R) rem R) of
	[{_,Value,Ts}] ->
	    print_value(Topic,Value,Ts,I),
	    show_values_(Topic,Pos-1,R,I-1);
	[] ->
	    ok
    end.

show_value(Topic) ->
    [{_,Value,Ts}] = prefix_read_(?DATA_PREFIX,Topic),
    print_value(Topic,Value,Ts).

print_value(Topic,Value,Ts) ->
    {{Y,M,D},{TH,TM,TS},Micro} = datetime_us_from_timestamp(Ts),
    io:format("~s = ~w [" ?DATE_FORMAT "]\n",
	      [Topic, Value,
	       Y,M,D,TH,TM,TS,Micro]).

show_value(Topic, I) ->
    [{_,Value,Ts}] = read_retained_(Topic,I),
    print_value(Topic,Value,Ts,I).

print_value(Topic,Value,Ts,I) ->
    {{Y,M,D},{TH,TM,TS},Micro} = datetime_us_from_timestamp(Ts),
    io:format("~s.~w = ~w [" ?DATE_FORMAT "]\n",
	      [Topic, I, Value,
	       Y,M,D,TH,TM,TS,Micro]).
