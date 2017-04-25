# xbus - erlang message bus

xbus is yet an other message bus for erlang, it is based on
tree_db, and can subscribe on topic patterns among a limited
set of features.

# API

### xbus:start() -> {ok,[atom()]}

     Start the xbus application

### xbus:sub(Topic::pattern_key()) -> true | {error,Reason::term()}

Subscribe to messages sent by calling pub. For example

    > xbus:sub(<<"xbus.*.message">>).
    > xbus:pub(<<"xbus.a.message">>, "Hello").
    > flush().
    Shell got {xbus,<<"xbus.*.message">>,<<"xbus.a.message">>,"Hello"}

### xbus:unsub(Topic::pattern_key()) -> boolean()

Remove a subscription

### xbus:pub(Topic::key(), Value) -> true

Broadcast a message on the message bus.

    > xbus:pub(<<"xbus.an.example">>, [{value,99},{unit,"%"}]).

### xbus:meta(Topic::key(), Data) -> true

Meta will publish (and retain) meta information under the
meta topic <<"{META}.a.topic">>. It is also possible to
subscribe to <<"{META}.*">> to detect declared channels.

    > xbus:meta(<<"a.topic">>, [{comment,"A comment"},{unit,"mph"}]).

# Topics

Patterns in xbus may contain '*' or '?' matching any number of, and 
one, number of components respectivly. The patterns must replace
a complete component and can not match inside components, so
*a*.b is not a topic pattern but a.*.b is.

# Config

    {xbus,
       [%% declare meta information about initial topics
        {topics,[
           {<<"sensor.1.temperature">>, [{unit,"C"}]},
           {<<"sensor.2.motion">>, [{unit,"boolean"}]}
	]}]}.

    The topics meta information is stored as:
        pub("{META}.<name>", MetaData)
