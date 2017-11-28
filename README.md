# xbus - erlang message bus

xbus is yet an other message bus for erlang, it is based on
tree_db, and can subscribe on topic patterns among a limited
set of features.

# API

### xbus:start() -> {ok,[atom()]}

     Start the xbus application

### xbus:sub(Topic::pattern_key()) -> true | {error,Reason::term()}

Subscribe to messages sent by calling pub. For example

    > xbus:sub("xbus.*.message").
    > xbus:pub("xbus.a.message", "Hello").
    > flush().
    Shell got {xbus,<<"xbus.*.message">>,
                #{timestamp => 1494846399372848,
                  topic => <<"xbus.a.message">>,
                  value => "Hello"}}

Note that the pattern key is always reported as binary.

### xbus:unsub(Topic::pattern_key()) -> boolean()

Remove a subscription. If the same process has several subscriptions
under the same pattern, all are removed.

### xbus:pub(Topic::key(), Value::term() [, TimeStamp::integer()]) -> true

Broadcast a message on the message bus.

    > xbus:pub("a.topic", 99).

An optional time stamp maybe supplied, and should in this case be
xbus:timestamp() or tree_db_bin:timestamp() which is in unix micro seconds.

### xbus:pub_meta(Topic::key(), Data) -> true

pub_meta will publish (and retain) meta information.

    > xbus:pub_meta("a.topic", [{comment,"A comment"},{unit,"%"}]).

The meta {persistent, true} will declare that the topic values
are stored in persistent storage. A sys.config is needed to
open a dets file.
The number of retained values may be declared as {retain,N} when N>=0.
The default is to ratain one value. If no retain is wanted the {retain,0}
is used. retained values may be combined with persistent storage.

### xbus:sub_meta(Topic::pattern_key()) -> true | {error,Reason::term()}

Subscribe to meta declaration sent by calling pub_meta. For example

    > xbus:sub_meta("xbus.*.message").
    > xbus:pub_meta("xbus.a.message", [{unit,"mm"}]).
    > flush().
    > Shell got {xbus_meta,<<"xbus.*.message">>,
                      #{timestamp => 1494848025006097,
                      topic => <<"xbus.a.message">>,
                      value => [{unit,"mm"}]}}

# Topics

Patterns in xbus may contain * or ? matching any number of, and 
one, number of components respectivly. The patterns must replace
a complete component and can not match inside components, so
*a*.b is not a topic pattern but a.*.b is.

# Config

    {xbus,
       [%% declare meta information about initial topics
        {persistent, false},   %% default is not to store (using dets)
        {file, "xbus_retain"}, %% default filename for persistence, if used.
        {topics,[
           {<<"sensor.1.temperature">>, [{unit,"C"},{persistent,true},{retain,100}]},
           {<<"sensor.2.motion">>, [{unit,"boolean"}]}
	]}]}.

