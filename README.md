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
    Shell got {xbus,<<"xbus.*.message">>,<<"xbus.a.message">>,
                #{timestamp => 1494846399372848,
                  topic => <<"xbus.a.message">>,
                  value => "Hello"}}

Note that the pattern key is always reported as binary.

### xbus:unsub(Topic::pattern_key()) -> boolean()

Remove a subscription

### xbus:pub(Topic::key(), Value::term() [, TimeStamp::integer()]) -> true

Broadcast a message on the message bus.

    > xbus:pub("xbus.an.example", [{value,99},{unit,"%"}]).

An optional time stamp maybe supplied, and should in this case be
xbus:timestamp() or tree_db_bin:timestamp() which is in unix micro seconds.

### xbus:pub_meta(Topic::key(), Data) -> true

pub_meta will publish (and retain) meta information under the
meta topic <<"{META}.a.topic">>. It is also possible to
subscribe to <<"{META}.*">> (use sub_meta) to detect declared channels.

    > xbus:pub_meta("a.topic", [{comment,"A comment"},{unit,"mph"}]).

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
    > Shell got {xbus,<<"{META}.xbus.a.message">>,
                      #{timestamp => 1494848025006097,
                      topic => <<"{META}.xbus.a.message">>,
                      value => [{unit,"mm"}]}}

# Topics

Patterns in xbus may contain * or ? matching any number of, and 
one, number of components respectivly. The patterns must replace
a complete component and can not match inside components, so
*a*.b is not a topic pattern but a.*.b is.

Meta information is stored in the "{META}" subtree.
The meta information for topic "a.b.c" may be
"{META}.a.b.c", [{unit,"m/s"},{retain,100},{persistent,true}].
Special topic is also "a.b.c.#" which is the current position when
retain > 1, the retained values are then stored as "a.b.c.0" ... "a.b.c.N"
and then wrap around as a circular queue. Current values is also stored under
the normal topic, "a.b.c" in this case.

# Config

    {xbus,
       [%% declare meta information about initial topics
        {persistent, false},   %% default is not to store (using dets)
	{file, "xbus_retain"}, %% default filename if persistent is used
        {topics,[
           {<<"sensor.1.temperature">>, [{unit,"C"},{persist,true},{retain,100}]},
           {<<"sensor.2.motion">>, [{unit,"boolean"}]}
	]}]}.

