%%%-------------------------------------------------------------------
%%% @author Tony Rogvall <tony@rogvall.se>
%%% @copyright (C) 2017, Tony Rogvall
%%% @doc
%%%
%%% @end
%%% Created : 27 Nov 2017 by Tony Rogvall <tony@rogvall.se>
%%%-------------------------------------------------------------------
-module(xbus_SUITE).

%% Note: This directive should only be used in test suites.
-compile(export_all).

-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%%  Returns list of tuples to set default properties
%%  for the suite.
%%
%% Function: suite() -> Info
%%
%% Info = [tuple()]
%%   List of key/value pairs.
%%
%% Note: The suite/0 function is only meant to be used to return
%% default data values, not perform any other operations.
%%
%% @spec suite() -> Info
%% @end
%%--------------------------------------------------------------------
suite() ->
    [{timetrap,{minutes,10}}].

%%--------------------------------------------------------------------
%% @doc
%% Initialization before the whole suite
%%
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%% Reason = term()
%%   The reason for skipping the suite.
%%
%% Note: This function is free to add any key/value pairs to the Config
%% variable, but should NOT alter/remove any existing entries.
%%
%% @spec init_per_suite(Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% @end
%%--------------------------------------------------------------------
init_per_suite(Config) ->
    xbus:start(),
    Config.

%%--------------------------------------------------------------------
%% @doc
%% Cleanup after the whole suite
%%
%% Config - [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%%
%% @spec end_per_suite(Config) -> _
%% @end
%%--------------------------------------------------------------------
end_per_suite(_Config) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Initialization before each test case group.
%%
%% GroupName = atom()
%%   Name of the test case group that is about to run.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding configuration data for the group.
%% Reason = term()
%%   The reason for skipping all test cases and subgroups in the group.
%%
%% @spec init_per_group(GroupName, Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% @end
%%--------------------------------------------------------------------
init_per_group(_GroupName, Config) ->
    Config.

%%--------------------------------------------------------------------
%% @doc
%% Cleanup after each test case group.
%%
%% GroupName = atom()
%%   Name of the test case group that is finished.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding configuration data for the group.
%%
%% @spec end_per_group(GroupName, Config0) ->
%%               term() | {save_config,Config1}
%% @end
%%--------------------------------------------------------------------
end_per_group(_GroupName, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Initialization before each test case
%%
%% TestCase - atom()
%%   Name of the test case that is about to be run.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%% Reason = term()
%%   The reason for skipping the test case.
%%
%% Note: This function is free to add any key/value pairs to the Config
%% variable, but should NOT alter/remove any existing entries.
%%
%% @spec init_per_testcase(TestCase, Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% @end
%%--------------------------------------------------------------------
init_per_testcase(pub_no_meta, Config) ->
    Config;
init_per_testcase(pub_no_retain, Config) ->
    xbus:pub_meta("b.c.d", [{retain,0}]),
    xbus:pub_meta("b.c.e", [{retain,0}]),
    xbus:pub_meta("b.c.f", [{retain,0}]),
    Config;
init_per_testcase(pub_retain_2, Config) ->
    xbus:pub_meta("c.d.e", [{retain,2}]),
    xbus:pub_meta("c.d.f", [{retain,2}]),
    xbus:pub_meta("c.d.g", [{retain,2}]),
    Config;
init_per_testcase(sub_no_meta, Config) ->
    Config;
init_per_testcase(sub_no_retain, Config) ->
    xbus:pub_meta("e.f.g", [{retain,0}]),
    xbus:pub_meta("e.f.h", [{retain,0}]),
    xbus:pub_meta("e.f.i", [{retain,0}]),
    Config;
init_per_testcase(sub_retain_2, Config) ->
    xbus:pub_meta("f.g.h", [{retain,2}]),
    xbus:pub_meta("f.g.i", [{retain,2}]),
    xbus:pub_meta("f.g.j", [{retain,2}]),
    Config;

init_per_testcase(sub_meta, Config) ->
    xbus:pub_meta("g.h.i", [{retain,0},{unit,"mm"}]),
    xbus:pub_meta("g.h.k", [{retain,1},{unit,"km/h"}]),
    xbus:pub_meta("g.h.l", [{retain,2},{unit,"liter"}]),
    Config;

init_per_testcase(sub_pattern_retain_2, Config) ->
    xbus:pub_meta("h.x",  [{retain,2}]),
    xbus:pub_meta("h.y",  [{retain,2}]),
    xbus:pub_meta("h.z",  [{retain,2}]),
    Config;

init_per_testcase(sub_ack, Config) ->
    xbus:pub_meta("i.x",  [{retain,0}]),
    xbus:pub_meta("i.y",  [{retain,1}]),
    xbus:pub_meta("i.z",  [{retain,2}]),
    Config;

init_per_testcase(_TestCase, Config) ->
    Config.

%%--------------------------------------------------------------------
%% @doc
%% Cleanup after each test case
%%
%% TestCase - atom()
%%   Name of the test case that is finished.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%%
%% @spec end_per_testcase(TestCase, Config0) ->
%%               term() | {save_config,Config1} | {fail,Reason}
%% @end
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Returns a list of test case group definitions.
%%
%% Group = {GroupName,Properties,GroupsAndTestCases}
%% GroupName = atom()
%%   The name of the group.
%% Properties = [parallel | sequence | Shuffle | {RepeatType,N}]
%%   Group properties that may be combined.
%% GroupsAndTestCases = [Group | {group,GroupName} | TestCase]
%% TestCase = atom()
%%   The name of a test case.
%% Shuffle = shuffle | {shuffle,Seed}
%%   To get cases executed in random order.
%% Seed = {integer(),integer(),integer()}
%% RepeatType = repeat | repeat_until_all_ok | repeat_until_all_fail |
%%              repeat_until_any_ok | repeat_until_any_fail
%%   To get execution of cases repeated.
%% N = integer() | forever
%%
%% @spec: groups() -> [Group]
%% @end
%%--------------------------------------------------------------------
groups() ->
    [].

%%--------------------------------------------------------------------
%% @doc
%%  Returns the list of groups and test cases that
%%  are to be executed.
%%
%% GroupsAndTestCases = [{group,GroupName} | TestCase]
%% GroupName = atom()
%%   Name of a test case group.
%% TestCase = atom()
%%   Name of a test case.
%% Reason = term()
%%   The reason for skipping all groups and test cases.
%%
%% @spec all() -> GroupsAndTestCases | {skip,Reason}
%% @end
%%--------------------------------------------------------------------
all() -> 
    [
     pub_no_meta,
     pub_no_retain,
     pub_retain_2,
     sub_no_meta,
     sub_no_retain,
     sub_retain_2,
     sub_meta,
     sub_pattern_retain_2,
     sub_ack
    ].


%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc 
%%  Test case info function - returns list of tuples to set
%%  properties for the test case.
%%
%% Info = [tuple()]
%%   List of key/value pairs.
%%
%% Note: This function is only meant to be used to return a list of
%% values, not perform any other operations.
%%
%% @spec TestCase() -> Info 
%% @end
%%--------------------------------------------------------------------
pub_no_meta() -> 
    [].

pub_no_retain() -> 
    [].

%%--------------------------------------------------------------------
%% @doc Test case function. (The name of it must be specified in
%%              the all/0 list or in a test case group for the test case
%%              to be executed).
%%
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%% Reason = term()
%%   The reason for skipping the test case.
%% Comment = term()
%%   A comment about the test case that will be printed in the html log.
%%
%% @spec TestCase(Config0) ->
%%           ok | exit() | {skip,Reason} | {comment,Comment} |
%%           {save_config,Config1} | {skip_and_save,Reason,Config1}
%% @end
%%--------------------------------------------------------------------

pub_no_meta(_Config) ->
    xbus:pub("a.b.c", 1),
    xbus:pub("a.b.d", 2),
    xbus:pub("a.b.e", 3),
    ?line [{<<"a.b.c">>,1,_}] = xbus:read("a.b.c"),
    ?line [{<<"a.b.d">>,2,_}] = xbus:read("a.b.d"),
    ?line [{<<"a.b.e">>,3,_}] = xbus:read("a.b.e"),
    ok.

pub_no_retain(_Config) ->
    xbus:pub("b.c.d", 1),
    xbus:pub("b.c.e", 2),
    xbus:pub("b.c.f", 3),
    ?line [] = xbus:read("b.c.d"),
    ?line [] = xbus:read("b.c.e"),
    ?line [] = xbus:read("b.c.f"),
    ok.

pub_retain_2(_Config) ->
    xbus:pub("c.d.e", 10),
    xbus:pub("c.d.f", 20),
    xbus:pub("c.d.f", 21),
    xbus:pub("c.d.g", 30),
    xbus:pub("c.d.g", 31),
    xbus:pub("c.d.g", 32),
    ?line [{_,10,_}] = xbus:read_n("c.d.e", 3),
    ?line [{_,21,_},{_,20,_}] = xbus:read_n("c.d.f", 3),
    ?line [{_,32,_},{_,31,_}] = xbus:read_n("c.d.g", 3),
    ok.

sub_no_meta(_Config) ->
    xbus:sub("d.e.f"),
    xbus:sub("d.e.g"),
    xbus:sub("d.e.h"),
    xbus:pub("d.e.f", 1),
    xbus:pub("d.e.g", 2),
    xbus:pub("d.e.h", 3),
    ?line ok = receive 
		   {xbus,<<"d.e.f">>,#{ topic := <<"d.e.f">>,value := 1}} -> ok
	       after 0 -> timeout
	       end,
    ?line ok = receive 
		   {xbus,<<"d.e.g">>,#{ topic:= <<"d.e.g">>,value:=2}} -> ok
	       after 0 -> timeout
	       end,
    ?line ok = receive
		   {xbus,<<"d.e.h">>,#{ topic:= <<"d.e.h">>,value:=3}} -> ok
	       after 0 -> timeout
	       end,
    ok.

sub_no_retain(_Config) ->
    xbus:sub("e.f.g"),
    xbus:sub("e.f.h"),
    xbus:sub("e.f.i"),
    xbus:pub("e.f.g",1),
    xbus:pub("e.f.h",2),
    xbus:pub("e.f.i",3),
    ?line ok = receive 
		   {xbus,<<"e.f.g">>,#{ topic:= <<"e.f.g">>,value:=1}} -> ok
	       after 0 -> timeout
	       end,
    ?line ok = receive 
		   {xbus,<<"e.f.h">>,#{ topic:= <<"e.f.h">>,value:=2}} -> ok
	       after 0 -> timeout
	       end,
    ?line ok = receive
		   {xbus,<<"e.f.i">>,#{ topic:= <<"e.f.i">>,value:=3}} -> ok
	       after 0 -> timeout
	       end,
    ?line [] = xbus:read("e.f.g"),
    ?line [] = xbus:read("e.f.h"),
    ?line [] = xbus:read("e.f.i"),
    ok.

sub_retain_2(_Config) ->
    xbus:sub("f.g.h"),
    xbus:sub("f.g.i"),
    xbus:sub("f.g.j"),

    xbus:pub("f.g.h", 10),

    xbus:pub("f.g.i", 20),
    xbus:pub("f.g.i", 21),

    xbus:pub("f.g.j", 30),
    xbus:pub("f.g.j", 31),
    xbus:pub("f.g.j", 32),

    ?line ok = receive 
		   {xbus,<<"f.g.h">>,#{ topic:= <<"f.g.h">>,value:=10}} -> ok
	       after 0 -> timeout
	       end,

    ?line ok = receive 
		   {xbus,<<"f.g.i">>,#{ topic:= <<"f.g.i">>,value:=20}} -> ok
	       after 0 -> timeout
	       end,
    ?line ok = receive 
		   {xbus,<<"f.g.i">>,#{ topic:= <<"f.g.i">>,value:=21}} -> ok
	       after 0 -> timeout
	       end,

    ?line ok = receive
		   {xbus,<<"f.g.j">>,#{ topic:= <<"f.g.j">>,value:=30}} -> ok
	       after 0 -> timeout
	       end,
    ?line ok = receive
		   {xbus,<<"f.g.j">>,#{ topic:= <<"f.g.j">>,value:=31}} -> ok
	       after 0 -> timeout
	       end,
    ?line ok = receive
		   {xbus,<<"f.g.j">>,#{ topic:= <<"f.g.j">>,value:=32}} -> ok
	       after 0 -> timeout
	       end,

    ?line [{_,10,_}] = xbus:read_n("f.g.h", 3),
    ?line [{_,21,_},{_,20,_}] = xbus:read_n("f.g.i", 3),
    ?line [{_,32,_},{_,31,_}] = xbus:read_n("f.g.j", 3),
    ok.

sub_meta(_Congig) ->
    xbus:sub_meta("g.h.i"),
    xbus:sub_meta("g.h.k"),
    xbus:sub_meta("g.h.l"),
    xbus:sub_meta("g.h.n"),  %% not published yet
    xbus:sub_meta("g.h.m"),  %% not published yet

    xbus:pub_meta("g.h.n", [{unit,"%"}]),
    xbus:pub_meta("g.h.m", [{unit,"ohm"}]),
    
    ?line ok = receive
		   {xbus_meta,<<"g.h.i">>,
		    #{ topic:= <<"g.h.i">>,
		       value:=[{retain,0},{unit,"mm"}]}} -> ok
	       after 0 -> timeout
	       end,    

    ?line ok = receive
		   {xbus_meta,<<"g.h.k">>,
		    #{ topic:= <<"g.h.k">>,
		       value:=[{retain,1},{unit,"km/h"}]}} -> ok
	       after 0 -> timeout
	       end,

    ?line ok = receive
		   {xbus_meta,<<"g.h.l">>,
		    #{ topic:= <<"g.h.l">>,
		       value:=[{retain,2},{unit,"liter"}]}} -> ok
	       after 0 -> timeout
	       end,    

    ?line ok = receive
		   {xbus_meta,<<"g.h.n">>,
		    #{ topic:= <<"g.h.n">>,
		       value:=[{unit,"%"}]}} -> ok
	       after 0 -> timeout
	       end,    

    ?line ok = receive
		   {xbus_meta,<<"g.h.m">>,
		    #{ topic:= <<"g.h.m">>,
		       value:=[{unit,"ohm"}]}} -> ok
	       after 0 -> timeout
	       end,    
    ok.

%% as sub_retain_2 but with pattern.
sub_pattern_retain_2(_Config) ->
    xbus:sub("h.*"),

    xbus:pub("h.x", 10),

    xbus:pub("h.y", 20),
    xbus:pub("h.y", 21),

    xbus:pub("h.z", 30),
    xbus:pub("h.z", 31),
    xbus:pub("h.z", 32),    
    
    ?line ok = receive 
		   {xbus,<<"h.*">>,#{ topic:= <<"h.x">>,value:=10}} -> ok
	       after 0 -> timeout
	       end,

    ?line ok = receive 
		   {xbus,<<"h.*">>,#{ topic:= <<"h.y">>,value:=20}} -> ok
	       after 0 -> timeout
	       end,
    ?line ok = receive 
		   {xbus,<<"h.*">>,#{ topic:= <<"h.y">>,value:=21}} -> ok
	       after 0 -> timeout
	       end,

    ?line ok = receive
		   {xbus,<<"h.*">>,#{ topic:= <<"h.z">>,value:=30}} -> ok
	       after 0 -> timeout
	       end,
    ?line ok = receive
		   {xbus,<<"h.*">>,#{ topic:= <<"h.z">>,value:=31}} -> ok
	       after 0 -> timeout
	       end,
    ?line ok = receive
		   {xbus,<<"h.*">>,#{ topic:= <<"h.z">>,value:=32}} -> ok
	       after 0 -> timeout
	       end,

    ?line [{_,10,_}] = xbus:read_n("h.x", 3),
    ?line [{_,21,_},{_,20,_}] = xbus:read_n("h.y", 3),
    ?line [{_,32,_},{_,31,_}] = xbus:read_n("h.z", 3),
    ok.


sub_ack(_Config) ->
    xbus:sub_ack("i.x"),
    xbus:sub_ack("i.y"),
    xbus:sub_ack("i.z"),

    %% i.x has retain 0 
    xbus:pub("i.x", 100),
    xbus:pub("i.x", 200),
    xbus:pub("i.x", 300),
    
    ?line ok = receive
		   {xbus,<<"i.x">>,#{ topic := <<"i.x">>,value := 100}} -> ok
	       after 0 -> timeout
	       end,
    ?line ok = receive
		   {xbus,<<"i.x">>,#{ topic := <<"i.x">>,value := 200}} -> error
	       after 0 -> ok
	       end,
    ?line ok = receive
		   {xbus,<<"i.x">>,#{ topic := <<"i.x">>,value := 300}} -> error
	       after 0 -> ok
	       end,

    xbus:ack("i.x"),  %% no messages should be sent, since retain=0

    ?line ok = receive
		   {xbus,<<"i.x">>,#{ topic := <<"i.x">> }} -> error
	       after 0 -> ok
	       end,


    %% i.y has retain 1
    xbus:pub("i.y", 101),
    xbus:pub("i.y", 201),
    xbus:pub("i.y", 301),
    
    ?line ok = receive
		   {xbus,<<"i.y">>,#{ topic := <<"i.y">>,value := 101}} -> ok
	       after 0 -> timeout
	       end,
    ?line ok = receive
		   {xbus,<<"i.x">>,#{ topic := <<"i.x">>,value := 201}} -> error
	       after 0 -> ok
	       end,
    ?line ok = receive
		   {xbus,<<"i.x">>,#{ topic := <<"i.x">>,value := 301}} -> error
	       after 0 -> ok
	       end,

    xbus:pub("i.y", 401),

    xbus:ack("i.y"),  %% 401 should arrive but 201,301 should not

    ?line ok = receive
		   {xbus,<<"i.x">>,#{ topic := <<"i.x">>,value := 401}} -> ok
	       after 0 -> ok
	       end,
    ?line ok = receive
		   {xbus,<<"i.x">>,#{ topic := <<"i.x">> }} -> error
	       after 0 -> ok
	       end,
    %% check that value do not surce with i.z
    
    xbus:pub("i.z", 1001),
    ?line ok = receive
		   {xbus,<<"i.z">>,#{ topic := <<"i.z">>,value := 1001}} -> ok
	       after 0 -> timeout
	       end,

    xbus:ack("i.z"),  %% nust not send data again!
    ?line ok = receive
		   {xbus,<<"i.z">>,#{ topic := <<"i.z">>}} -> error
	       after 0 -> ok
	       end,

    xbus:pub("i.z", 1002),  %% but now it is ok
    ?line ok = receive
		   {xbus,<<"i.z">>,#{ topic := <<"i.z">>,value := 1002}} -> ok
	       after 0 -> timeout
	       end,

    xbus:ack("i.z"),
    ?line ok = receive
		   {xbus,<<"i.z">>,#{ topic := <<"i.z">>}} -> error
	       after 0 -> ok
	       end,

    ok.
