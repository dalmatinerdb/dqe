-module(dqe_span).

-export([start/2, stop/0, tag/2, log/1]).


start(_, undefined) ->
    ok;

start(Name, TraceID) ->
    ottersp:start(Name, TraceID).

stop() ->
    ottersp:finish().

tag(Key, Value) ->
    ottersp:tag(Key, Value, "dqe").

log(Text) ->
    ottersp:log(Text, "dqe").
