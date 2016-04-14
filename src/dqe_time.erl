-module(dqe_time).

-export([apply_times/2, to_ms/1]).


apply_times(T, R)  when is_integer(T) ->
    erlang:max(1, T div R);

apply_times(T, R) ->
    erlang:max(1, to_ms(T) div R).

to_ms(T) when is_integer(T) ->
    T;
to_ms({time, N, ms}) ->
    N;
to_ms({time, N, s}) ->
    N*1000;
to_ms({time, N, m}) ->
    N*1000*60;
to_ms({time, N, h}) ->
    N*1000*60*60;
to_ms({time, N, d}) ->
    N*1000*60*60*24;
to_ms({time, N, w}) ->
    N*1000*60*60*24*7.
