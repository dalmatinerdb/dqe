-module(dqe_lib).
-include_lib("dproto/include/dproto.hrl").

-export([glob_to_string/1, pstart/0, pdebug/3, debugid/0, get_log/0]).

pdebug(S, M, E) ->
    D =  erlang:system_time(nano_seconds) - pstart(),
    MS = erlang:convert_time_unit(D, nano_seconds, milli_seconds),
    Log = io_lib:format("[~s|~pms] " ++ M ++ "~n", [S, MS | E]),
    add_log(Log),
    ID = debugid(),
    LogStr = "<~s> [dqe:~s|~pms] " ++ M,
    lager:debug(LogStr, [ID, S, MS | E]).

pstart() ->
    case get(start) of
        N when is_integer(N) ->
            N;
        _ ->
            N = erlang:system_time(),
            put(start, N),
            N
    end.

debugid() ->
    case get(debug_id) of
        ID when is_binary(ID) ->
            ID;
        _ ->
            ID = base64:encode(crypto:strong_rand_bytes(6)),
            put(debug_id, ID),
            ID
    end.

add_log(Log) ->
    case get(debug_log) of
        List when is_list(List) ->
            put(debug_log, [Log | List]);
        _ ->
            put(debug_log, [Log])
    end.

get_log() ->
    case get(debug_log) of
        List when is_list(List) ->
            lists:reverse(List);
        _ ->
            []
    end.

glob_to_string(G) ->
    G1 = [case E of
              '*' ->
                  "'*'";
              B when is_binary(B) ->
                  [$', binary_to_list(B), $']
          end || E <- G],
    string:join(G1, ".").
