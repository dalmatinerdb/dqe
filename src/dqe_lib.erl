-module(dqe_lib).
-include_lib("dproto/include/dproto.hrl").

-export([glob_to_string/1, pdebug/3, debugid/0]).

pdebug(S, M, E) ->
    D =  erlang:system_time() - pstart(),
    MS = round(D / 1000 / 1000),
    lager:debug("<~s> [dqe:~s|~p|~pms] " ++ M, [debugid(), S, self(), MS | E]).

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

glob_to_string(G) ->
    G1 = [case E of
              '*' ->
                  "'*'";
              B when is_binary(B) ->
                  [$', binary_to_list(B), $']
          end || E <- G],
    string:join(G1, ".").
