-module(dql_unparse).
-export([unparse/1, unparse_metric/1]).

unparse(L) when is_list(L) ->
    Ps = [unparse(Q) || Q <- L],
    Unparsed = combine(Ps, <<>>),
    Unparsed;

unparse(#{op   := fcall,
          args := #{name      := Name,
                    inputs    := Args}}) ->
    Qs = unparse(Args),
    <<Name/binary, "(", Qs/binary, ")">>;

unparse(#{op   := combine,
          args := #{name      := Name,
                    inputs    := Args}}) ->
    Qs = unparse(Args),
    <<Name/binary, "(", Qs/binary, ")">>;

unparse(#{op := named, args := [N, Q]}) when is_binary(N) ->
    Qs = unparse(Q),
    <<Qs/binary, " AS '", N/binary, "'">>;

unparse(#{op := named, args := [L, Q]}) when is_list(L) ->
    N = unparse_named(L),
    Qs = unparse(Q),
    <<Qs/binary, " AS ", N/binary>>;

unparse(#{op := time, args := [N, U]}) ->
    Us = atom_to_binary(U, utf8),
    <<(integer_to_binary(N))/binary, " ", Us/binary>>;


unparse(#{ op := get, args := [B, M] }) ->
    <<(unparse_metric(M))/binary, " BUCKET '", B/binary, "'">>;

unparse(#{ op := sget, args := [B, M] }) ->
    <<(unparse_metric(M))/binary, " BUCKET '", B/binary, "'">>;

unparse({select, Q, [], T}) ->
    <<"SELECT ", (unparse(Q))/binary, " ",
      (unparse(T))/binary>>;

unparse({select, Q, A, T}) ->
    <<"SELECT ", (unparse(Q))/binary, " ALIAS ", (unparse(A))/binary, " ",
      (unparse(T))/binary>>;

unparse(#{op := timeshift, args := [T, Q]}) ->
    <<(unparse(Q))/binary, " SHIFT BY ", (unparse(T))/binary>>;

unparse(#{op := last, args := [Q]}) ->
    <<"LAST ", (unparse(Q))/binary>>;
unparse(#{op := between, args := [A, B]}) ->
    <<"BETWEEN ", (unparse(A))/binary, " AND ", (unparse(B))/binary>>;
unparse(#{op := 'after', args := [A, B]}) ->
    <<"AFTER ", (unparse(A))/binary, " FOR ", (unparse(B))/binary>>;
unparse(#{op := before, args := [A, B]}) ->
    <<"BEFORE ", (unparse(A))/binary, " FOR ", (unparse(B))/binary>>;

unparse(#{op := ago, args := [T]}) ->
    <<(unparse(T))/binary, " AGO">>;

unparse(now) ->
    <<"NOW">>;

unparse(N) when is_integer(N)->
    <<(integer_to_binary(N))/binary>>;

unparse(#{op := lookup, args := [B, M]}) ->
    <<(unparse_metric(M))/binary, " FROM '", B/binary, "'">>;
unparse(#{op := lookup, args := [B, M, Where]}) ->
    <<(unparse_metric(M))/binary, " FROM '", B/binary,
      "' WHERE ", (unparse_where(Where))/binary>>.

unparse_metric(Ms) ->
    <<".", Result/binary>> = unparse_metric(Ms, <<>>),
    Result.
unparse_metric(['*' | R], Acc) ->
    unparse_metric(R, <<Acc/binary, ".*">>);
unparse_metric([Metric | R], Acc) ->
    unparse_metric(R, <<Acc/binary, ".'", Metric/binary, "'">>);
unparse_metric([], Acc) ->
    Acc.

unparse_named(Ms) ->
    Ms1 = [unparse_name(E) || E <- Ms],
    <<".", Result/binary>> = unparse_named(Ms1, <<>>),
    Result.
unparse_named([Named | R], Acc) ->
    unparse_named(R, <<Acc/binary, ".", Named/binary, "">>);
unparse_named([], Acc) ->
    Acc.

unparse_name(B) when is_binary(B) ->
    <<"'", B/binary, "'">>;
unparse_name({pvar, I}) ->
    <<"$", (integer_to_binary(I))/binary>>;
unparse_name({dvar, {<<>>, K}}) ->
    <<"$'", K/binary, "'">>;
unparse_name({dvar, {Ns, K}}) ->
    <<"$'", Ns/binary, "':'", K/binary, "'">>.

unparse_tag({tag, <<>>, K}) ->
    <<"'", K/binary, "'">>;
unparse_tag({tag, N, K}) ->
    <<"'", N/binary, "':'", K/binary, "'">>.
unparse_where({'=', T, V}) ->
    <<(unparse_tag(T))/binary, " = '", V/binary, "'">>;
unparse_where({'!=', T, V}) ->
    <<(unparse_tag(T))/binary, " != '", V/binary, "'">>;
unparse_where({'or', Clause1, Clause2}) ->
    P1 = unparse_where(Clause1),
    P2 = unparse_where(Clause2),
    <<P1/binary, " OR (", P2/binary, ")">>;
unparse_where({'and', Clause1, Clause2}) ->
    P1 = unparse_where(Clause1),
    P2 = unparse_where(Clause2),
    <<P1/binary, " AND (", P2/binary, ")">>.

combine([], Acc) ->
    Acc;
combine([E | R], <<>>) ->
    combine(R, E);
combine([E | R], Acc) ->
    combine(R, <<Acc/binary, ", ", E/binary>>).
