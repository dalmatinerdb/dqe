-module(dql_unparse).
-export([unparse/1, unparse_metric/1]).

unparse(L) when is_list(L) ->
    Ps = [unparse(Q) || Q <- L],
    Unparsed = combine(Ps),
    Unparsed;

unparse(#{op := dummy}) ->
    <<>>;

unparse(#{op := group_by,
          args := [From, Groupings, #{args := #{name := Function}}]}) ->
    FromB = unparse(From),
    Dvars = [unparse_name({dvar, G}) || G <- Groupings],
    GroupingsB = combine(Dvars),
    <<FromB/binary, " GROUP BY ", GroupingsB/binary, " USING ",
      Function/binary>>;

unparse(#{op   := fcall,
          args := #{name      := Name,
                    inputs    := Args}}) ->
    Qs = unparse(Args),
    <<Name/binary, "(", Qs/binary, ")">>;

unparse(#{op   := events,
          args := #{bucket := Bucket,
                    filter := []}}) ->
    <<"EVENTS FROM ", Bucket/binary>>;
unparse(#{op   := events,
          args := #{bucket := Bucket,
                    filter := Filter}}) ->
    FilterS = unparse_filter(Filter),
    <<"EVENTS FROM ", Bucket/binary, " WHERE ", FilterS/binary>>;

unparse(#{op   := combine,
          args := #{name      := Name,
                    inputs    := Args}}) ->
    Qs = unparse(Args),
    <<Name/binary, "(", Qs/binary, ")">>;

unparse(#{op := named, args := [N, M, Q]}) when is_binary(N) ->
    Qs = unparse(Q),
    Ms = unparse_metadata(M),
    <<Qs/binary, " AS '", N/binary, "'", Ms/binary>>;

unparse(#{op := named, args := [L, M, Q]}) when is_list(L) ->
    N = unparse_named(L),
    Qs = unparse(Q),
    Ms = unparse_metadata(M),
    <<Qs/binary, " AS ", N/binary, Ms/binary>>;

unparse(#{op := time, args := [N, U]}) ->
    Us = atom_to_binary(U, utf8),
    <<(integer_to_binary(N))/binary, " ", Us/binary>>;


unparse(#{ op := get, args := [B, M] }) ->
    <<(unparse_metric(M))/binary, " BUCKET '", B/binary, "'">>;

unparse(#{ op := sget, args := [B, M] }) ->
    <<(unparse_metric(M))/binary, " BUCKET '", B/binary, "'">>;

unparse({select, Q, [], T, L}) ->
    <<"SELECT ", (unparse(Q))/binary, " ",
      (unparse(T))/binary, (unparse_limit(L))/binary>>;

unparse({select, Q, A, T, L}) ->
    <<"SELECT ", (unparse(Q))/binary, " ALIAS ", (unparse(A))/binary, " ",
      (unparse(T))/binary, (unparse_limit(L))/binary>>;

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
unparse(N) when is_float(N) ->
    <<(float_to_binary(N))/binary>>;

unparse(#{op := lookup, args := [B, undefined]}) ->
    <<"ALL FROM '", B/binary, "'">>;
unparse(#{op := lookup, args := [B, undefined, Where]}) ->
    <<"ALL FROM '", B/binary,
      "' WHERE ", (unparse_where(Where))/binary>>;
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

combine(L) ->
    combine(L, <<", ">>).

combine(L, C) ->
    combine(L, C, <<>>).

combine([], _C, Acc) ->
    Acc;
combine([E | R], C, <<>>) ->
    combine(R, C, E);
combine([<<>> | R], C, Acc) ->
    combine(R, C, Acc);
combine([E | R], C, Acc) ->
    combine(R, C, <<Acc/binary, C/binary, E/binary>>).

unparse_limit(undefined) ->
    <<>>;
unparse_limit({top, N, Fun}) ->
    Fs = unparse(Fun),
    <<" TOP ", (integer_to_binary(N))/binary, " BY ", Fs/binary>>;
unparse_limit({bottom, N, Fun}) ->
    Fs = unparse(Fun),
    <<" BOTTOM ", (integer_to_binary(N))/binary, " BY ", Fs/binary>>.

unparse_filter([E]) ->
    unparse_filter(E);

unparse_filter(L) when is_list(L) ->
    L1 = [unparse_filter(E) || E <- L],
    S = combine(L1, <<" AND ">>),
    <<"(", S/binary, ")">>;

unparse_filter({'or', L, R}) ->
    LS = unparse_filter(L),
    RS = unparse_filter(R),
    <<"(", LS/binary, " OR ", RS/binary, ")">>;

unparse_filter({Op, Path, Val}) ->
    OpS = atom_to_binary(Op, utf8),
    ValS = unparse_val(Val),
    PathS = unparse_path(Path, <<>>),
    <<"(",
      PathS/binary,
      " ", OpS/binary, " ",
      ValS/binary,
      ")">>.

unparse_val(V) when is_binary(V) ->
    <<"'", V/binary, "'">>;
unparse_val(V) when is_integer(V) ->
    integer_to_binary(V);
unparse_val(V) when is_float(V) ->
float_to_binary(V).

unparse_path([], Acc) ->
    Acc;
unparse_path([B | R], <<>>) when is_binary(B) ->
    unparse_path(R, <<"'", B/binary, "'">>);
unparse_path([B | R], Acc) when is_binary(B) ->
    unparse_path(R, <<Acc/binary, ".'", B/binary, "'">>);
unparse_path([I | R], Acc) when is_integer(I) ->
    unparse_path(R, <<Acc/binary, "[", (integer_to_binary(I))/binary, "]">>).


unparse_metadata([]) ->
    <<>>;
unparse_metadata(L) ->
    Es = [unparse_metadata(K, V) || {K, V} <- L],
    Eb = combine(Es),
    <<" METADATA {", Eb/binary, "}">>.

unparse_metadata(K, V) ->
    Vs = unparse_kdata_value(V),
    <<"'", K/binary, "': ", Vs/binary>>.

unparse_kdata_value(I) when is_integer(I) ->
    integer_to_binary(I);
unparse_kdata_value(F) when is_float(F) ->
    float_to_binary(F);
unparse_kdata_value(O) ->
    unparse_name(O).
