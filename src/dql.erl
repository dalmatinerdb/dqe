-module(dql).
-export([prepare/1, parse/1, execute/1, unparse/1, glob_match/2]).
-ignore_xref([prepare/1, parse/1, execute/1, unparse/1, glob_match/2]).

parse(S) when is_binary(S)->
    parse(binary_to_list(S));
parse(S) ->
    case dql_lexer:string(S) of
        {error,{Line, dql_lexer,E},1} ->
            lexer_error(Line, E);
        {ok, L, _} ->
            case dql_parser:parse(L) of
                {error, {Line, dql_parser, E}} ->
                    parser_error(Line, E);
                R ->
                    R
            end
    end.

parser_error(Line, E)  ->
    {error, list_to_binary(io_lib:format("Parser error in line ~p: ~s",
                                         [Line, E]))}.

lexer_error(Line, E)  ->
    {error, list_to_binary(io_lib:format("Lexer error in line ~p: ~s",
                                         [Line, E]))}.

prepare(S) ->
    case parse(S) of
        {ok, {select, Qx, Aliasesx, Tx, Rx}} ->
            {ok, prepare(Qx, Aliasesx, Tx, Rx)};
        {ok, {select, Qx, Tx, Rx}} ->
            {ok, prepare(Qx, [], Tx, Rx)};
        E ->
            E
    end.

prepare(Qs, Aliases, T, R) ->
    Rms = to_ms(R),
    T1 = apply_times(T, Rms),
    {_AF, AliasesF, MetricsF} =
        lists:foldl(fun({alias, Alias, Resolution}, {QAcc, AAcc, MAcc}) ->
                            {Q1, A1, M1} = preprocess_qry(Resolution, AAcc, MAcc, Rms),
                            {[Q1 | QAcc], gb_trees:enter(Alias, Resolution, A1), M1}
                    end, {[], gb_trees:empty(), gb_trees:empty()}, Aliases),
    {QQ, AliasesQ, MetricsQ} =
        lists:foldl(fun(Q, {QAcc, AAcc, MAcc}) ->
                            {Q1, A1, M1} = preprocess_qry(Q, AAcc, MAcc, Rms),
                            {[Q1 | QAcc] , A1, M1}
                    end, {[], AliasesF, MetricsF}, Qs),
    QQ1 = lists:reverse(QQ),
    {Start, Count} = compute_se(T1, Rms),
    {QQ1, Start, Count, Rms, AliasesQ, MetricsQ}.

compute_se({between, S, E}, _Rms) when E > S->
    {S, E - S};
compute_se({between, S, E}, _Rms) ->
    {E, S - E};

compute_se({last, N}, Rms) ->
    NowMs = erlang:system_time(milli_seconds),
    %%UTC = calendar:now_to_universal_time(Now),
    %%UTCs = calendar:datetime_to_gregorian_seconds(UTC) - 62167219200,
    %%UTCms = (UTCs * 1000) + (NowMs rem 1000),
    RelativeNow = NowMs div Rms,
    {RelativeNow - N, N};

compute_se({before, E, D}, _Rms) ->
    {E - D, D};
compute_se({'after', S, D}, _Rms) ->
    {S, D}.

preprocess_qry({named, N, Q}, Aliases, Metrics, Rms) ->
    {Q1, A1, M1} = preprocess_qry(Q, Aliases, Metrics, Rms),
    {{named, N, Q1}, A1, M1};

preprocess_qry({aggr, AggF, Q}, Aliases, Metrics, Rms) ->
    {Q1, A1, M1} = preprocess_qry(Q, Aliases, Metrics, Rms),
    {{aggr, AggF, Q1}, A1, M1};

preprocess_qry({aggr, AggF, Q, T}, Aliases, Metrics, Rms) ->
    {Q1, A1, M1} = preprocess_qry(Q, Aliases, Metrics, Rms),
    {{aggr, AggF, Q1, T}, A1, M1};

preprocess_qry({aggr, AggF, Q, Arg, T}, Aliases, Metrics, Rms) ->
    {Q1, A1, M1} = preprocess_qry(Q, Aliases, Metrics, Rms),
    {{aggr, AggF, Q1, Arg, T}, A1, M1};

preprocess_qry({math, MathF, Q, V}, Aliases, Metrics, Rms) ->
    {Q1, A1, M1} = preprocess_qry(Q, Aliases, Metrics, Rms),
    {{math, MathF, Q1, V}, A1, M1};


preprocess_qry({get, BM}, Aliases, Metrics, _Rms) ->
    Metrics1 = case gb_trees:lookup(BM, Metrics) of
                   none ->
                       gb_trees:insert(BM, {get, 0}, Metrics);
                   {value, {get, N}} ->
                       gb_trees:update(BM, {get, N + 1}, Metrics)
               end,
    {{get, BM}, Aliases, Metrics1};

preprocess_qry({mget, AggrF, BM}, Aliases, Metrics, _Rms) ->
    Metrics1 = case gb_trees:lookup({AggrF, BM}, Metrics) of
                   none ->
                       gb_trees:insert({AggrF, BM}, {mget, 0}, Metrics);
                   {value, {mget, N}} ->
                       gb_trees:update({AggrF, BM}, {mget, N + 1}, Metrics)
               end,
    {{mget, AggrF, BM}, Aliases, Metrics1};

preprocess_qry({var, V}, Aliases, Metrics, _Rms) ->
    Metrics1 = case gb_trees:lookup(V, Aliases) of
                   none ->
                       Metrics;
                   {value, {mget, AggrF, BM}} ->
                       case gb_trees:lookup({AggrF, BM}, Metrics) of
                           none ->
                               gb_trees:insert({AggrF, BM}, {mget, 0}, Metrics);
                           {value, {mget, N}} ->
                               gb_trees:update({AggrF, BM}, {mget, N + 1}, Metrics)
                       end;
                   {value, {get, BM}} ->
                       case gb_trees:lookup(BM, Metrics) of
                           none ->
                               gb_trees:insert(BM, {get, 1}, Metrics);
                           {value, {get, N}} ->
                               gb_trees:update(BM, {get, N + 1}, Metrics)
                       end
               end,
    {{var, V}, Aliases, Metrics1};

preprocess_qry(Q, A, M, _) ->
    {Q, A, M}.

execute(Qry) ->
    case prepare(Qry) of
        {ok, {Qs, S, C, _Rms, A, M}} ->
            {D, _} = lists:foldl(fun({named, Name, Q}, {RAcc, MAcc}) ->
                                         {{D, R}, M1} = execute(Q, S, C, 1, A, MAcc),
                                         {[{Name, R, D} | RAcc], M1};
                                     (Q, {RAcc, MAcc}) ->
                                         {{D, R}, M1} = execute(Q, S, C, 1, A, MAcc),
                                         {[{unparse(Q), R, D} | RAcc], M1}
                                 end, {[], M}, Qs),
            {ok, D};
        E ->
            E
    end.

execute({named, _, Q}, S, C, Rms, A, M) ->
    execute(Q, S, C, Rms, A, M);

execute({aggr, derivate, Q}, S, C, Rms, A, M) ->
    {{D, Res}, M1} = execute(Q, S, C, Rms, A, M),
    {{mmath_aggr:derivate(D), Res}, M1};

execute({aggr, avg, Q, T}, S, C, Rms, A, M) ->
    {{D, Res}, M1} = execute(Q, S, C, Rms, A, M),
    T1 = apply_times(T, Rms * Res),
    {{mmath_aggr:avg(D, T1), T1}, M1};

execute({aggr, sum, Q, T}, S, C, Rms, A, M) ->
    {{D, Res}, M1} = execute(Q, S, C, Rms, A, M),
    T1 = apply_times(T, Rms * Res),
    {{mmath_aggr:sum(D, T1), T1}, M1};

execute({aggr, max, Q, T}, S, C, Rms, A, M) ->
    {{D, Res}, M1} = execute(Q, S, C, Rms, A, M),
    T1 = apply_times(T, Rms * Res),
    {{mmath_aggr:max(D, T1), T1}, M1};

execute({aggr, empty, Q, T}, S, C, Rms, A, M) ->
    {{D, Res}, M1} = execute(Q, S, C, Rms, A, M),
    T1 = apply_times(T, Rms * Res),
    {{mmath_aggr:empty(D, T1), T1}, M1};

execute({aggr, min, Q, T}, S, C, Rms, A, M) ->
    {{D, Res}, M1} = execute(Q, S, C, Rms, A, M),
    T1 = apply_times(T, Rms * Res),
    {{mmath_aggr:min(D, T1), T1}, M1};

execute({aggr, percentile, Q, P, T}, S, C, Rms, A, M) ->
    {{D, Res}, M1} = execute(Q, S, C, Rms, A, M),
    T1 = apply_times(T, Rms * Res),
    {{mmath_aggr:percentile(D, T1, P), T1}, M1};

execute({math, multiply, Q, V}, S, C, Rms, A, M) ->
    {{D, Res}, M1} = execute(Q, S, C, Rms, A, M),
    {{mmath_aggr:scale(D, V), Res}, M1};

execute({math, divide, Q, V}, S, C, Rms, A, M) ->
    {{D, Res}, M1} = execute(Q, S, C, Rms, A, M),
    {{mmath_aggr:scale(D, 1/V), Res}, M1};



execute({get, BM = {B, M}}, S, C, _Rms, _A, Metrics) ->
    case gb_trees:get(BM, Metrics) of
        {get, N} when N =< 1 ->
            {ok, Res, D} = dalmatiner_connection:get(B, M, S, C),
            {{D, Res}, gb_trees:delete(BM, Metrics)};
        {get, N} ->
            {ok, Res, D} = dalmatiner_connection:get(B, M, S, C),
            {{D, Res}, gb_trees:update(BM, {get, {D, Res}, N - 1}, Metrics)};
        {get, D, N} when N =< 1 ->
            {D, gb_trees:delete(BM, Metrics)};
        {get, D, N} ->
            {D, gb_trees:update(BM, {get, D, N - 1}, Metrics)}
    end;

execute({mget, F, BM = {B, M}}, S, C, _Rms, _A, Metrics) ->
    case gb_trees:get({F, BM}, Metrics) of
        {mget, N} when N =< 1 ->
            {ok, D} = mget(F, B, M, S, C),
            {{D, 1}, gb_trees:delete({F, BM}, Metrics)};
        {mget, N} ->
            {ok, D} = mget(F, B, M, S, C),
            {{D, 1}, gb_trees:update({F, BM}, {mget, {D, 1}, N - 1}, Metrics)};
        {mget, D, N} when N =< 1 ->
            {D, gb_trees:delete(BM, Metrics)};
        {mget, D, N} ->
            {D, gb_trees:update({F, BM}, {mget, D, N - 1}, Metrics)}
    end;

execute({var, V}, S, C, Rms, A, M) ->
    execute(gb_trees:get(V, A), S, C, Rms, A, M).

combine([], Acc) ->
    Acc;
combine([E | R], <<>>) ->
    combine(R, E);
combine([E | R], Acc) ->
    combine(R, <<Acc/binary, ", ", E/binary>>).


unparse_metric(Ms) ->
    <<".", Result/binary>> = unparse_metric(Ms, <<>>),
    Result.
unparse_metric(['*' | R], Acc) ->
    unparse_metric(R, <<Acc/binary, ".*">>);
unparse_metric([Metric | R], Acc) ->
    unparse_metric(R, <<Acc/binary, ".'", Metric/binary, "'">>);
unparse_metric([], Acc) ->
    Acc.

unparse(L) when is_list(L) ->
    Ps = [unparse(Q) || Q <- L],
    Unparsed = combine(Ps, <<>>),
    Unparsed;

unparse({select, Q, A, T, R}) ->
    <<"SELECT ", (unparse(Q))/binary, " FROM ", (unparse(A))/binary, " ",
      (unparse(T))/binary, " IN ", (unparse(R))/binary>>;
unparse({select, Q, T, R}) ->
    <<"SELECT ", (unparse(Q))/binary, " ", (unparse(T))/binary, " IN ",
      (unparse(R))/binary>>;
unparse({last, Q}) ->
    <<"LAST ", (unparse(Q))/binary>>;
unparse({between, A, B}) ->
    <<"BETWEEN ", (unparse(A))/binary, " AND ", (unparse(B))/binary>>;
unparse({'after', A, B}) ->
    <<"AFTER ", (unparse(A))/binary, " FOR ", (unparse(B))/binary>>;
unparse({before, A, B}) ->
    <<"BEFORE ", (unparse(A))/binary, " FOR ", (unparse(B))/binary>>;

unparse({var, V}) ->
    V;

unparse({alias, A, V}) ->
    <<(unparse(V))/binary, " AS '", A/binary, "'">>;
unparse({get, {B, M}}) ->
    <<(unparse_metric(M))/binary, " BUCKET '", B/binary, "'">>;
unparse({mget, Fun, {B, M}}) ->
    Funs = list_to_binary(atom_to_list(Fun)),
    <<Funs/binary, "(", (unparse_metric(M))/binary, " BUCKET '", B/binary, "')">>;
unparse(N) when is_integer(N)->
    <<(integer_to_binary(N))/binary>>;
unparse(F) when is_float(F)->
    <<(float_to_binary(F))/binary>>;
unparse({time, N, ms}) ->
    <<(integer_to_binary(N))/binary, " ms">>;
unparse({time, N, s}) ->
    <<(integer_to_binary(N))/binary, " s">>;
unparse({time, N, m}) ->
    <<(integer_to_binary(N))/binary, " m">>;
unparse({time, N, h}) ->
    <<(integer_to_binary(N))/binary, " h">>;
unparse({time, N, d}) ->
    <<(integer_to_binary(N))/binary, " d">>;
unparse({time, N, w}) ->
    <<(integer_to_binary(N))/binary, " w">>;
unparse({aggr, Fun, Q}) ->
    Funs = list_to_binary(atom_to_list(Fun)),
    Qs = unparse(Q),
    <<Funs/binary, "(", Qs/binary, ")">>;
unparse({maggr, Fun, Q}) ->
    Funs = list_to_binary(atom_to_list(Fun)),
    Qs = unparse(Q),
    <<Funs/binary, "(", Qs/binary, ")">>;
unparse({aggr, Fun, Q, T}) ->
    Funs = list_to_binary(atom_to_list(Fun)),
    Qs = unparse(Q),
    Ts = unparse(T),
    <<Funs/binary, "(", Qs/binary, ", ", Ts/binary, ")">>;
unparse({aggr, Fun, Q, A, T}) ->
    Funs = list_to_binary(atom_to_list(Fun)),
    Qs = unparse(Q),
    As = unparse(A),
    Ts = unparse(T),
    <<Funs/binary, "(", Qs/binary, ", ", As/binary, ", ", Ts/binary, ")">>;
unparse({math, Fun, Q, V}) ->
    Funs = list_to_binary(atom_to_list(Fun)),
    Qs = unparse(Q),
    Vs = unparse(V),
    <<Funs/binary, "(", Qs/binary, ", ", Vs/binary, ")">>.

apply_times({last, L}, R) ->
    {last, apply_times(L, R)};

apply_times({between, S, E}, R) ->
    {between, apply_times(S, R), apply_times(E, R)};

apply_times({'after', S, D}, R) ->
    {'after', apply_times(S, R), apply_times(D, R)};

apply_times({before, E, D}, R) ->
    {before, apply_times(E, R), apply_times(D, R)};

apply_times(N, _) when is_integer(N) ->
    erlang:min(1, N);

apply_times(now, R) ->
    NowMs = erlang:system_time(milli_seconds),
    erlang:min(1, NowMs div R);

apply_times({ago, T}, R) ->
    NowMs = erlang:system_time(milli_seconds),
    erlang:min(1, (NowMs - to_ms(T)) div R);

apply_times(T, R) ->
    erlang:min(1, to_ms(T) div R).

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

mget(sum, Bucket, G, Start, Count) ->
    {ok, Ms} = dalmatiner_connection:list(Bucket),
    Ms1 = glob_match(G, Ms),
    Res =  mget_sum(Bucket, Ms1, Start, Count),
    {ok, Res};

mget(avg, Bucket, G, Start, Count) ->
    {ok, Ms} = dalmatiner_connection:list(Bucket),
    Ms1 = glob_match(G, Ms),
    Res = mget_avg(Bucket, Ms1, Start, Count),
    {ok, Res}.


mget_avg(Bucket, Ms, A, B) ->
    mmath_aggr:scale(mget_sum(Bucket, Ms, A, B), 1/length(Ms)).

mget_sum(Bucket, Ms, A, B) ->
    mmath_comb:sum(mget_sum(Bucket, Ms, A, B, [])).

mget_sum(Bucket, [MA, MB, MC, MD | R], S, C, Acc) ->
    Self = self(),
    RefA = make_ref(),
    spawn(fun() ->
                  {ok, V} = dalmatiner_connection:get(Bucket, MA, S, C),
                  Self ! {RefA, V}
          end),
    RefB = make_ref(),
    spawn(fun() ->
                  {ok, V} = dalmatiner_connection:get(Bucket, MB, S, C),
                  Self ! {RefB, V}
          end),
    Va = receive
             {RefA, VA} ->
                 VA
         after
             1000 ->
                 throw(timeout)
         end,
    Vb = receive
             {RefB, VB} ->
                 VB
         after
             1000 ->
                 throw(timeout)
         end,

    RefC = make_ref(),
    spawn(fun() ->
                  {ok, V} = dalmatiner_connection:get(Bucket, MC, S, C),
                  Self ! {RefC, V}
          end),
    RefD = make_ref(),
    spawn(fun() ->
                  {ok, V} = dalmatiner_connection:get(Bucket, MD, S, C),
                  Self ! {RefD, V}
          end),
    Vab = mmath_comb:sum([Va, Vb]),
    Vc = receive
             {RefC, VC} ->
                 VC
         after
             1000 ->
                 throw(timeout)
         end,
    Vabc = mmath_comb:sum([Vab, Vc]),
    Vd = receive
             {RefD, VD} ->
                 VD
         after
             1000 ->
                 throw(timeout)
         end,
    mget_sum(Bucket, R, S, C, [mmath_comb:sum([Vabc, Vd]) | Acc]);

mget_sum(Bucket, [MA, MB | R], S, C, Acc) ->
    RefA = make_ref(),
    RefB = make_ref(),
    Self = self(),
    spawn(fun() ->
                  {ok, V} = dalmatiner_connection:get(Bucket, MA, S, C),
                  Self ! {RefA, V}
          end),
    spawn(fun() ->
                  {ok, V} = dalmatiner_connection:get(Bucket, MB, S, C),
                  Self ! {RefB, V}
          end),
    Va = receive
             {RefA, VA} ->
                 VA
         after
             1000 ->
                 throw(timeout)
         end,
    Vb = receive
             {RefB, VB} ->
                 VB
         after
             1000 ->
                 throw(timeout)
         end,
    mget_sum(Bucket, R, S, C, [mmath_comb:sum([Va, Vb]) | Acc]);

mget_sum(_, [], _, _, Acc) ->
    Acc;
mget_sum(Bucket, [MA], S, C, Acc) ->
    {ok, V} = dalmatiner_connection:get(Bucket, MA, S, C),
    [V | Acc].

glob_match(G, Ms) ->
    GE = re:split(G, "\\*"),
    F = fun(M) ->
                rmatch(GE, M)
        end,
    lists:filter(F, Ms).


rmatch([<<>>, <<$., Ar1/binary>> | Ar], B) ->
    rmatch([Ar1 | Ar], skip_one(B));
rmatch([<<>> | Ar], B) ->
    rmatch(Ar, skip_one(B));
rmatch([<<$., Ar1/binary>> | Ar], B) ->
    rmatch([Ar1 | Ar], skip_one(B));
rmatch([A | Ar], B) ->
    case binary:longest_common_prefix([A, B]) of
        L when L == byte_size(A) ->
            <<_:L/binary, Br/binary>> = B,
            rmatch(Ar, Br);
        _ ->
            false
    end;
rmatch([], <<>>) ->
    true;
rmatch(_A, _B) ->
    false.

skip_one(<<$., R/binary>>) ->
    R;
skip_one(<<>>) ->
    <<>>;
skip_one(<<_, R/binary>>) ->
    skip_one(R).
