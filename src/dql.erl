-module(dql).
-export([prepare/1, parse/1, unparse/1, glob_match/2, flatten/1, unparse_metric/1]).


-type bm() :: {binary(), [binary()]}.

-type gbm() :: {binary(), [binary() | '*']}.

-type time() :: {time, pos_integer(), ms | s | m | h | d | w} | pos_integer().

-type resolution() :: time().

-type relative_time() :: time() |
                         now |
                         {'after' | before, pos_integer(), time()} |
                         {ago, time()}.


-type range() :: {between, relative_time(), relative_time()} |
                 {last, time()}.

-type comb_fun() :: avg | min | max | sum.
-type aggr_fun1() :: derivate.
-type aggr_fun2() :: avg | sum | min | max.

-type get_stmt() ::
        {get, bm()} |
        {sget, gbm()}.

-type aggr_stmt() ::
        {aggr, aggr_fun1(), statement()} |
        {aggr, aggr_fun2(), statement(), time()}.

-type cmb_stmt() ::
        {combine, comb_fun(), [statement()]}.

-type statement() ::
        get_stmt() |
        aggr_stmt() |
        cmb_stmt().

-type flat_aggr_fun() ::
        {aggr, aggr_fun2(), time()} |
        {aggr, aggr_fun1()}.

-type flat_terminal() ::
        {combine, comb_fun(), [flat_stmt()]}.

-type flat_stmt() ::
        flat_terminal() |
        {calc, [flat_aggr_fun()], flat_terminal() | get_stmt()}.

-type parser_error() ::
        {error, binary()}.

-type alias() :: term().

-spec parse(string() | binary()) ->
                   parser_error() |
                   {ok, {select, [statement()], range(), resolution()}} |
                   {ok, {select, [statement()], [alias()], range(), resolution()}}.


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

flatten({named, N, Child}) ->
    {named, N, flatten(Child, [])};

flatten(Child) ->
    {named, unparse(Child), flatten(Child, [])}.

-spec flatten(statement(), [flat_aggr_fun()]) ->
                     flat_stmt().
flatten({sget, _} = Get, Chain) ->
    {calc, Chain, Get};

flatten({get, _} = Get, Chain) ->
    {calc, Chain, Get};

flatten({combine, Aggr, Children}, []) ->
    Children1 = [flatten(C, []) || C <- Children],
    {combine, Aggr, Children1};

flatten({combine, Aggr, Children}, Chain) ->
    Children1 = [flatten(C, []) || C <- Children],
    {calc, Chain, {combine, Aggr, Children1}};

flatten({math, Fun, Child, Val}, Chain) ->
    flatten(Child, [{math, Fun, Val} | Chain]);

flatten({aggr, Aggr, Child}, Chain) ->
    flatten(Child, [{aggr, Aggr} | Chain]);


flatten({aggr, Aggr, Child, Time}, Chain) ->
    flatten(Child, [{aggr, Aggr, Time} | Chain]).


parser_error(Line, E)  ->
    {error, list_to_binary(io_lib:format("Parser error in line ~p: ~s",
                                         [Line, E]))}.

lexer_error(Line, {illegal, E})  ->
    {error, list_to_binary(io_lib:format("Lexer error in line ~p illegal: ~s",
                                         [Line, E]))};

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
    QQ2 = [flatten(Q) || Q <- QQ1],
    {Start, Count} = compute_se(T1, Rms),
    {QQ2, Start, Count, Rms, AliasesQ, MetricsQ}.

compute_se({between, S, E}, _Rms) when E > S->
    {S, E - S};
compute_se({between, S, E}, _Rms) ->
    {E, S - E};

compute_se({last, N}, Rms) ->
    NowMs = erlang:system_time(milli_seconds),
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

preprocess_qry({sget, BM}, Aliases, Metrics, _Rms) ->
    Metrics1 = case gb_trees:lookup(BM, Metrics) of
                   none ->
                       gb_trees:insert(BM, {sget, 0}, Metrics);
                   {value, {sget, N}} ->
                       gb_trees:update(BM, {sget, N + 1}, Metrics)
               end,
    {{sget, BM}, Aliases, Metrics1};

preprocess_qry({var, V}, Aliases, Metrics, _Rms) ->
    Metrics1 = case gb_trees:lookup(V, Aliases) of
                   none ->
                       Metrics;
                   {value, {sget, BM}} ->
                       case gb_trees:lookup(BM, Metrics) of
                           none ->
                               gb_trees:insert(BM, {sget, 0}, Metrics);
                           {value, {sget, N}} ->
                               gb_trees:update(BM, {sget, N + 1}, Metrics)
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
unparse({sget, {B, M}}) ->
    <<(unparse_metric(M))/binary, " BUCKET '", B/binary, "'">>;
unparse({combine, Fun, L}) ->
    Funs = list_to_binary(atom_to_list(Fun)),
    <<Funs/binary, "(", (unparse(L))/binary, ")">>;

unparse(N) when is_integer(N)->
    <<(integer_to_binary(N))/binary>>;

unparse(F) when is_float(F)->
    <<(float_to_binary(F))/binary>>;

unparse(now) ->
    <<"NOW">>;

unparse({ago, T}) ->
    <<(unparse(T))/binary, " AGO">>;

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
unparse({math, multiply, Q, V}) ->
    Qs = unparse(Q),
    Vs = unparse(V),
    <<Qs/binary, " * ", Vs/binary>>;
unparse({math, divide, Q, V}) ->
    Qs = unparse(Q),
    Vs = unparse(V),
    <<Qs/binary, " / ", Vs/binary>>;
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
    erlang:max(1, N);

apply_times(now, R) ->
    NowMs = erlang:system_time(milli_seconds),
    erlang:max(1, NowMs div R);

apply_times({ago, T}, R) ->
    NowMs = erlang:system_time(milli_seconds),
    erlang:min(1, (NowMs - to_ms(T)) div R);

apply_times(T, R) ->
    erlang:max(1, to_ms(T) div R).

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
