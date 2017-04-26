-module(dql).

-export([prepare/1, parse/1]).
-ignore_xref([parse/1]).

-export_type([query_part/0, dqe_fun/0, query_stmt/0, get_stmt/0, flat_stmt/0,
              statement/0, named/0, time/0, raw_query/0, limit/0,
              event_getter/0, dqe_fun_writh_return/0, return/0]).

-type time_unit() :: ms | s | m | h | d | w.

-type time() :: #{ op => time,
                   args => [pos_integer() | time_unit()]} |
                #{ op => timeshift } | pos_integer().

-type relative_time() :: time() |
                         now |
                         #{ op => 'after' | before,
                            args => [pos_integer() | time()]} |
                         #{op => ago, ags => [time()]}.


-type range() :: #{ op => between, args => [relative_time()]} |
                 #{ op => last, args => [time()]}.

-type sig_element() :: string | number | metric.

-type event_filter() :: term().
-type return()       :: events | metric | time.
-type event_getter() :: #{
                    return => events,
                    op     => events,
                    times  => list(),
                    args   => #{
                      bucket => binary(),
                      filter => event_filter()
                     }
                   }.
-type dqe_fun() ::
        #{
           op => fcall,
           signature => [sig_element()],
           args => #{}
         }.

-type dqe_fun_writh_return() ::
        #{
           return    => return(),
           op        => fcall,
           signature => [sig_element()],
           args      => #{}
         }.

-type get_stmt() ::
        #{op => get, args => [[binary()] | non_neg_integer()]} |
        #{op => sget, args => [[binary() | '*'] | non_neg_integer()]}.

-type query_part() :: cmb_stmt() | flat_stmt().

-type cmb_stmt() ::
        {combine, dqe_fun(), [statement()] | get_stmt()}.

-type statement() ::
        get_stmt() |
        dqe_fun() |
        cmb_stmt().

-type flat_terminal() ::
        {combine, dqe_fun(), [flat_stmt() | get_stmt()]}.

-type flat_stmt() ::
        {calc, [dqe_fun()], flat_terminal() | get_stmt()}.

-type named() :: #{op        => named,
                   return    => dql:return(),
                   signature => [sig_element()],
                   args      => [binary() | {binary(), term()} |
                                 flat_stmt()]}.

-type query_stmt() :: {named, binary(), [{binary(), binary()}], flat_stmt()}.

-type parser_error() ::
        {error, binary()}.

-type alias() :: term().
-type limit() :: undefined | {top | bottom, pos_integer(), dqe_fun()}.

-type raw_query() :: string() | binary().

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Takes a query, parses it and prepares it for the query
%% engine to execute.
%% @end
%%--------------------------------------------------------------------
-spec prepare(raw_query()) ->
                     {error, term()} |
                     {ok, [query_stmt()], pos_integer(), limit()}.
prepare(S) ->
    case parse(S) of
        {ok, {select, Qs, Aliases, T, Limit}} ->
            dqe_span:log("parsed"),
            dqe_lib:pdebug('parse', "Query parsed: ~s", [S]),
            add_limit(Qs, Aliases, T, Limit);
        E ->
            E
    end.

%%%===================================================================
%%% Parsing steps
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc Add limit functionality
%% @end
%%--------------------------------------------------------------------

add_limit(Qs, Aliases, T, Limit) ->
    case expand_limit(Limit) of
        {ok, L1} ->
            dqe_span:log("limits expanded"),
            case expand_aliases(Qs, Aliases, T) of
                {ok, Parts, Start} ->
                    {ok, Parts, Start, L1};
                E1 ->
                    E1
            end;
        E2 ->
            E2
    end.

expand_limit(undefined) ->
    {ok, undefined};
expand_limit({Direction, Count,
              #{op := fcall, args :=
                    A = #{inputs := Inputs}}}) ->
    Inputs1 = [#{op => dummy, return => metric} |
               Inputs ++ [#{op => dummy, return => time}]],
    F1 = #{op => fcall, args => A#{inputs => Inputs1}},
    case dql_resolver:resolve([F1]) of
        {ok, [F]} ->
            {ok, {Direction, Count, F}};
        E ->
            E
    end.
%%--------------------------------------------------------------------
%% @private
%% @doc Expand aliases
%% @end
%%--------------------------------------------------------------------
expand_aliases(Qs, Aliases, T) ->
    case dql_alias:expand(Qs, Aliases) of
        {ok, Qs1} ->
            dqe_span:log("aliases expanded"),
            resolve_query_functions(Qs1, T);
        E ->
            E
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc Apply types and resolve the functions called in the query
%% to dqe_fun functions.
%% @end
%%--------------------------------------------------------------------
-spec resolve_query_functions([statement()], time()) ->
                     {error, term()} |
                     {ok, [query_stmt()], pos_integer()}.
resolve_query_functions(Qs, T) ->
    case dql_resolver:resolve(Qs) of
        {ok, Qs1} ->
            dqe_span:log("functions resolved"),
            flatten_step(Qs1, T);
        E ->
            E
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc Flattens the tree into a list of flat queries.
%% @end
%%--------------------------------------------------------------------
-spec flatten_step([statement()], time()) ->
                          {ok, [query_stmt()], pos_integer()}.
flatten_step(Qs, T) ->
    Qs1 = dql_flatten:flatten(Qs),
    dqe_lib:pdebug('parse', "Query flattened.", []),
    dqe_span:log("query flattend"),
    expand(Qs1, T).

%%--------------------------------------------------------------------
%% @private
%% @doc Expand lookup and glob.
%% @end
%%--------------------------------------------------------------------
-spec expand([flat_stmt()], time()) ->
                    {'ok',[query_stmt()], pos_integer()}.
expand(Qs, T) ->
    Qs1 = dql_expand:expand(Qs),
    dqe_span:log("query expanded"),
    get_resolution(Qs1, T).

%%--------------------------------------------------------------------
%% @private
%% @doc Fetch the resolution for each sub query.
%% @end
%%--------------------------------------------------------------------
-spec get_resolution([flat_stmt()], time()) ->
                            {error, term()} |
                            {'ok',[query_stmt()],
                             pos_integer()}.
get_resolution(Qs, T) ->
    case dql_resolution:resolve(Qs, T) of
        {error, E} ->
            {error, E};
        {ok, Qs1} ->
            dqe_span:log("resolutions resolved"),
            propagate_resolutions(Qs1, T)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc calculate resolutions for the whole call stack.
%% @end
%%--------------------------------------------------------------------
propagate_resolutions(Qs, T) ->
    Qs1 = dql_resolution:propagate(Qs),
    Start = dql_resolution:start_time(T),
    dqe_span:log("resolutions propagated"),
    dqe_span:tag(start, Start),
    apply_names(Qs1, Start).

apply_names(Qs, Start) ->
    Qs1 = dql_naming:update(Qs),
    dqe_span:log("names applied"),
    {ok, Qs1, Start}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc Run a string through the lexer and parser and return somewhat
%% sensible errors if requried.
%% @end
%%--------------------------------------------------------------------
-spec parse(raw_query()) ->
                   parser_error() |
                   {ok, {select, [statement()], [alias()], range(), limit()}}.

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

lexer_error(Line, {illegal, E})  ->
    {error, list_to_binary(io_lib:format("Lexer error in line ~p illegal: ~s",
                                         [Line, E]))};

lexer_error(Line, E)  ->
    {error, list_to_binary(io_lib:format("Lexer error in line ~p: ~s",
                                         [Line, E]))}.
