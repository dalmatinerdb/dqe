%% -*- erlang -*-
Nonterminals
funs fun selector select timeframe aliases alias int_or_time mb fune tag pit
glob_metric part_or_name calculatable fun_arg fun_args gmb bucket
infix mfrom var metric where where_part as_part as_clause perhaps_shifted.

%% hist  calculatables.

Terminals '(' ')' ',' '.' '*' '/' '=' ':'
part  integer kw_bucket kw_select kw_last kw_as kw_from date
kw_between kw_and kw_or kw_ago kw_now time float name
kw_after kw_before kw_for kw_where kw_alias pvar dvar kw_shift
kw_by.

%% caggr aggr derivate  float name
%% kw_after kw_before kw_for histogram percentile avg hfun mm kw_where
%% confidence.


%%%===================================================================
%%% Root statement
%%%===================================================================
Rootsymbol select.


select -> kw_select funs timeframe : {select, '$2', [], '$3'}.
select -> kw_select funs kw_alias aliases timeframe : {select, '$2', '$4', '$5'}.

%%%===================================================================
%%% SELECT part
%%%===================================================================

%% List of functions in the select part of the statement
funs -> perhaps_shifted : ['$1'].
funs -> perhaps_shifted ',' funs : ['$1'] ++ '$3'.

perhaps_shifted -> fune kw_shift kw_by int_or_time : #{op        => timeshift,
                                                       args      => ['$4', '$1']}.
perhaps_shifted -> fune : '$1'.

%% Element in the function list, either a calculatable or a calculatable
%% with a name
fune -> calculatable kw_as as_clause : named('$3', '$1').
fune -> calculatable : '$1'.

as_part -> part_or_name          : '$1'.
as_part -> dvar ':' part_or_name : {dvar, {unwrap('$1'), '$3'}}.
as_part -> dvar                  : {dvar, {<<>>, unwrap('$1')}}.
as_part -> pvar                  : '$1'.

as_clause -> as_part               : ['$1'].
as_clause -> as_part '.' as_clause : ['$1'] ++ '$3'.

%% Something that can be calculated:
%% * a function that can be resolved
calculatable -> fun : '$1'.
%% * a variable that can be looked up
calculatable -> var : '$1'.
%% * a selector that can be retrived
calculatable -> selector : '$1'.
%% * a mathematical expression
calculatable -> infix : '$1'.

%% calculatables -> calculatable : ['$1'].
%% calculatables -> calculatable ',' calculatables : ['$1'] ++ '$3'.

%% hist -> histogram '(' integer ',' integer ',' calculatable ',' int_or_time ')'
%%             : {histogram, unwrap('$3'), unwrap('$5'), '$7', '$9'}.

fun_arg -> int_or_time : '$1'.
fun_arg -> calculatable : '$1'.
%%fun_arg -> integer : '$1'.
fun_arg -> float : '$1'.

fun_args -> fun_arg : ['$1'].
fun_args -> fun_arg ',' fun_args : ['$1'] ++ '$3'.

fun -> part_or_name '(' fun_args ')' : #{op => fcall,
                                         args => #{name => '$1',
                                                   inputs => '$3'}}.

%% %% Histogram based aggregation functiosn
%% fun -> mm         '(' hist          ')' : {hfun, unwrap('$1'), '$3'}.
%% fun -> hfun       '(' hist          ')' : {hfun, unwrap('$1'), '$3'}.
%% fun -> avg        '(' hist          ')' : {hfun, avg, '$3'}.
%% fun -> percentile '(' hist          ',' float ')' : {hfun, percentile, '$3', unwrap('$5')}.

%% %% A aggregation function
%% fun -> derivate   '(' calculatable  ')' : {aggr, derivate, '$3'}.
%% fun -> confidence '(' calculatable  ')' : {aggr, confidence, '$3'}.
%% fun -> mm         '(' calculatable  ',' int_or_time ')' : {f, unwrap('$1'), ['$3', '$5']}.
%% fun -> aggr       '(' calculatable  ',' int_or_time ')' : {f, unwrap('$1'), ['$3', '$5']}.
%% fun -> avg        '(' calculatables ')' : {combine, unwrap('$1'), '$3'}.
%% fun -> avg        '(' calculatable  ',' int_or_time ')' : {aggr, unwrap('$1'), '$3', '$5'}.
%% fun -> caggr      '(' calculatables ')' : {combine, unwrap('$1'), '$3'}.
%% fun -> caggr      '(' calculatable  ',' int_or_time ')' : {aggr, unwrap('$1'), '$3', '$5'}.
%% fun -> math       '(' calculatable  ',' integer ')' : {math, unwrap('$1'), '$3', unwrap('$5')}.

infix -> calculatable '/' integer : {math, divide, '$1', unwrap('$3')}.
infix -> calculatable '*' integer : {math, multiply, '$1', unwrap('$3')}.

%% A variable.
var -> part_or_name : #{op => var, args => ['$1']}.

%% A selector, either a combination of <metric> BUCKET <bucket> or a mget aggregate.
selector -> mb : #{
              op        => get,
              args      => '$1',
              signature => [integer, integer, integer, metric, bucket],
              return    => metric
             }.
selector -> gmb : #{
              op        => sget,
              args      => '$1',
              signature => [integer, integer, integer, glob, bucket],
              return    => metric
             }.
selector -> mfrom : #{
              op => lookup,
              args => '$1',
              return => metric
             }.

%% A bucket and metric combination used as a solution
mb -> metric kw_bucket part_or_name : ['$3', '$1'].

gmb -> glob_metric kw_bucket bucket : ['$3', '$1'].

mfrom -> metric kw_from bucket : ['$3', '$1'].
mfrom -> metric kw_from bucket kw_where where : ['$3', '$1', '$5'].


tag -> part_or_name                  : {tag, <<>>, '$1'}.
tag -> part_or_name ':' part_or_name : {tag, '$1', '$3'}.

where_part -> tag '=' part_or_name : {'=', '$1', '$3'}.
where_part -> tag                  : {'=', '$1', <<>>}.
where_part -> '(' where ')'        : '$2'.

where -> where_part              : '$1'.
where -> where kw_and where_part : {'and', '$1', '$3'}.
where -> where kw_or where_part  : {'or', '$1', '$3'}.

%%%===================================================================
%%% From section, aliased selectors
%%%===================================================================

%% List of aliases after the AS statement
aliases -> alias : ['$1'].
aliases -> alias ',' aliases  : ['$1'] ++ '$3'.

%% A single alias in the AS statement
alias -> selector kw_as part_or_name : {alias, '$3', '$1'}.

%%%===================================================================
%%% TIEM related symbols
%%%===================================================================

%% A timeframe for the select statment
timeframe -> kw_last    int_or_time                    : #{op => last,    args => ['$2']}.
timeframe -> kw_between pit         kw_and pit         : #{op => between, args => ['$2', '$4']}.
timeframe -> kw_after   pit         kw_for int_or_time : #{op => 'after', args => ['$2', '$4']}.
timeframe -> kw_before  pit         kw_for int_or_time : #{op => before,  args => ['$2', '$4']}.


%% A point in time either a integer, a relative time with a AGO statement or the
%% NOW keyword.
pit          -> int_or_time kw_ago : #{op => ago, args => ['$1']}.
pit          -> integer : unwrap('$1').
pit          -> date : {time, qdate:to_unixtime(unwrap('$1')) * 1000, ms}.
pit          -> kw_now : now.


%% A relative time either given as absolute integer or relative.
int_or_time  -> integer time : time(unwrap('$1'), unwrap('$2')).
int_or_time  -> integer : unwrap('$1').

%%%===================================================================
%%% Helper symbols
%%%===================================================================

%% A naming, either wrapped in 's or not if the syntax is supported, reserved
%% words always have to be wrapped in 's
part_or_name -> part : unwrap('$1').
part_or_name -> name : unwrap('$1').


%% A metric, one or more parts seperated by '.'
metric -> part_or_name : ['$1'].
metric -> part_or_name '.' metric: ['$1' | '$3'].

%% A metric including a glob
glob_metric -> '*' : ['*'].
glob_metric -> '*' '.' metric : ['*' | '$3'].
glob_metric -> '*' '.' glob_metric : ['*' | '$3'].
glob_metric -> part_or_name '.' glob_metric : ['$1' | '$3'].

bucket -> part_or_name : '$1'.

%%%===================================================================
%%% Erlang code.
%%%===================================================================

Erlang code.
-ignore_xref([format_error/1, parse_and_scan/1, return_error/2]).

unwrap({_,_,V}) -> V;
unwrap({_,V}) -> V.

time(T, U) ->
    #{
      op => time,
      args => [T, U],
      signature => [integer, time_unit],
      return => integer
     }.

named(N, Q) ->
    #{
      op => named,
      args => [N, Q],
      return => undefined
    }.
