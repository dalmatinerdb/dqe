%% -*- erlang -*-
Nonterminals
funs fun selector select timeframe aliases alias resolution int_or_time mb fune
var pit metric glob_metric part_or_name calculatable bucket gmb calculatables.

Terminals '(' ')' ',' '.' '*'
part caggr aggr integer kw_bucket kw_select kw_last kw_as kw_from kw_in date
kw_between kw_and kw_ago kw_now derivate time math percentile float name
kw_after kw_before kw_for.


%%%===================================================================
%%% Root statement
%%%===================================================================
Rootsymbol select.


select -> kw_select funs timeframe : {select, '$2', '$3', {time, 1, s}}.
select -> kw_select funs kw_from aliases timeframe : {select, '$2', '$4', '$5', {time, 1, s}}.
select -> kw_select funs timeframe resolution : {select, '$2', '$3', '$4'}.
select -> kw_select funs kw_from aliases timeframe resolution : {select, '$2', '$4', '$5', '$6'}.

%%%===================================================================
%%% SELECT part
%%%===================================================================

%% List of functions in the select part of the statement
funs -> fune : ['$1'].
funs -> fune ',' funs : ['$1'] ++ '$3'.

%% Element in the funciton list, either a calculatable or a calculatable
%% with a name
fune -> calculatable kw_as part_or_name : {named, '$3', '$1'}.
fune -> calculatable : '$1'.

%% Something that can be calculated:
%% * a function that can be resolved
calculatable -> fun : '$1'.
%% * a variable that can be looked up
calculatable -> var : '$1'.
%% * a selector that can be retrived
calculatable -> selector : '$1'.

calculatables -> calculatable : ['$1'].
calculatables -> calculatable ',' calculatables : ['$1'] ++ '$3'.

%% A aggregation function
fun -> derivate '(' calculatable ')' : {aggr, derivate, '$3'}.
fun -> percentile '(' calculatable ',' float ',' int_or_time ')' : {aggr, percentile, '$3', unwrap('$5'), '$7'}.
fun -> aggr '(' calculatable ',' int_or_time ')' : {aggr, unwrap('$1'), '$3', '$5'}.
fun -> caggr '(' calculatables ')' : {combine, unwrap('$1'), '$3'}.
fun -> caggr '(' calculatable ',' int_or_time ')' : {aggr, unwrap('$1'), '$3', '$5'}.
fun -> math '(' calculatable ',' integer ')' : {math, unwrap('$1'), '$3', unwrap('$5')}.

%% A variable.
var -> part_or_name : {var, '$1'}.

%% A selector, either a combination of <metric> BUCKET <bucket> or a mget aggregate.
selector -> mb : {get, '$1'}.
selector -> gmb : {sget, '$1'}.

%% A bucket and metric combination used as a solution
mb -> metric kw_bucket part_or_name : {'$3', '$1'}.

gmb -> glob_metric kw_bucket bucket : {'$3', '$1'}.

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
timeframe -> kw_last    int_or_time                    : {last,    '$2'}.
timeframe -> kw_between pit         kw_and pit         : {between, '$2', '$4'}.
timeframe -> kw_after   pit         kw_for int_or_time : {'after', '$2', '$4'}.
timeframe -> kw_before  pit         kw_for int_or_time : {before,  '$2', '$4'}.


%% A point in time either a integer, a relative time with a AGO statement or the
%% NOW keyword.
pit          -> int_or_time kw_ago : {ago, '$1'}.
pit          -> integer : unwrap('$1').
pit          -> date:  qdate:to_unixtime(unwrap('$1')).
pit          -> kw_now : now.


%% A relative time either given as absolute integer or relative.
int_or_time  -> integer time : {time, unwrap('$1'), unwrap('$2')}.
int_or_time  -> integer : unwrap('$1').


resolution   -> kw_in int_or_time : '$2'.

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
