%% -*- erlang -*-
Nonterminals
funs fun selector select timeframe aliases alias int_or_time mb fune tag pit
glob_metric part_or_name calculatable fun_arg fun_args gmb bucket
mfrom var metric where where_part as_part as_clause maybe_shifted
math math1 math2 number number2 number3  maybe_scoped_dvar
maybe_group_by grouping metric_or_all limit limit_direction
op op_re events event_condition event_logic event_path event_value
mdata mdata_elements mdata_element named.

%% hist  calculatables.

Terminals '(' ')' ',' '.' '*' '/' '=' ':' '+' '-' '[' ']'
'==' '>=' '=<' '>' '<' '~=' '{' '}'
part  integer kw_bucket kw_select kw_last kw_as kw_from date
kw_between kw_and kw_or kw_ago kw_now time float name
kw_after kw_before kw_for kw_where kw_alias pvar dvar kw_shift
kw_by kw_not op_ne kw_group kw_using kw_all kw_top kw_bottom
kw_events kw_metadata.

%% caggr aggr derivate  float name
%% kw_after kw_before kw_for histogram percentile avg hfun mm kw_where
%% confidence.

%%%===================================================================
%%% Root statement
%%%===================================================================
Rootsymbol select.


select -> kw_select funs timeframe
              : {select, '$2', [], '$3', undefined}.
select -> kw_select funs kw_alias aliases timeframe
              : {select, '$2', '$4', '$5', undefined}.
select -> kw_select funs timeframe limit
              : {select, '$2', [], '$3', '$4'}.
select -> kw_select funs kw_alias aliases timeframe limit
              : {select, '$2', '$4', '$5', '$6'}.


limit -> limit_direction integer kw_by fun : {'$1', unwrap('$2'), '$4'}.

limit_direction -> kw_top    : top.
limit_direction -> kw_bottom : bottom.

%%%===================================================================
%%% SELECT part
%%%===================================================================

%% List of functions in the select part of the statement
funs -> fune : ['$1'].
funs -> fune ',' funs : ['$1'] ++ '$3'.

%% Element in the function list, either a calculatable or a calculatable
%% with a name
fune -> math : '$1'.
fune -> events  : '$1'.
fune -> math named : named('$2', '$1').
fune -> events named : named('$2', '$1').

named -> kw_as as_clause kw_metadata mdata: {'$2', '$4'}.
named -> kw_as as_clause : {'$2', []}.
named -> kw_metadata mdata: {undefined, '$2'}.


mdata -> '{' mdata_elements '}' : '$2'.

mdata_elements -> mdata_element : ['$1'].
mdata_elements -> mdata_element ',' mdata_elements : ['$1' | '$3'].

mdata_element -> part_or_name ':' as_part : {'$1', '$3'}.
mdata_element -> part_or_name ':' number : {'$1', '$3'}.

events -> kw_events kw_from part_or_name :
              #{op => events,
                return => events,
                args =>
                    #{bucket => '$3',
                      filter => []}}.

events -> kw_events kw_from part_or_name kw_where event_logic :
              #{op => events,
                return => events,
                args =>
                    #{bucket => '$3',
                      filter => flatten('$5')}}.


event_logic -> event_condition              : '$1'.
event_logic -> event_logic kw_and event_condition : {'and', '$1', '$3'}.
event_logic -> event_logic kw_or event_condition  : {'or', '$1', '$3'}.


op  -> '==' : '=='.
op  -> '>=' : '>='.
op  -> '=<' : '=<'.
op  -> '>' : '>'.
op  -> '<' : '<'.
op_re -> '~=' : '~='.

event_condition -> kw_not event_condition   : {'not', '$2'}.
event_condition -> event_path op event_value      : {'$2', '$1', '$3'}.
event_condition -> event_path op_ne event_value   : {'not', [{'==', '$1', '$3'}]}.
event_condition -> event_path op_re part_or_name  : {'~=', '$1', '$3'}.
event_condition -> '(' event_logic ')'      : '$2'.

event_value -> integer : unwrap('$1').
event_value -> float   : unwrap('$1').
event_value -> part_or_name  : '$1'.

event_path -> part_or_name : ['$1'].
event_path -> event_path '.' part_or_name : '$1' ++ ['$3'].
event_path -> event_path '[' integer ']': '$1' ++ [unwrap('$3')].


as_part -> part_or_name          : '$1'.
as_part -> maybe_scoped_dvar     : {dvar, '$1'}.
as_part -> pvar                  : '$1'.


maybe_scoped_dvar -> dvar ':' part_or_name : {unwrap('$1'), '$3'}.
maybe_scoped_dvar -> dvar                  : {<<>>, unwrap('$1')}.



as_clause -> as_part               : ['$1'].
as_clause -> as_part '.' as_clause : ['$1'] ++ '$3'.

math -> math1 : '$1'.


math1 -> math1 '-' math1: #{op => fcall,
                            args => #{name => <<"diff">>,
                                      inputs => ['$1', '$3']}}.
math1 -> math1 '+' math1 : #{op => fcall,
                             args => #{name => <<"sum">>,
                                       inputs => ['$1', '$3']}}.
math1 -> math1 '-' number: #{op => fcall,
                             args => #{name => <<"sub">>,
                                       inputs => ['$1', '$3']}}.
math1 -> math1 '+' number: #{op => fcall,
                             args => #{name => <<"add">>,
                                       inputs => ['$1', '$3']}}.

math1 -> math2 : '$1'.

math2 -> math2 '*' math2 : #{op => fcall,
                             args => #{name => <<"product">>,
                                       inputs => ['$1', '$3']}}.
math2 -> math2 '/' math2 : #{op => fcall,
                             args => #{name => <<"quotient">>,
                                       inputs => ['$1', '$3']}}.
%% math2 -> math2 '*' number : #{op => fcall,
%%                               args => #{name => <<"multiply">>,
%%                                         inputs => ['$1', '$3']}}.
%% math2 -> math2 '/' number : #{op => fcall,
%%                              args => #{name => <<"divide">>,
%%                                        inputs => ['$1', '$3']}}.
math2 -> '(' math1 ')' : '$2'.
math2 -> calculatable : '$1'.

%% Something that can be calculated:
%% * a function that can be resolved
calculatable -> fun : '$1'.
%% * a variable that can be looked up
calculatable -> var : '$1'.
%% * a selector that can be retrived
calculatable -> maybe_shifted : '$1'.
%% * a mathematical expression

%% calculatables -> calculatable : ['$1'].
%% calculatables -> calculatable ',' calculatables : ['$1'] ++ '$3'.

%% hist -> histogram '(' integer ',' integer ',' calculatable ',' int_or_time ')'
%%             : {histogram, unwrap('$3'), unwrap('$5'), '$7', '$9'}.


fun_arg -> int_or_time : '$1'.
fun_arg -> math        : '$1'.
fun_arg -> float       : unwrap('$1').


number -> number '+' number: '$1' + '$3'.
number -> number '-' number: '$1' - '$3'.
number -> number2 : '$1'.

number2 -> number2 '*' number2: '$1' * '$3'.
number2 -> number2 '/' number2: '$1' / '$3'.
number2 -> number3 : '$1'.

number3 -> integer : unwrap('$1').
number3 -> float   : unwrap('$1').
number3 -> '(' number ')' : '$2'.

fun_args -> fun_arg : ['$1'].
fun_args -> fun_arg ',' fun_args : ['$1'] ++ '$3'.

fun -> part_or_name '(' ')' : #{op => fcall,
                                args => #{name => '$1',
                                          inputs => []}}.
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

%% A variable.
var -> part_or_name : #{op => var, args => ['$1']}.

%% A selector, either a combination of <metric> BUCKET <bucket> or a mget aggregate.

maybe_shifted -> selector kw_shift kw_by int_or_time :
                     #{op        => timeshift,
                       args      => ['$4', '$1']}.

maybe_shifted -> selector : '$1'.

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
selector -> maybe_group_by : '$1'.

%% A bucket and metric combination used as a solution
mb -> metric kw_bucket part_or_name : ['$3', '$1'].

gmb -> glob_metric kw_bucket bucket : ['$3', '$1'].

maybe_group_by -> mfrom kw_group kw_by grouping kw_using part_or_name :
                      #{
                         op => group_by,
                         args => ['$1', '$4', '$6'],
                         return => metric
                       }.

maybe_group_by -> mfrom : '$1'.

grouping -> maybe_scoped_dvar : ['$1'].
grouping -> maybe_scoped_dvar ',' grouping : ['$1'] ++ '$3'.

mfrom -> metric_or_all kw_from bucket : #{
                                 op => lookup,
                                 args => ['$3', '$1'],
                                 return => metric
                                }.

mfrom -> metric_or_all kw_from bucket kw_where where :#{
                                          op => lookup,
                                          args => ['$3', '$1', '$5'],
                                          return => metric
                                         }.

metric_or_all -> kw_all : undefined.
metric_or_all -> metric : '$1'.

tag -> part_or_name                  : {tag, <<>>, '$1'}.
tag -> part_or_name ':' part_or_name : {tag, '$1', '$3'}.

where_part -> tag '=' part_or_name    : {'=', '$1', '$3'}.
where_part -> tag op_ne part_or_name  : {'!=', '$1', '$3'}.
where_part -> tag kw_not part_or_name : {'!=', '$1', '$3'}.
where_part -> tag                     : {'=', '$1', <<>>}.
where_part -> '(' where ')'           : '$2'.

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
alias -> maybe_shifted kw_as part_or_name : {alias, '$3', '$1'}.

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
      return => time
     }.

named({N, M}, Q) ->
    #{
      op => named,
      args => [N, M, Q],
      return => undefined
    }.

flatten({'and', A, B}) ->
    lists:flatten([flatten(A), flatten(B)]);
flatten({'or', A, B}) ->
    [{'or', flatten(A), flatten(B)}];
flatten({'not', A}) ->
    [{'not', flatten(A)}];
flatten(O) ->
    lists:flatten([O]).
