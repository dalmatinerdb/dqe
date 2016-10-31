%% -*- erlang -*-
% DQL lexer - based on the example from https://github.com/relops/leex_yecc_example/blob/master/src/selector_lexer.xrl

Definitions.

Sign     = [\-+]?
Digit    = [0-9]
Float    = {Digit}+\.{Digit}+([eE][-+]?[0-9]+)?

PART     = '(\\.|[^\'\\])+'
DATE     = "(\\.|[^\"\\])+"
S        = [A-Za-z][A-Za-z0-9_@-]*
PVAR     = [$][0-9]+
QVAR     = [$]'(\\.|[^\'\\])+'
%'% damn you syntax highlighter
VAR      = [$][A-Za-z0-9_@-]+
WS       = ([\000-\s]|%.*)
TIME     = (ms|s|m|h|d|w)
SELECT   = [Ss][Ee][Ll][Ee][Cc][Tt]
BUCKET   = [Bb][Uu][Cc][Kk][Ee][Tt]
LAST     = [Ll][Aa][Ss][Tt]
AS       = [Aa][Ss]
FROM     = [Ff][Rr][Oo][Mm]
ALIAS    = [Aa][Ll][Ii][Aa][Ss]
LIKE     = [Ll][Ii][Kk][Ee]
BETWEEN  = [Bb][Ee][Tt][Ww][Ee][Ee][Nn]
NOW      = [Nn][Oo][Ww]
AGO      = [Aa][Gg][Oo]
AND      = [Aa][Nn][Dd]
OR       = [Oo][Rr]
AFTER    = [Aa][Ff][Tt][Ee][Rr]
BEFORE   = [Bb][Ee][Ff][Oo][Rr][Ee]
FOR      = [Ff][Oo][Rr]
WHERE    = [Ww][Hh][Ee][Rr][Ee]
SHIFT    = [Ss][Hh][Ii][Ff][Tt]
GROUP    = [Gg][Rr][Oo][Uu][Pp]
BY       = [Bb][Yy]
USING    = [Uu][Ss][Ii][Nn][Gn]
NOT      = [Nn][Oo][Tt]
ALL      = [Aa][Ll][Ll]
EVENTS   = [Ee][Vv][Ee][Nn][Tt][Ss]
TOP      = [Tt][Oo][Pp]
BOTTOM   = [Bb][Oo][Tt][Tt][Oo][Mm]
METADATA = [Mm][Ee][Tt][Aa][Dd][Aa][Tt][Aa]
OP_NE    = (!=)
OP       = (~=|==|>=|=<|>|<)

Rules.
{SELECT}    :   {token, {kw_select,     TokenLine}}.
{BUCKET}    :   {token, {kw_bucket,     TokenLine}}.
{LAST}      :   {token, {kw_last,       TokenLine}}.
{AS}        :   {token, {kw_as,         TokenLine}}.
{FROM}      :   {token, {kw_from,       TokenLine}}.
{ALIAS}     :   {token, {kw_alias,      TokenLine}}.
{BETWEEN}   :   {token, {kw_between,    TokenLine}}.
{METADATA}  :   {token, {kw_metadata,   TokenLine}}.
{LIKE}      :   {token, {kw_like,       TokenLine}}.
{NOW}       :   {token, {kw_now,        TokenLine}}.
{AGO}       :   {token, {kw_ago,        TokenLine}}.
{AND}       :   {token, {kw_and,        TokenLine}}.
{OR}        :   {token, {kw_or,         TokenLine}}.
{AFTER}     :   {token, {kw_after,      TokenLine}}.
{BEFORE}    :   {token, {kw_before,     TokenLine}}.
{FOR}       :   {token, {kw_for,        TokenLine}}.
{WHERE}     :   {token, {kw_where,      TokenLine}}.
{SHIFT}     :   {token, {kw_shift,      TokenLine}}.
{GROUP}     :   {token, {kw_group,      TokenLine}}.
{USING}     :   {token, {kw_using,      TokenLine}}.
{BY}        :   {token, {kw_by,         TokenLine}}.
{NOT}       :   {token, {kw_not,        TokenLine}}.
{ALL}       :   {token, {kw_all,        TokenLine}}.
{EVENTS}    :   {token, {kw_events,     TokenLine}}.
{TOP}       :   {token, {kw_top,        TokenLine}}.
{BOTTOM}    :   {token, {kw_bottom,     TokenLine}}.

{OP_NE}     :   {token, {op_ne,         TokenLine}}.

{TIME}      :   {token, {time,          TokenLine, a(TokenChars)}}.

{Sign}{Digit}+ : {token, {integer,      TokenLine, i(TokenChars)}}.
{Sign}{Float}  : {token, {float,        TokenLine, f(TokenChars)}}.

{PART}       :   S = strip(TokenChars,   TokenLen),
                 {token, {part,          TokenLine, b(S)}}.
{DATE}       :   S = strip(TokenChars,   TokenLen),
                 {token, {date,          TokenLine, S}}.
{S}          :   {token, {name,          TokenLine, b(TokenChars)}}.
({OP}|[(),.*/=:[\]{}+-]) :   {token, {a(TokenChars), TokenLine}}.
{PVAR}       :   {token, {pvar,          i(strip_var(TokenChars))}}.
{QVAR}       :   {token, {dvar,          b(strip_var(TokenChars, TokenLen))}}.
{VAR}        :   {token, {dvar,          b(strip_var(TokenChars))}}.
{WS}+        :   skip_token.

Erlang code.

-include_lib("eunit/include/eunit.hrl").
-ignore_xref([format_error/1, string/2, token/2, token/3, tokens/2, tokens/3]).
-dialyzer({nowarn_function, yyrev/2}).

strip(TokenChars,TokenLen) ->
    S = lists:sublist(TokenChars, 2, TokenLen - 2),
    re:replace(S, "\\\\(.)", "\\1", [global, {return, list}]).
strip_var([$$ | R]) -> R.
strip_var([$$ | R], Len) -> strip(R, Len - 1).

a(L) -> list_to_atom(L).
i(L) -> list_to_integer(L).
f(L) -> list_to_float(L).
b(L) -> list_to_binary(L).

part_test_() ->
    [?_assertEqual({ok, [{part, 1, <<"base">>}], 1},
                   dql_lexer:string("'base'")),
     ?_assertEqual({ok, [{part, 1, <<"'quoted'">>}], 1},
                   dql_lexer:string("'\\'quoted\\''")),
     ?_assertEqual({ok, [{part, 1, <<$\\, "at_beginning">>}], 1},
                   dql_lexer:string("'\\\\at_beginning'")),
     ?_assertEqual({ok, [{part, 1, <<"at_end", $\\>>}], 1},
                   dql_lexer:string("'at_end\\\\'")),
     ?_assertEqual({ok, [{part, 1, <<"c:\\">>},
                         {'.', 1},
                         {part, 1, <<"size">>}], 1},
                   dql_lexer:string("'c:\\\\'.'size'"))
    ].
