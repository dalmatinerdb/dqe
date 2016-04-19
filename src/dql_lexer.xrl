%% -*- erlang -*-
% DQL lexer - based on the example from https://github.com/relops/leex_yecc_example/blob/master/src/selector_lexer.xrl

Definitions.

Sign    = [\-+]?
Digit   = [0-9]
Float   = {Digit}+\.{Digit}+([eE][-+]?[0-9]+)?

PART    = '([^']|\.)+'
%'% damn you syntax highlighter
DATE    = "([^"]|\.)+"
%"% damn you syntax highlighter
MET     = {PART}(\.{PART})+
S       = [A-Za-z][A-Za-z0-9_@-]*
WS      = ([\000-\s]|%.*)
MM      = (min|max)
AVG     = (avg)
HFUN    = (mean|median|stddev)
AGGR    = (empty)
CAGGR   = (sum)
MATH    = (divide|multiply)
HIST    = (histogram)
PERC    = (percentile)
TIME    = (ms|s|m|h|d|w)
SELECT  = [Ss][Ee][Ll][Ee][Cc][Tt]
BUCKET  = [Bb][Uu][Cc][Kk][Ee][Tt]
LAST    = [Ll][Aa][Ss][Tt]
AS      = [Aa][Ss]
FROM    = [Ff][Rr][Oo][Mm]
LIKE    = [Ll][Ii][Kk][Ee]
BETWEEN = [Bb][Ee][Tt][Ww][Ee][Ee][Nn]
IN      = [Ii][Nn]
NOW     = [Nn][Oo][Ww]
AGO     = [Aa][Gg][Oo]
AND     = [Aa][Nn][Dd]
OR      = [Oo][Rr]
AFTER   = [Aa][Ff][Tt][Ee][Rr]
BEFORE  = [Bb][Ee][Ff][Oo][Rr][Ee]
FOR     = [Ff][Oo][Rr]
WHERE   = [Ww][Hh][Ee][Rr][Ee]



Rules.
{SELECT}    :   {token, {kw_select,     TokenLine}}.
{BUCKET}    :   {token, {kw_bucket,     TokenLine}}.
{LAST}      :   {token, {kw_last,       TokenLine}}.
{AS}        :   {token, {kw_as,         TokenLine}}.
{IN}        :   {token, {kw_in,         TokenLine}}.
{FROM}      :   {token, {kw_from,       TokenLine}}.
{BETWEEN}   :   {token, {kw_between,    TokenLine}}.
{LIKE}      :   {token, {kw_like,       TokenLine}}.
{NOW}       :   {token, {kw_now,        TokenLine}}.
{AGO}       :   {token, {kw_ago,        TokenLine}}.
{AND}       :   {token, {kw_and,        TokenLine}}.
{OR}        :   {token, {kw_or,         TokenLine}}.
{AFTER}     :   {token, {kw_after,      TokenLine}}.
{BEFORE}    :   {token, {kw_before,     TokenLine}}.
{FOR}       :   {token, {kw_for,        TokenLine}}.
{WHERE}     :   {token, {kw_where,      TokenLine}}.

derivate    :   {token, {derivate,      TokenLine, a(TokenChars)}}.
{AGGR}      :   {token, {aggr,          TokenLine, a(TokenChars)}}.
{MM}        :   {token, {mm,            TokenLine, a(TokenChars)}}.
{AVG}       :   {token, {avg,           TokenLine, a(TokenChars)}}.
{HFUN}      :   {token, {hfun,          TokenLine, a(TokenChars)}}.
{MATH}      :   {token, {math,          TokenLine, a(TokenChars)}}.
{CAGGR}     :   {token, {caggr,         TokenLine, a(TokenChars)}}.
{TIME}      :   {token, {time,          TokenLine, a(TokenChars)}}.
{HIST}      :   {token, {histogram,     TokenLine, a(TokenChars)}}.
{PERC}      :   {token, {percentile,    TokenLine, a(TokenChars)}}.

{Sign}{Digit}+ : {token, {integer,       TokenLine, i(TokenChars)}}.
{Sign}{Float}  : {token, {float,         TokenLine, f(TokenChars)}}.

{PART}      :   S = strip(TokenChars,   TokenLen),
                {token, {part,          TokenLine, b(S)}}.
{DATE}      :   S = strip(TokenChars,   TokenLen),
                {token, {date,          TokenLine, S}}.
{S}         :   {token, {name,          TokenLine, b(TokenChars)}}.
[(),.*/=:]  :   {token, {a(TokenChars), TokenLine}}.
{WS}+       :   skip_token.

Erlang code.

-ignore_xref([format_error/1, string/2, token/2, token/3, tokens/2, tokens/3]).
-dialyzer({nowarn_function, yyrev/2}).
strip(TokenChars,TokenLen) -> lists:sublist(TokenChars, 2, TokenLen - 2).

a(L) -> list_to_atom(L).
i(L) -> list_to_integer(L).
f(L) -> list_to_float(L).
b(L) -> list_to_binary(L).
