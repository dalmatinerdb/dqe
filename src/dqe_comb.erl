-module(dqe_comb).

-behaviour(dflow).

-export([init/1, describe/1, start/2, emit/3, done/2]).

-record(state, {
          aggr,
          acc = gb_trees:empty(),
          count,
          term_for_child = dict:new()
         }).

init([Aggr, SubQs]) ->
    SubQs1 = [{make_ref(), SubQ} || SubQ <- SubQs],
    Count = length(SubQs1),
    {ok, #state{aggr = Aggr, count = Count}, SubQs1}.

describe(#state{aggr = Aggr}) ->
    atom_to_list(Aggr).

start({_Start, _Count}, State) ->
    {ok, State}.

emit(Child, {realized, {Data, Resolution}},
     State = #state{aggr = Aggr, term_for_child = TFC,
                    count = Count, acc = Tree}) ->
    TFC1 = dict:update_counter(Child, 1, TFC),
    Term = dict:fetch(Child, TFC1),
    Tree1 = add_to_tree(Term, Data, Aggr, Tree),
    case shrink_tree(Tree1, Count, <<>>) of
        {Tree2, <<>>} ->
            {ok, State#state{acc = Tree2, term_for_child = TFC1}};
        {Tree2, Data1} ->
            {emit, {realized, {Data1, Resolution}},
             State#state{acc = Tree2, term_for_child = TFC1}}
    end.

done({last, _Child}, State) ->
    {done, State};

done(_, State) ->
    {ok, State}.

add_to_tree(Term, Data, Aggr, Tree) ->
    case gb_trees:lookup(Term, Tree) of
        none ->
            gb_trees:insert(Term, {Data, 1}, Tree);
        {value, {Sum, Count}} ->
            Sum1 = mmath_comb:Aggr([Sum, Data]),
            gb_trees:update(Term, {Sum1, Count+1}, Tree)
    end.

shrink_tree(Tree, Count, Acc) ->
    case gb_trees:is_empty(Tree) of
        true ->
            {Tree, Acc};
        _ ->
            case gb_trees:smallest(Tree) of
                {Term0, {Data, FirstCount}} when FirstCount =:= Count ->
                    Tree1 = gb_trees:delete(Term0, Tree),
                    shrink_tree(Tree1, Count, <<Acc/binary, Data/binary>>);
                _ ->
                    {Tree, Acc}
            end
    end.
