-module(pastry_test).
-export([run/0]).
run()->
    rand:seed(exsplus,{1,2,3}),
    lists:foreach(fun check/1,[1,2,17,100,1000]),
    io:format("All routing and object tests passed.~n").
check(N)->
    Peers=project3:build(N), Ids=[I||{I,_}<-Peers],
    %% Exact IDs, boundaries, and arbitrary keys. Oracle is only in tests.
    Keys=Ids++[0,(1 bsl 128)-1]++[rand:uniform(1 bsl 128)-1||_<-lists:seq(1,100)],
    lists:foreach(fun(Key)->
        {_,P}=lists:nth(rand:uniform(N),Peers),
        Expected=hd(lists:sort(fun(A,B)->{abs(A-Key),A}<{abs(B-Key),B} end,Ids)),
        Ref=pastry_node:route(P,Key,{put,"test-value"},self()),
        receive {delivered,Ref,Actual,ok,_}->
            case Actual=:=Expected of true->ok;false->error({wrong_owner,N,Key,Expected,Actual}) end
        after 5000->error(route_timeout) end,
        {_,Q}=lists:nth(rand:uniform(N),Peers),
        Get=pastry_node:route(Q,Key,get,self()),
        receive {delivered,Get,Expected,{ok,"test-value"},_}->ok
        after 5000->error(get_failed) end
    end,Keys),
    lists:foreach(fun({Id,P})->
        S=pastry_node:state(P),true=length(maps:get(leaf,S))=<16,
        true=length(maps:get(near,S))=<16,
        maps:foreach(fun({Row,Digit},{Other,_})->
            Row=pastry_node:prefix(Id,Other),
            Digit=(Other bsr (124-4*Row)) band 15
        end,maps:get(table,S))
    end,Peers),
    [pastry_node:stop(P)||{_,P}<-Peers],io:format("Validated ~p peers (~p keys).~n",[N,length(Keys)]).
