-module(failure_test).
-export([run/0]).
run()->
    rand:seed(exsplus,{11,22,33}),
    node_failure(1),node_failure(7),links(),partition(),
    io:format("All failure tests passed.~n").
request(P,K,M)->
    Ref=pastry_node:route(P,K,M,self()),
    receive {delivered,Ref,Id,Value,H}->{Id,Value,H};
        {route_failed,Ref,Why}->error({unexpected_failure,Why})
    after 5000->error(failure_test_timeout) end.
settle(Peers)->
    [pastry_node:repair(P)||{_,P}<-Peers],
    timer:sleep(1200).
node_failure(Count)->
    Peers=lists:sort(project3:build(128)),
    Victims=lists:sublist(lists:nthtail(50,Peers),Count),
    {Key,Owner}=hd(Victims),{_,Source}=hd(Peers),
    {Key,ok,_}=request(Source,Key,{put,"survives"}),
    %% Barrier proves backup delivery before injecting the crash.
    S=pastry_node:state(Owner),
    Backups=lists:sublist(lists:sort(fun({A,_},{B,_})->
        {abs(A-Key),A}<{abs(B-Key),B} end,maps:get(leaf,S)),2),
    [begin BS=pastry_node:state(B),"survives"=maps:get(Key,maps:get(objects,BS)) end||{_,B}<-Backups],
    InFlight=case Count of
        1->true=erlang:suspend_process(Owner),
            F=pastry_node:route(Source,Key,get,self()),
            %% The source barrier ensures the packet was sent to the suspended owner.
            _=pastry_node:state(Source),F;
        _->none
    end,
    [exit(P,kill)||{_,P}<-Victims],
    Live=Peers--Victims,settle(Live),
    case InFlight of
        none->ok;
        _->receive {delivered,InFlight,_,{ok,"survives"},_}->ok
            after 5000->error(inflight_retry_failed) end
    end,
    Keys=[I||{I,_}<-Peers]++[rand:uniform(1 bsl 128)-1||_<-lists:seq(1,200)],
    Hops=lists:map(fun(K)->
        {Expected,_}=hd(lists:sort(fun({A,_},{B,_})->
            {abs(A-K),A}<{abs(B-K),B} end,Live)),
        {_,P}=lists:nth(rand:uniform(length(Live)),Live),
        {Expected,ok,H}=request(P,K,{put,"after-failure"}),H
    end,lists:delete(Key,Keys)),
    %% Seven adjacent failures exceed the three-copy data redundancy;
    %% routing is tested in that case without claiming object durability.
    case Count of 1->{_,{ok,"survives"},_}=request(Source,Key,get);_->ok end,
    io:format("~p adjacent node deaths: ~p routes, average ~.4f hops.~n",
        [Count,length(Hops),lists:sum(Hops)/length(Hops)]),
    [pastry_node:stop(P)||{_,P}<-Live].
links()->
    Peers=project3:build(128),{_,A}=hd(Peers),
    S=pastry_node:state(A),{Key,B}=hd(maps:get(leaf,S)),
    {Key,ok,1}=request(A,Key,{put,"link-value"}),
    ok=pastry_node:block(A,B,permanent),ok=pastry_node:block(B,A,permanent),
    {Key,{ok,"link-value"},H}=request(A,Key,get),true=H>1,
    ok=pastry_node:heal(A,B),ok=pastry_node:heal(B,A),
    {Key,{ok,"link-value"},1}=request(A,Key,get),
    ok=pastry_node:block(A,B,300),ok=pastry_node:block(B,A,300),
    {Key,{ok,"link-value"},Temp}=request(A,Key,get),true=Temp>1,
    timer:sleep(350),{Key,{ok,"link-value"},1}=request(A,Key,get),
    io:format("Permanent/temporary link detours: ~p/~p hops; healed: 1 hop.~n",[H,Temp]),
    [pastry_node:stop(P)||{_,P}<-Peers].
partition()->
    Peers=project3:build(32),{_,A}=hd(Peers),{Key,_}=lists:last(Peers),
    [pastry_node:block(A,P,permanent)||{_,P}<-tl(Peers)],
    Ref=pastry_node:route(A,Key,get,self()),
    receive {route_failed,Ref,unreachable}->ok
    after 4000->error(partition_not_reported) end,
    io:format("Isolated source: explicit unreachable result (no false delivery).~n"),
    [pastry_node:stop(P)||{_,P}<-Peers].
