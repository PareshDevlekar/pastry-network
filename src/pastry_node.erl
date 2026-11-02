%% One receive-loop actor per Pastry peer. No shared routing registry.
-module(pastry_node).
-export([start/2, join/2, route/4, state/1, stop/1, prefix/2, block/3, heal/2, repair/1]).

start(Id, Owner) -> spawn(fun() -> erlang:send_after(30000+rand:uniform(5000),self(),maintenance), loop(#{id=>Id, owner=>Owner,
    blocked=>#{}, pending=>#{}, seen=>#{}, monitors=>#{},
    leaf=>[], near=>[], table=>#{}, objects=>#{}}) end).
join(Pid, Bootstrap) -> call(Pid, {join, Bootstrap}).
route(Pid, Key, Message, Reply) when Key >= 0, Key < (1 bsl 128) ->
    Ref=make_ref(), Pid ! {route, Key, Message, Reply, Ref, 0}, Ref.
state(Pid) -> call(Pid, state).
%% Directed link; call on both endpoints for an undirected failure.
block(Pid, Other, Duration) -> call(Pid,{block,Other,Duration}).
heal(Pid, Other) -> call(Pid,{heal,Other}).
repair(Pid) -> call(Pid,repair).
stop(Pid) -> Pid ! stop.
call(Pid, Msg) -> Ref=make_ref(), Pid ! {call,self(),Ref,Msg},
    receive {Ref,Answer}->Answer after 30000->error({timeout,Msg}) end.
entry(S) -> {maps:get(id,S),self()}.
known(S) -> lists:usort(maps:get(leaf,S)++maps:get(near,S)++maps:values(maps:get(table,S))).
%% All actors are local; deterministic virtual coordinates provide a proximity metric.
position(Id) -> Id rem 1000003.
proximity(A,B) -> abs(position(A)-position(B)).
prefix(A,B) -> prefix(A,B,0).
prefix(_,_,32) -> 32;
prefix(A,B,N) -> case digit(A,N)=:=digit(B,N) of
    true->prefix(A,B,N+1); false->N end.
digit(Id,N) -> (Id bsr (124-4*N)) band 15.
merge(S, Entries) ->
    Id=maps:get(id,S),
    All=lists:usort([E || E={I,_} <- known(S)++Entries,I=/=Id,is_process_alive(element(2,E))]),
    Lower=lists:reverse(lists:sort([E || E={I,_}<-All,I<Id])),
    Upper=lists:sort([E || E={I,_}<-All,I>Id]),
    Leaf=lists:sublist(Lower,8)++lists:sublist(Upper,8),
    Near=lists:sublist(lists:sort(fun({A,_},{B,_})->
        {proximity(Id,A),A} < {proximity(Id,B),B} end,All),16),
    Table=lists:foldl(fun(E={I,_},T)->
        K={prefix(Id,I),digit(I,prefix(Id,I))},
        case maps:find(K,T) of
            error->T#{K=>E};
            {ok,{Old,_}} -> case {proximity(Id,I),I} < {proximity(Id,Old),Old} of
                true->T#{K=>E};false->T end
        end end,#{},All),
    Retained=lists:usort(Leaf++Near++maps:values(Table)),
    OldMons=maps:get(monitors,S),
    Keep=[P || {_,P}<-Retained],
    maps:foreach(fun(P,R)->case lists:member(P,Keep) of
        true->ok;false->erlang:demonitor(R,[flush]) end end,OldMons),
    Mons=lists:foldl(fun({_,P},M)->case maps:is_key(P,M) of
        true->M;false->M#{P=>erlang:monitor(process,P)} end end,maps:with(Keep,OldMons),Retained),
    S#{leaf=>Leaf,near=>Near,table=>Table,monitors=>Mons}.
closest(Key,Es) -> hd(lists:sort(fun({A,_},{B,_})->
    {abs(A-Key),A} < {abs(B-Key),B} end,Es)).
next(Key,S) ->
    Me=entry(S), Id=maps:get(id,S), Leaf=[Me|maps:get(leaf,S)],
    Ids=[I || {I,_}<-Leaf],
    case Key>=lists:min(Ids) andalso Key=<lists:max(Ids) of
        true->closest(Key,Leaf);
        false when Key=:=Id->Me;
        false -> L=prefix(Id,Key),
            case maps:find({L,digit(Key,L)},maps:get(table,S)) of
                {ok,E}->E;
                error -> Candidates=[E || E={I,_}<-known(S),
                    prefix(I,Key)>=L,abs(I-Key)<abs(Id-Key)],
                    closest(Key,[Me|Candidates])
            end
    end.
loop(S) -> receive
    {call,From,Ref,state}->From!{Ref,S},loop(S);
    {call,From,Ref,{join,none}}->From!{Ref,ok},loop(S);
    {call,From,Ref,{join,Bootstrap}}->
        Bootstrap!{join_route,entry(S),self(),[],From,Ref},loop(S);
    {join_route,New={Key,_},Joiner,Path,From,Ref}->
        Snapshot=[entry(S)|known(S)],
        case next(Key,S) of
            {_,P} when P=:=self()->Joiner!{install,lists:append([Snapshot|Path]),From,Ref};
            {_,P}->P!{join_route,New,Joiner,[Snapshot|Path],From,Ref}
        end,loop(S);
    {install,Entries,From,Ref}->
        S1=merge(S,Entries), Peers=known(S1),
        [P!{announce,entry(S1),self(),Ref} || {_,P}<-Peers],
        await_acks(length(Peers),Ref),From!{Ref,ok},loop(S1);
    {announce,E,Sender,Ref}->Sender!{ack,Ref},loop(merge(S,[E]));
    {call,From,Ref,{block,P,Duration}}->
        Until=case Duration of permanent->permanent;N when is_integer(N),N>0->
            erlang:monotonic_time(millisecond)+N end,
        From!{Ref,ok},loop(S#{blocked=>maps:put(P,Until,maps:get(blocked,S))});
    {call,From,Ref,{heal,P}}->From!{Ref,ok},
        loop(S#{blocked=>maps:remove(P,maps:get(blocked,S))});
    {call,From,Ref,repair}->self()!maintenance_once,From!{Ref,ok},loop(S);
    maintenance->erlang:send_after(30000+rand:uniform(5000),self(),maintenance),
        self()!maintenance_once,loop(S);
    maintenance_once->
        S1=merge(S,[]),
        [P!{exchange,entry(S1),known(S1)} || {_,P}<-repair_peers(S1), link_up(P,S1)],
        loop(S1);
    {exchange,E={_,P},Entries}->
        case link_up(P,S) of
            true->P!{exchange_reply,entry(S),known(S)},loop(merge(S,[E|Entries]));
            false->loop(S)
        end;
    {exchange_reply,E={_,P},Entries}->
        case link_up(P,S) of true->loop(merge(S,[E|Entries]));false->loop(S) end;
    {'DOWN',_,process,P,_}->
        %% Rebuild from surviving entries and ask surviving neighbors for replacements.
        S1=merge(S,[]),self()!maintenance_once,
        loop(S1#{monitors=>maps:remove(P,maps:get(monitors,S1))});
    {route,Key,Msg,Reply,Ref,Hops}->
        loop(handle_route({route,Key,Msg,Reply,Ref,Hops},merge(S,[])));
    {wire,Token,Origin,Target,Payload,Visited,Extra}->
        case maps:is_key(Token,maps:get(seen,S)) of
            true->loop(S);
            false->
                erlang:send_after(10000,self(),{forget,Token}),
                S1=S#{seen=>maps:put(Token,true,maps:get(seen,S))},
                case Target=:=self() of
                    true->Origin!{wire_ack,Token},
                        {route,K,M,R,F,H}=Payload,
                        loop(handle_route({route,K,M,R,F,H+Extra},merge(S1,[])));
                    false->relay(Token,Origin,Target,Payload,[self()|Visited],Extra+1,S1),loop(S1)
                end
        end;
    {wire_ack,Token}->loop(S#{pending=>maps:remove(Token,maps:get(pending,S))});
    {forget,Token}->loop(S#{seen=>maps:remove(Token,maps:get(seen,S))});
    {wire_timeout,Token}->
        case maps:take(Token,maps:get(pending,S)) of
            error->loop(S);
            {{Target,Payload},Rest}->
                S1=S#{pending=>Rest},
                case is_process_alive(Target) of
                    false->loop(handle_route(Payload,merge(S1,[])));
                    true-> {route,_,_,Reply,Ref,_}=Payload,
                        Reply!{route_failed,Ref,unreachable},loop(S1)
                end
        end;
    {replica,Key,Value}->loop(S#{objects=>maps:put(Key,Value,maps:get(objects,S))});
    {work,NumKeys,Count,Collector}->
        self()!{tick,NumKeys,Count,Collector},loop(S);
    {tick,_,0,_}->loop(S);
    {tick,NumKeys,Count,Collector}->
        <<Key:128,_/binary>>=crypto:hash(sha256,integer_to_binary(rand:uniform(NumKeys))),
        route(self(),Key,get,Collector),
        erlang:send_after(1000,self(),{tick,NumKeys,Count-1,Collector}),loop(S);
    stop->ok
end.
await_acks(0,_)->ok;
await_acks(N,Ref)->receive {ack,Ref}->await_acks(N-1,Ref)
    after 30000->error(join_announcement_timeout) end.
%% Application's deliver callback: a key/string object access service.
deliver({put,Value},Key,S) when is_list(Value);is_binary(Value) ->
    Backups=lists:sublist(lists:sort(fun({A,_},{B,_})->
        {abs(A-Key),A} < {abs(B-Key),B} end,maps:get(leaf,S)),2),
    [P!{replica,Key,Value} || {_,P}<-Backups,link_up(P,S)],
    {ok,S#{objects=>maps:put(Key,Value,maps:get(objects,S))}};
deliver(get,Key,S)->{maps:find(Key,maps:get(objects,S)),S}.

link_up(P,S)->
    case maps:get(P,maps:get(blocked,S),0) of
        permanent->false;
        0->true;
        Until->erlang:monotonic_time(millisecond)>=Until
    end.
handle_route(Payload={route,Key,Msg,Reply,Ref,Hops},S)->
    case next(Key,S) of
        {_,P} when P=:=self()->
            {Result,S1}=deliver(Msg,Key,S),
            Reply!{delivered,Ref,maps:get(id,S),Result,Hops},S1;
        {_,P}->
            Token=make_ref(),
            {route,K,M,R,F,H}=Payload, Out={route,K,M,R,F,H+1},
            relay(Token,self(),P,Out,[self()],0,S),
            erlang:send_after(2000,self(),{wire_timeout,Token}),
            S#{pending=>maps:put(Token,{P,Payload},maps:get(pending,S))}
    end.
%% Normal path is one direct message. A broken connection invokes bounded
%% duplicate-suppressed flooding of known neighbors to find a transport detour.
relay(Token,Origin,Target,Payload,Visited,Extra,S)->
    case is_process_alive(Target) andalso link_up(Target,S) of
        true->Target!{wire,Token,Origin,Target,Payload,Visited,Extra};
        false->
            [P!{wire,Token,Origin,Target,Payload,Visited,Extra} || {_,P}<-known(S),
                not lists:member(P,Visited),is_process_alive(P),link_up(P,S)]
    end.

repair_peers(S)->
    Entries=known(S),
    case Entries of
        []->[];
        _->Sorted=lists:sort(maps:get(leaf,S)),
            Edges=case Sorted of []->[];_->[hd(Sorted),lists:last(Sorted)] end,
            lists:usort(Edges++[lists:nth(rand:uniform(length(Entries)),Entries)||_<-[1,2]])
    end.
