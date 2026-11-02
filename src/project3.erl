-module(project3).
-export([main/1, build/1, collect/3]).
main(Args) ->
    try
        [NS,RS]=Args, N=list_to_integer(NS), R=list_to_integer(RS),
        true=N>0, true=R>=0,
        Peers=build(N),
        %% Populate after joins so object placement is stable.
        lists:foreach(fun({Id,P})->
            Ref=pastry_node:route(P,Id,{put,"object-"++integer_to_list(Id)},self()),
            receive {delivered,Ref,Id,ok,_}->ok after 30000->error(put_timeout) end
        end,Peers),
        [P!{work,N,R,self()} || {_,P}<-Peers],
        {Total,Count}=collect(N*R,0,0),
        Avg=case Count of 0->0.0;_->Total/Count end,
        io:format("Average hops: ~.6f~n",[Avg]),
        [pastry_node:stop(P) || {_,P}<-Peers]
    catch _:Reason -> io:format(standard_error,"Error: ~p~nUsage: project3 numNodes numRequests~n",[Reason]),halt(1)
    end.
build(N) -> build(1,N,[]).
build(I,N,Peers) when I>N -> lists:reverse(Peers);
build(I,N,Peers) ->
    <<Id:128,_/binary>>=crypto:hash(sha256,integer_to_binary(I)),
    P=pastry_node:start(Id,self()),
    Bootstrap=case Peers of []->none;_->
        {_,B}=lists:nth(rand:uniform(length(Peers)),Peers),B end,
    ok=pastry_node:join(P,Bootstrap),build(I+1,N,[{Id,P}|Peers]).
collect(0,Total,Count)->{Total,Count};
collect(N,Total,Count)->receive
    {delivered,_,_,{ok,_},H}->collect(N-1,Total+H,Count+1);
    {delivered,_,_,Result,_}->error({lookup_failed,Result});
    {route_failed,Ref,Reason}->error({route_failed,Ref,Reason})
    after 60000->error({request_timeout,N}) end.
