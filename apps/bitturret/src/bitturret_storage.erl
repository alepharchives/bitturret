-module(bitturret_storage).

-compile(export_all).

% Public API

%create 256 ETS tables for each first byte of the infohash

start() ->
	spawn_loop(256).


% adds a peer to the list of peers for the infohash
addpeer(Peer, InfoHash) ->
	Tablename = get_tablename(InfoHash),
    PeersTmp = ets:lookup(Tablename,InfoHash),
	case PeersTmp of
			[] ->
					ets:insert(Tablename,{InfoHash,[Peer]});
			_ ->
					{_InfoHash,PeerList} = hd(PeersTmp),
					ets:insert(Tablename,{InfoHash,[Peer|PeerList]})
	    end.


% get all the peers for the specified infohash
getpeers(InfoHash,_Count) ->
	Tablename = get_tablename(InfoHash),
	PeersTmp = ets:lookup(Tablename,InfoHash),
		{_InfoHash,PeerList} = hd(PeersTmp),
			PeerList.

% Private Functions

get_tablename(<< Prefix:8/big,_Rest/binary >>) -> 
	list_to_atom("IH_" ++ integer_to_list(Prefix));


get_tablename(Num) ->
	list_to_atom("IH_" ++ integer_to_list(Num)).



spawn_loop(0) ->
	ok;


spawn_loop(Num) -> 
	spawn(?MODULE,infohash_start,[Num]),
	spawn_loop(Num-1).	



infohash_start(Prefix) ->
	Tabname = get_tablename(Prefix),
	ets:new(Tabname,[set,public,named_table]),	
	infohash_loop(Prefix).



infohash_loop(Prefix) ->
	receive
		{flush,_} -> infohash_flush(Prefix);
		{stats,_} -> infohash_stats(Prefix);
		{_,_} -> ok
	end,
	infohash_loop(Prefix).



infohash_flush(_Prefix) ->
	ok.



infohash_stats(_Prefix) ->
	ok.
