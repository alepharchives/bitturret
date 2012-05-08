-module (bitturret_handler).
-export ([start/0, loop_buffer/2, loop_accept/2, handle/2]).
-export ([stats_loop/1,  print_stats/0]).
% Packet buffer length.
-define (BUFLEN, 256).


% OTP boilerplate.
start() ->
    PortNo   = bitturret_config:get(port),
    BindAddr = bitturret_config:get(bind_addr),

	%start ETS Tables
	bitturret_storage:start(),

    stats_debug(),

    BufferPid = spawn_link(?MODULE, loop_buffer, [0, []]),

    % Open the socket itself.
    SocketOpts = [
        binary,
        {ip, BindAddr},
        {active, true}
        % {recbuf, 512},
        % {read_packets, 16}
    ],
    {ok, Socket} = gen_udp:open(PortNo, SocketOpts),

    loop_accept(Socket, BufferPid).



stats_debug() ->
    StatsPid = spawn(?MODULE, stats_loop, [0]),
    register(stats, StatsPid),
    timer:apply_interval(1000, ?MODULE, print_stats, []).


print_stats() -> stats ! flush.


stats_loop(Overall0) ->
    receive
        flush ->
            io:format("~p~n", [Overall0]),
            stats_loop(0);
        NumPackets ->
            stats_loop(Overall0 + NumPackets)
    end.



% Attempt to accept new packets as fast as possible.
loop_accept(Socket, BufferPid) ->
    receive
        Message ->
            % Buffer messages first, continue accepting.
            BufferPid ! Message,
            ?MODULE:loop_accept(Socket, BufferPid)
    end.


% Flush message buffer in case full.
loop_buffer(?BUFLEN, Buffer) ->
    flush_buffer(Buffer,?BUFLEN),
    loop_buffer(0, []);


% Add new messages to a non-full buffer.
loop_buffer(BufferLength, Buffer) ->
    receive
        % Prepend for performance.
        Message ->
            loop_buffer(1 + BufferLength, [Message|Buffer])
    after
        50 ->
            % Flush on timeout.
            flush_buffer(Buffer,BufferLength),
            loop_buffer(0, [])
    end.


% Handle all messages in the buffer. Immediately returns.
flush_buffer(Buffer,BufferLength) ->
    spawn(?MODULE, handle, [Buffer,BufferLength]).


% For every received message, make a new worker process handle it.
handle(Messages,BufferLength) ->
    stats ! BufferLength,
    spawn(bitturret_worker, handle, [Messages]).
