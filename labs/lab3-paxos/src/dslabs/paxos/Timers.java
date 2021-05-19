package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Timer;
import dslabs.paxos.PaxosServer.Ballot;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
    static final int CLIENT_RETRY_MILLIS = 100;

    // Your code here...
    private final AMOCommand command;
}

// Your code here...
@Data
final class P2ATimer implements Timer {
    static final int P2A_RETRY_TIMER = 25;
    private final Integer slotNum;
    private final AMOCommand command;
    private final Ballot ballot;
}

@Data
final class P1ATimer implements Timer {
    static final int P1A_RETRY_TIMER = 25;
    //private final Address acceptor;
    private final Ballot ballot;
}

@Data
final class HeartbeatCheckTimer implements Timer {
    static final int HEARTBEAT_CHECK_MILLIS = 300;
}

@Data
final class HeartbeatReplyCheckTimer implements Timer {
    static final int HEARTBEAT_REPLY_CHECK_MILLIS = 400;
    //private final Ballot ballot;
}

@Data
final class HeartbeatTimer implements Timer {
    static final int HEARTBEAT_MILLIS = 25;
    private final Ballot ballot;
}
