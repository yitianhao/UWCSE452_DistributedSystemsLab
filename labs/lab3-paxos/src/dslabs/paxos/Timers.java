package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Timer;
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
    static final int P2A_RETRY_TIMER = 50;
    private final Integer slotNum;
    private final Address acceptor;
    private final AMOCommand command;
}
