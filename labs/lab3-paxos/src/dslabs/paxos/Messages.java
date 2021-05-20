package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Message;
import dslabs.paxos.PaxosServer.Ballot;
import dslabs.paxos.PaxosServer.LogEntry;
import java.util.HashMap;
import lombok.Data;

// Your code here...
@Data
class P2A implements Message {
    // P2A message should contain (ballot, slot number, command)
    private final Ballot ballot;
    private final AMOCommand command;
    private final int slotNum;
}

@Data
class P2B implements Message {
    private final Ballot ballot;
    private final Integer slotNum;
}

@Data
class Heartbeat implements Message {
    private final HashMap<Integer, LogEntry> log;
    private final Ballot ballot;
    private final HashMap<Address, Integer> garbageCollection;
}

@Data
class HeartbeatReply implements Message {
    private final int slotOut;
}

@Data
class P1A implements Message {
    private final Ballot ballot;
}

@Data
class P1B implements Message {
    private final Ballot ballot;
    private final HashMap<Integer, LogEntry> log;
}
