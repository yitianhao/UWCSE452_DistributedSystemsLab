package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Message;
import dslabs.paxos.PaxosServer.Ballot;
import lombok.Data;

// Your code here...
@Data
class P2A implements Message {
    // P2A message should contain (ballot, slot number, command)
    private final Ballot ballot;
    private final Integer slotNum;
    private final AMOCommand command;
}

@Data
class P2B implements Message {
    private final Ballot ballot;
    private final Integer slotNum;
}
