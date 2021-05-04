package dslabs.paxos;

import dslabs.atmostonce.AMOResult;
import dslabs.framework.Message;
import lombok.Data;

@Data
public final class PaxosReply implements Message {
    // Your code here...
    private final AMOResult result;
}
