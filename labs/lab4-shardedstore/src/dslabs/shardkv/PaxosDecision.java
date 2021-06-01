package dslabs.shardkv;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Command;
import dslabs.framework.Message;
import lombok.Data;

// Your code here...
@Data
public final class PaxosDecision implements Message {
    private final Command command;
}
