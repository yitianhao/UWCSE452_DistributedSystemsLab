package dslabs.atmostonce;

import dslabs.framework.Address;
import dslabs.framework.Command;
import lombok.Data;

@Data
public final class AMOCommand implements Command {
    // Your code here...
    private final Command command;
    private final Address clientID;
    private final int sequenceNum;
}
