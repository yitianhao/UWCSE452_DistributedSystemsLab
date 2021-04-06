package dslabs.atmostonce;

import dslabs.framework.Address;
import dslabs.framework.Result;
import lombok.Data;

@Data
public final class AMOResult implements Result {
    // Your code here...
    private final Result result;
    private final Address serverID;
    private final Address clientID;
    private final int sequenceNum;
}
