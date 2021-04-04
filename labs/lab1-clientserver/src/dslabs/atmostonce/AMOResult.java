package dslabs.atmostonce;

import dslabs.framework.Result;
import lombok.Data;

@Data
public final class AMOResult implements Result {
    // Your code here...
    private final Result result;
    private final String clientID;
    private final int sequenceNum;
}
