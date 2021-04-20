package dslabs.primarybackup;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Timer;
import lombok.Data;

@Data
final class PingCheckTimer implements Timer {
    static final int PING_CHECK_MILLIS = 100;
}

@Data
final class PingTimer implements Timer {
    static final int PING_MILLIS = 25;
}

@Data
final class ClientTimer implements Timer {
    static final int CLIENT_RETRY_MILLIS = 100;

    // Your code here...
    private final AMOCommand amoCommand;
}

// Your code here...
@Data
final class ForwardedRequestTimer implements Timer {
    static final int FORWARDED_RETRY_MILLIS = 100;
    private final AMOCommand amoCommand;
    private final Address client;
}

@Data
final class TransferredStateTimer implements Timer {
    static final int TRANSFERRED_RETRY_MILLIS = 100;
}
