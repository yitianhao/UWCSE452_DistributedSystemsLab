package dslabs.shardkv;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Timer;
import java.util.HashMap;
import jdk.jfr.DataAmount;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
    static final int CLIENT_RETRY_MILLIS = 200;

    // Your code here...
    private final AMOCommand command;
    private final int groupID;
}

// Your code here...
@Data
final class QueryTimer implements Timer {
    static final int QUERY_RETRY_MILLIS = 150;
}

@Data
final class ShardMoveTimer implements Timer {
    static final int SHARD_MOVE_RETRY_MILLIS = 50;
    private final Integer destGroupId;
    private final Integer configNum;
    private final HashMap<Integer, AMOApplication> shardsAndApp;
}