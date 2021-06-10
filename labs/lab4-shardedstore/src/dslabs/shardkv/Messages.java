package dslabs.shardkv;

import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Message;
import dslabs.shardkv.ShardStoreServer.ShardMove;
import dslabs.shardkv.ShardStoreServer.ShardMoveAck;
import lombok.Data;

@Data
final class ShardStoreRequest implements Message {
    // Your code here...
    private final AMOCommand command;
    private final int configNum;
}

@Data
final class ShardStoreReply implements Message {
    // Your code here...
    private final AMOResult result;
}

@Data
final class ShardMoveMsg implements Message {
    private final ShardMove shardMoveCommand;
}

@Data
final class ShardMoveAckMsg implements Message {
    private final ShardMoveAck shardMoveAckCommand;
}

