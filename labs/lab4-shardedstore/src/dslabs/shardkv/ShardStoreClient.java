package dslabs.shardkv;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore.SingleKeyCommand;
import dslabs.paxos.PaxosReply;
import dslabs.paxos.PaxosRequest;
import dslabs.shardmaster.ShardMaster.Error;
import dslabs.shardmaster.ShardMaster.Query;
import dslabs.shardmaster.ShardMaster.ShardConfig;
import dslabs.shardmaster.ShardMaster.ShardMasterResult;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;

import static dslabs.shardkv.ClientTimer.CLIENT_RETRY_MILLIS;
import static dslabs.shardkv.QueryTimer.QUERY_RETRY_MILLIS;
import static dslabs.shardkv.ShardStoreServer.DUMMY_SEQ_NUM;
import static dslabs.shardmaster.ShardMaster.INITIAL_CONFIG_NUM;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreClient extends ShardStoreNode implements Client {
    // Your code here...
    private ShardConfig currShardConfig = new ShardConfig(DUMMY_SEQ_NUM, new HashMap<>());
    private ShardStoreRequest shardStoreRequest;
    private ShardStoreReply shardStoreReply;
    private int seqNum;
    private AMOCommand skippedCommand;
    private boolean firstCommandSkipped;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public ShardStoreClient(Address address, Address[] shardMasters,
                            int numShards) {
        super(address, shardMasters, numShards);
    }

    @Override
    public synchronized void init() {
        // Your code here...
        broadcastToShardMasters(new PaxosRequest(new AMOCommand(new Query(-1), address(), DUMMY_SEQ_NUM)));
        this.set(new QueryTimer(), QUERY_RETRY_MILLIS);

        currShardConfig = new ShardConfig(DUMMY_SEQ_NUM, new HashMap<>());
    }

    /* -------------------------------------------------------------------------
        Public methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command command) {
        // Your code here...
        //System.out.println(command);
        AMOCommand amoCommand = new AMOCommand(command, this.address(), seqNum);
        shardStoreRequest = new ShardStoreRequest(amoCommand, currShardConfig.configNum());
        shardStoreReply = null;

        int groupID = getGroupIdForShard(command);
        if (currShardConfig.configNum() >= INITIAL_CONFIG_NUM) {
            broadcast(new ShardStoreRequest(amoCommand, currShardConfig.configNum()), currShardConfig.groupInfo().get(groupID).getLeft());
        }
        this.set(new ClientTimer(amoCommand), CLIENT_RETRY_MILLIS);
    }

    @Override
    public synchronized boolean hasResult() {
        // Your code here...
        return shardStoreReply != null;
    }

    @Override
    public synchronized Result getResult() throws InterruptedException {
        // Your code here...
        while (shardStoreReply == null) {
            wait();
        }

        seqNum++;
        return shardStoreReply.result().result();
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private synchronized void handleShardStoreReply(ShardStoreReply m,
                                                    Address sender) {
        // Your code here...
        if (m != null && m.result() != null && seqNum == m.result().sequenceNum()) {
//            System.out.println("---" + m.result().toString());
//            System.out.println();
            shardStoreReply = m;
            notify();
        }
    }

    // Your code here...
    private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
        if (m != null && m.result() != null && m.result().result() instanceof ShardConfig &&
                ((ShardConfig) m.result().result()).configNum() > currShardConfig.configNum()) {
            currShardConfig = (ShardConfig) m.result().result();
            //System.out.println(currShardConfig);
        }
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        // Your code here...
        if (currShardConfig.configNum() >= INITIAL_CONFIG_NUM
                && seqNum == t.command().sequenceNum() && shardStoreReply == null) {
            int groupID = getGroupIdForShard(t.command().command());
            broadcast(new ShardStoreRequest(t.command(), currShardConfig.configNum()), currShardConfig.groupInfo().get(groupID).getLeft());
            this.set(t, CLIENT_RETRY_MILLIS);
        } else if (currShardConfig.configNum() < INITIAL_CONFIG_NUM) {
            this.set(t, CLIENT_RETRY_MILLIS);
        }
    }

    private void onQueryTimer(QueryTimer t) {
        broadcastToShardMasters(new PaxosRequest(new AMOCommand(new Query(-1), address(), DUMMY_SEQ_NUM)));
        this.set(t, QUERY_RETRY_MILLIS);
    }

    private int getGroupIdForShard(Command command) {
        int shardNum = keyToShard(((SingleKeyCommand) command).key());
        for (int groupID : currShardConfig.groupInfo().keySet()) {
            if (currShardConfig.groupInfo().get(groupID).getRight().contains(shardNum)) return groupID;
        }
        return -1;
    }

}
