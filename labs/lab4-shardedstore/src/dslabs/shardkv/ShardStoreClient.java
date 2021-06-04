package dslabs.shardkv;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Result;
import dslabs.paxos.PaxosReply;
import dslabs.paxos.PaxosRequest;
import dslabs.shardmaster.ShardMaster.Query;
import dslabs.shardmaster.ShardMaster.ShardConfig;
import dslabs.shardmaster.ShardMaster.ShardMasterResult;
import java.util.HashMap;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.shardkv.ClientTimer.CLIENT_RETRY_MILLIS;
import static dslabs.shardkv.QueryTimer.QUERY_RETRY_MILLIS;
import static dslabs.shardmaster.ShardMaster.INITIAL_CONFIG_NUM;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreClient extends ShardStoreNode implements Client {
    // Your code here...
    private ShardConfig currShardConfig;
    private ShardStoreRequest shardStoreRequest;
    private ShardStoreReply shardStoreReply;
    private int seqNum;

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
        broadcastToShardMasters(new PaxosRequest(new AMOCommand(new Query(-1), address(), -1)));
        this.set(new QueryTimer(), QUERY_RETRY_MILLIS);

        currShardConfig = new ShardConfig(-1, new HashMap<>());
    }

    /* -------------------------------------------------------------------------
        Public methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command command) {
        // Your code here...
        int shardNum = keyToShard(command.toString());
        int groupID = getGroupIdForShard(shardNum);

        AMOCommand amoCommand = new AMOCommand(command, this.address(), seqNum);
        shardStoreRequest = new ShardStoreRequest(amoCommand);
        shardStoreReply = null;

        if (currShardConfig.configNum() >= INITIAL_CONFIG_NUM) {
            for (Address server : currShardConfig.groupInfo().get(groupID).getLeft()) {
                this.send(new ShardStoreRequest(amoCommand), server);
            }
            this.set(new ClientTimer(amoCommand, groupID), CLIENT_RETRY_MILLIS);
        }
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
            shardStoreReply = m;
            notify();
        }
    }

    // Your code here...
    private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
        ShardConfig sc = (ShardConfig) m.result().result();
        if (m != null && m.result() != null && currShardConfig.configNum() < sc.configNum()) {
            currShardConfig = sc;
        }
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        // Your code here...
        if (seqNum == t.command().sequenceNum() && shardStoreReply == null) {
            for (Address server : currShardConfig.groupInfo().get(t.groupID()).getLeft()) {
                this.send(new ShardStoreRequest(t.command()), server);
            }
            this.set(t, CLIENT_RETRY_MILLIS);
        }
    }

    private void onQueryTimer(QueryTimer t) {
        for (Address shardMaster : shardMasters()) {
            this.send(new PaxosRequest(new AMOCommand(new Query(-1), address(), -1)), shardMaster);
        }
        this.set(t, QUERY_RETRY_MILLIS);
    }

    private int getGroupIdForShard(int shardNum) {
        for (int groupID : currShardConfig.groupInfo().keySet()) {
            if (currShardConfig.groupInfo().get(groupID).getRight().contains(shardNum)) return groupID;
        }
        return -1;
    }

}
