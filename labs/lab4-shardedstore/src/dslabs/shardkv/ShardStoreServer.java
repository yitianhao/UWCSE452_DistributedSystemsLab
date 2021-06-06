package dslabs.shardkv;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Command;
import dslabs.kvstore.KVStore;
import dslabs.kvstore.KVStore.SingleKeyCommand;
import dslabs.paxos.PaxosReply;
import dslabs.paxos.PaxosRequest;
import dslabs.paxos.PaxosServer;
import dslabs.shardmaster.ShardMaster.Query;
import dslabs.shardmaster.ShardMaster.ShardConfig;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.shardkv.QueryTimer.QUERY_RETRY_MILLIS;
import static dslabs.shardmaster.ShardMaster.INITIAL_CONFIG_NUM;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreServer extends ShardStoreNode {
    private final Address[] group;
    private final int groupId;

    // Your code here...
    private static final String PAXOS_ADDRESS_ID = "paxos";
    public static final int DUMMY_SEQ_NUM = -2;
    private Address paxosAddress;
    private Map<Integer, AMOApplication> shardToApplication = new HashMap<>();
    private boolean inReConfig = false;
    private ShardConfig currShardConfig = new ShardConfig(DUMMY_SEQ_NUM, new HashMap<>());

    /* -------------------------------------------------------------------------
        Construction and initialization
       -----------------------------------------------------------------------*/
    ShardStoreServer(Address address, Address[] shardMasters, int numShards,
                     Address[] group, int groupId) {
        super(address, shardMasters, numShards);
        this.group = group;
        this.groupId = groupId;

        // Your code here...
    }

    @Override
    public void init() {
        // Setup Paxos
        paxosAddress = Address.subAddress(address(), PAXOS_ADDRESS_ID);

        Address[] paxosAddresses = new Address[group.length];
        for (int i = 0; i < paxosAddresses.length; i++) {
            paxosAddresses[i] = Address.subAddress(group[i], PAXOS_ADDRESS_ID);
        }

        PaxosServer paxosServer = new PaxosServer(paxosAddress, paxosAddresses, address());
        addSubNode(paxosServer);
        paxosServer.init();

        broadcastToShardMasters(new PaxosRequest(new AMOCommand(new Query(-1), address(), DUMMY_SEQ_NUM)));
        this.set(new QueryTimer(), QUERY_RETRY_MILLIS);
    }



    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    // NOTE: When a ShardStoreServer has received one of these messages,
    // it CANNOT act on them until they are replicated (unless app has executed already,
    // in which case it’s still safe to reply, as before)
    // ShardStoreRequest from clients
    private void handleShardStoreRequest(ShardStoreRequest m, Address sender) {
        // Your code here...
        if (!inReConfig && currShardConfig.configNum() >= INITIAL_CONFIG_NUM) {
            process(m.command(), false);
        }
    }

    // Your code here...
    // TODO
    // Receive PaxosReply from the ShardMaster, informing about the new configs
    private void handlePaxosReply(PaxosReply m, Address sender) {
        if (m.result().result() instanceof ShardConfig && (((ShardConfig) m.result().result()).configNum() > currShardConfig.configNum())) {
            inReConfig = true;
            process(new NewConfig((ShardConfig) m.result().result()), false);
        }
    }

    // TODO
    // Receive PaxosDecision from Paxos sub-nodes
    private void handlePaxosDecision(PaxosDecision m, Address sender) {
        if (m.amoCommand().sequenceNum() >= 0) {
            processAMOCommand(m.amoCommand(), true);
        } else {
            process(m.amoCommand().command(), true);
        }
    }

    // TODO
    // Receive from other ShardStoreServer's
    private void handleShardMoveMsg(ShardMoveMsg m, Address sender) {
        process(m.shardMoveCommand(), false);
    }

    // TODO
    // Receive from other ShardStoreServer's
    private void handleShardMoveAckMsg(ShardMoveAckMsg m, Address sender) {
        process(m.shardMoveAckCommand(), false);
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    // Your code here...
    private void onQueryTimer(QueryTimer t) {
        broadcastToShardMasters(new PaxosRequest(new AMOCommand(new Query(-1), address(), DUMMY_SEQ_NUM)));
        this.set(t, QUERY_RETRY_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...
    private void process(Command command, boolean replicated) {
        if (command instanceof ShardMove) {
            processShardMove((ShardMove) command, replicated);
        } else if (command instanceof ShardMoveAck) {
            processShardMoveAck((ShardMoveAck) command, replicated);
        } else if (command instanceof NewConfig) {
            processNewConfig((NewConfig) command, replicated);
        } else if (command instanceof AMOCommand) {
            processAMOCommand((AMOCommand)command, replicated);
        }

        // Add cases for Lab 4 Part 3
//        else {
//            LOG.severe("Got unknown command: " + command);
//        }
    }

    private void processShardMove(ShardMove sm, boolean replicated) {

    }

    private void processShardMoveAck(ShardMoveAck sma, boolean replicated) {

    }

    private void processNewConfig(NewConfig nc, boolean replicated) {
        if (!replicated) {
            paxosPropose(nc);
            return;
        }
        currShardConfig = nc.shardConfig;
        // first time
        for (int shardNum : currShardConfig.groupInfo().get(groupId).getRight()) {
            AMOApplication app = new AMOApplication(new KVStore());
            shardToApplication.put(shardNum, app);
        }
        inReConfig = false;
    }

    private void paxosPropose(Command command) {
        handleMessage(new PaxosRequest(
                command instanceof AMOCommand ? (AMOCommand) command : new AMOCommand(command, address(), DUMMY_SEQ_NUM)),
                paxosAddress);
    }

    private void processAMOCommand(AMOCommand amoCommand, boolean replicated) {
        if (!replicated) {
            paxosPropose(amoCommand);
            return;
        }
        int shardNum = keyToShard(((SingleKeyCommand)amoCommand.command()).key());
        //System.out.println("key = " + ((SingleKeyCommand)amoCommand.command()).key() + " | shard = " + shardNum);
        AMOApplication app = shardToApplication.get(shardNum);
        AMOResult result = app.execute(amoCommand);
        shardToApplication.put(shardNum, app);
        this.send(new ShardStoreReply(result), amoCommand.clientID());
    }

    @Data
    final class ShardMove implements Command {

    }

    @Data
    final class ShardMoveAck implements Command {

    }

    @Data
    public static final class NewConfig implements Command {
//        private final int configNum;
//
//        // groupId -> <group members, shard numbers>
//        private final Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo;
        private final ShardConfig shardConfig;

    }

}
