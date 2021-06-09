package dslabs.shardkv;

import com.google.common.collect.Sets;
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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.shardkv.QueryTimer.QUERY_RETRY_MILLIS;
import static dslabs.shardkv.ShardMoveTimer.SHARD_MOVE_RETRY_MILLIS;
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

    // reconfiguration
    // see slide P21 bottom
    private boolean inReConfig = false;
    private ShardConfig currShardConfig = new ShardConfig(DUMMY_SEQ_NUM, new HashMap<>());
    private HashSet<Integer> shardsOwned = new HashSet<>();
    private HashSet<Integer> shardsNeeded = new HashSet<>();
    private HashMap<Integer, HashSet<Integer>> shardToMove = new HashMap<>();

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

        broadcastToShardMasters(new PaxosRequest(new AMOCommand(new Query(currShardConfig.configNum() + 1), address(), DUMMY_SEQ_NUM)));
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
        } else {
//            System.out.println(inReConfig);
//            System.out.println("currShardConfig.configNum() >= INITIAL_CONFIG_NUM = " + (currShardConfig.configNum() >= INITIAL_CONFIG_NUM));
//            System.out.println(m);
        }
    }

    // Your code here...
    // TODO
    // Receive PaxosReply from the ShardMaster, informing about the new configs
    private void handlePaxosReply(PaxosReply m, Address sender) {
//        if (inReConfig && m.result().result() instanceof ShardConfig
//                && (((ShardConfig) m.result().result()).configNum() > currShardConfig.configNum())) {
//            System.out.println(((ShardConfig) m.result().result()));
//        }
        // System.out.println(m.result());
        if (m.result().result() instanceof ShardConfig
                && (((ShardConfig) m.result().result()).configNum() > currShardConfig.configNum())
                && !inReConfig) {
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
        // if (this.groupId == 2) System.out.println(shardToApplication);
        broadcastToShardMasters(new PaxosRequest(new AMOCommand(new Query(currShardConfig.configNum() + 1), address(), DUMMY_SEQ_NUM)));
        this.set(t, QUERY_RETRY_MILLIS);
    }

    private void onShardMoveTimer(ShardMoveTimer t) {
        // Retry sending these shards until you receive an Ack
        if (shardToMove.containsKey(t.destGroupId())) {
            ShardMove sm = new ShardMove(this.groupId, t.destGroupId(), currShardConfig.configNum(), t.shardsAndApp());
            broadcast(new ShardMoveMsg(sm),currShardConfig.groupInfo().get(t.destGroupId()).getLeft());
            this.set(t, SHARD_MOVE_RETRY_MILLIS);
        }
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
        if (sm.configNum != currShardConfig.configNum()) return;

        if (!replicated) {
            paxosPropose(sm);
            return;
        }

        for (int n : sm.shardsAndApp.keySet()) {
            shardsNeeded.remove(n);
            shardsOwned.add(n);
            shardToApplication.put(n, sm.shardsAndApp.get(n));
        }

        ShardMoveAck sma = new ShardMoveAck(sm.startGroupId, sm.destGroupId, sm.configNum);
        broadcast(new ShardMoveAckMsg(sma), currShardConfig.groupInfo().get(sm.startGroupId).getLeft());

        if (shardToMove.isEmpty() && shardsNeeded.isEmpty()) inReConfig = false;
    }

    private void processShardMoveAck(ShardMoveAck sma, boolean replicated) {
        if (sma.configNum != currShardConfig.configNum()) return;

        if (!replicated) {
            paxosPropose(sma);
            return;
        }

        shardToMove.remove(sma.destGroupId);

        if (shardToMove.isEmpty() && shardsNeeded.isEmpty()) inReConfig = false;
    }

    private void processNewConfig(NewConfig nc, boolean replicated) {
        if (!nc.shardConfig.groupInfo().containsKey(groupId)) return;

        if (!replicated) {
            paxosPropose(nc);
            return;
        }

        // first time
        if (nc.shardConfig.configNum() == INITIAL_CONFIG_NUM) {
            for (int shardNum : nc.shardConfig.groupInfo().get(groupId).getRight()) {
                AMOApplication app = new AMOApplication(new KVStore());
                shardToApplication.put(shardNum, app);
            }
            inReConfig = false;
        } else {
            processNewConfigHelper(nc);
            // Send Shards to the Paxos Replica Group responsible for the Shard in the new configuration
            for (Integer destGroupId : shardToMove.keySet()) {
                HashMap<Integer, AMOApplication> shardsAndApp = new HashMap<>();
                for (Integer shard : shardToMove.get(destGroupId)) {
                    shardsAndApp.put(shard, shardToApplication.get(shard));
                    shardToApplication.remove(shard);
                }
                ShardMove sm = new ShardMove(this.groupId, destGroupId, nc.shardConfig.configNum(), shardsAndApp);
                broadcast(new ShardMoveMsg(sm), nc.shardConfig.groupInfo().get(destGroupId).getLeft());
                this.set(new ShardMoveTimer(destGroupId, nc.shardConfig.configNum(), shardsAndApp), SHARD_MOVE_RETRY_MILLIS);
            }
        }
        currShardConfig = nc.shardConfig;
    }

    private void processNewConfigHelper(NewConfig nc) {
        if (!currShardConfig.groupInfo().containsKey(groupId)) return;

        // clear out previous data
        HashSet<Integer> prevShardsOwned = new HashSet<>(shardsOwned);
        shardsOwned = new HashSet<>();
        shardToMove = new HashMap<>();
        shardsNeeded = new HashSet<>();

        Set<Integer> currShouldOwned = nc.shardConfig.groupInfo().get(groupId).getRight();
        Set<Integer> prevOwned = currShardConfig.groupInfo().get(groupId).getRight();
        for (Integer n : currShouldOwned) {
            // previous owned && current should owned
            if (prevOwned.contains(n)) shardsOwned.add(n);
            // previous not owned && current should owned
            else shardsNeeded.add(n);
        }

        // previous owned && current should not owned, specify to its destinations
        Set<Integer> toMove = Sets.difference(prevShardsOwned, shardsOwned); // shards currently not owned
        for (int groupId : nc.shardConfig.groupInfo().keySet()) {
            if (groupId != this.groupId) { // for all other groups
                // all its needs
                Set<Integer> totalShardNeededForGroupId = nc.shardConfig.groupInfo().get(groupId).getRight();
                // needed shards that can be moved from here
                HashSet<Integer> shardNeededForGroupId = new HashSet<>();
                for (int n : toMove) {
                    if (totalShardNeededForGroupId.contains(n)) {
                        shardNeededForGroupId.add(n);
                    }
                }
                if (!shardNeededForGroupId.isEmpty()) shardToMove.put(groupId, shardNeededForGroupId);
            }
        }
    }

    private void paxosPropose(Command command) {
        handleMessage(new PaxosRequest(
                command instanceof AMOCommand ? (AMOCommand) command : new AMOCommand(command, address(), DUMMY_SEQ_NUM)),
                paxosAddress);
    }

    private void processAMOCommand(AMOCommand amoCommand, boolean replicated) {
        if (!canServeAMOCommand(amoCommand.command())) return;

        if (!replicated) {
            paxosPropose(amoCommand);
            return;
        }
        int shardNum = keyToShard(((SingleKeyCommand)amoCommand.command()).key());
        AMOApplication app = shardToApplication.get(shardNum);
        AMOResult result = app.execute(amoCommand);
        shardToApplication.put(shardNum, app);
        this.send(new ShardStoreReply(result), amoCommand.clientID());
    }

    private boolean canServeAMOCommand(Command command) {
        return command instanceof SingleKeyCommand
                && currShardConfig.groupInfo().containsKey(groupId)
                && currShardConfig.groupInfo().get(groupId).getRight().contains(keyToShard(((SingleKeyCommand) command).key()))
                && shardToApplication.containsKey(keyToShard(((SingleKeyCommand)command).key()));
    }

    @Data
    final class ShardMove implements Command {
        private final Integer startGroupId;
        private final Integer destGroupId;
        private final Integer configNum;
        private final HashMap<Integer, AMOApplication> shardsAndApp;
    }

    @Data
    final class ShardMoveAck implements Command {
        private final Integer startGroupId;
        private final Integer destGroupId;
        private final Integer configNum;
    }

    @Data
    public static final class NewConfig implements Command {
        private final ShardConfig shardConfig;

    }

}
