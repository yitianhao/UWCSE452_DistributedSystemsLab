package dslabs.shardkv;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Command;
import dslabs.framework.Message;
import dslabs.framework.Result;
import dslabs.paxos.PaxosReply;
import dslabs.paxos.PaxosRequest;
import dslabs.paxos.PaxosServer;
import dslabs.shardmaster.ShardMaster.Query;
import dslabs.shardmaster.ShardMaster.ShardConfig;
import dslabs.shardmaster.ShardMaster.ShardMasterResult;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;

import static dslabs.shardkv.QueryTimer.QUERY_RETRY_MILLIS;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreServer extends ShardStoreNode {
    private final Address[] group;
    private final int groupId;

    // Your code here...
    private static final String PAXOS_ADDRESS_ID = "paxos";
    private Address paxosAddress;

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

        for (Address shardMaster : shardMasters()) {
            this.send(new PaxosRequest(new AMOCommand(new Query(-1), address(), -1)), shardMaster);
        }
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
        process(m.command(), false);
    }

    // Your code here...
    // TODO
    // Receive PaxosReply from the ShardMaster, informing about the new configs
    private void handlePaxosReply(PaxosReply m, Address sender) {
        process(new NewConfig((ShardConfig)(Result) m.result()), false);
    }

    // TODO
    // Receive PaxosDecision from Paxos sub-nodes
    private void handlePaxosDecision(PaxosDecision m, Address sender) {
        process(m.command(), true);
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
        for (Address shardMaster : shardMasters()) {
            this.send(new PaxosRequest(new AMOCommand(new Query(-1), address(), -1)), shardMaster);
        }
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

    }

    private void processAMOCommand(AMOCommand amoCommand, boolean replicated) {

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
