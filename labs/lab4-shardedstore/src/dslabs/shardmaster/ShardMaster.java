package dslabs.shardmaster;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import dslabs.paxos.PaxosReply;
import dslabs.paxos.PaxosRequest;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;

@ToString
@EqualsAndHashCode
public final class ShardMaster implements Application {
    public static final int INITIAL_CONFIG_NUM = 0;

    private final int numShards;

    // Your code here...
    private HashMap<Integer, ShardConfig> shardConfigHistory = new HashMap<>();
    private int nextConfigNum = INITIAL_CONFIG_NUM;

    public ShardMaster(int numShards) {
        this.numShards = numShards;
    }

    public interface ShardMasterCommand extends Command {
    }

    @Data
    public static final class Join implements ShardMasterCommand {
        // a unique positive integer replica group identifier
        private final int groupId;
        // set of server addresses
        private final Set<Address> servers;
    }

    @Data
    public static final class Leave implements ShardMasterCommand {
        private final int groupId;
    }

    @Data
    public static final class Move implements ShardMasterCommand {
        private final int groupId;
        private final int shardNum;
    }

    @Data
    public static final class Query implements ShardMasterCommand {
        private final int configNum;

        @Override
        public boolean readOnly() {
            return true;
        }
    }

    public interface ShardMasterResult extends Result {
    }

    @Data
    public static final class Ok implements ShardMasterResult {
    }

    @Data
    public static final class Error implements ShardMasterResult {
    }

    @Data
    public static final class ShardConfig implements ShardMasterResult {
        private final int configNum;

        // groupId -> <group members, shard numbers>
        private final Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo;

    }


    @Override
    public Result execute(Command command) {
        if (command instanceof Join) {
            Join join = (Join) command;

            // Your code here...
            ShardMasterResult res = null;
            //  A. if that group already exists in the latest configuration.
            if (groupAlreadyExistInLatest(join.groupId())) return new Error();
            // B. if the first time
            else if (nextConfigNum == INITIAL_CONFIG_NUM) {
                Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo = new HashMap<>();
                Pair<Set<Address>, Set<Integer>> pair = Pair.of(setDeepCopy(join.servers()),
                        IntStream.rangeClosed(1, numShards).boxed().collect(Collectors.toSet()));
                groupInfo.put(join.groupId(), pair);
                shardConfigHistory.put(nextConfigNum, new ShardConfig(nextConfigNum, groupInfo));
                res = new Ok();
            } else {
//                int prevCN = reBalance() ? nextConfigNum : nextConfigNum - 1;
//                join(join, prevCN);
                joinHelper(join);
                res = new Ok();
            }
            nextConfigNum += 1;
            return res;
        }

        if (command instanceof Leave) {
            Leave leave = (Leave) command;

            // Your code here...
            ShardMasterResult res = null;
            if (!groupAlreadyExistInLatest(leave.groupId())) return new Error();
            else {
                leave(leave.groupId());
                res = new Ok();
            }
            nextConfigNum += 1;
            return res;
        }

        if (command instanceof Move) {
            Move move = (Move) command;

            // Your code here...
            // create a new configuration in which the shard is assigned to the
            // group and only that shard is moved from the previous configuration
            Map<Integer, Pair<Set<Address>, Set<Integer>>> latestGroupInfo =
                    shardConfigHistory.get(nextConfigNum - 1).groupInfo();
            if (!latestGroupInfo.containsKey(move.groupId())
                    || latestGroupInfo.get(move.groupId()).getRight().contains(move.shardNum())
                    || move.shardNum() == 0 || move.shardNum() > numShards) {
                return new Error();
            }
            else {
                Map<Integer, Pair<Set<Address>, Set<Integer>>> newGroupInfo = new HashMap<>();
                for (Integer groupID : latestGroupInfo.keySet()) {
                    Set<Integer> shardNums = latestGroupInfo.get(groupID).getRight();
                    Set<Integer> newSharedNums = new HashSet<>(shardNums);
                    if (groupID == move.groupId()) {
                        newSharedNums.add(move.shardNum());
                    } else if (shardNums.contains(move.shardNum())) {
                        newSharedNums.remove(move.shardNum());
                    }
                    newGroupInfo.put(groupID, Pair.of(setDeepCopy(latestGroupInfo.get(groupID).getLeft()), newSharedNums));
                }
                ShardConfig newShardConfig = new ShardConfig(nextConfigNum, newGroupInfo);
                shardConfigHistory.put(nextConfigNum, newShardConfig);
            }
            nextConfigNum += 1;
            return new Ok();
        }

        if (command instanceof Query) {
            Query query = (Query) command;

            // Your code here...
            if (shardConfigHistory.isEmpty()) return new Error();
            else if (query.configNum() == -1 || query.configNum() > nextConfigNum - 1) {
                return shardConfigHistory.get(nextConfigNum - 1);
            } else {
                return shardConfigHistory.get(query.configNum());
            }
        }

        throw new IllegalArgumentException();
    }

    // added to pass test7: sometimes it will be unbalance due to lots of moves
    private boolean reBalance() {
        boolean reBalanced = false;
        Map<Integer, Pair<Set<Address>, Set<Integer>>> latestGroupInfo = shardConfigHistory.get(nextConfigNum - 1).groupInfo;
        System.out.println(latestGroupInfo.toString());
        Map<Integer, Pair<Set<Address>, Set<Integer>>> newGroupInfo = new HashMap<>();
        int nextLoadPerGroup = numShards / latestGroupInfo.size();
        int left = numShards - nextLoadPerGroup * latestGroupInfo.size();
        Set<Integer> tmp = new HashSet<>();
        for (Integer groupID : latestGroupInfo.keySet()) {
            Set<Integer> shardNums = latestGroupInfo.get(groupID).getRight();
            Set<Integer> newSharedNums = new HashSet<>(shardNums);
            int actualNextLoadPerGroup = (left > 0 ? nextLoadPerGroup + 1 : nextLoadPerGroup);
            if (newSharedNums.size() > actualNextLoadPerGroup) {
                while (newSharedNums.size() > actualNextLoadPerGroup) {
                    Integer toTransfer = Collections.max(newSharedNums);
                    newSharedNums.remove(toTransfer);
                    tmp.add(toTransfer);
                    reBalanced = true;
                }
            }
            left--;
            newGroupInfo.put(groupID, Pair.of(setDeepCopy(latestGroupInfo.get(groupID).getLeft()), newSharedNums));
        }
        for (Integer groupID : newGroupInfo.keySet()) {
            Set<Integer> shardNums = newGroupInfo.get(groupID).getRight();
            Set<Integer> newSharedNums = new HashSet<>(shardNums);
            int actualNextLoadPerGroup = (left > 0 ? nextLoadPerGroup + 1 : nextLoadPerGroup);
            if (newSharedNums.size() < actualNextLoadPerGroup) {
                while (newSharedNums.size() < actualNextLoadPerGroup) {
                    Integer toTransfer = Collections.max(tmp);
                    tmp.remove(toTransfer);
                    newSharedNums.add(toTransfer);
                    reBalanced = true;
                }
            }
            left--;
            newGroupInfo.put(groupID, Pair.of(setDeepCopy(latestGroupInfo.get(groupID).getLeft()), newSharedNums));
        }
        ShardConfig nextShardConfig = new ShardConfig(nextConfigNum, newGroupInfo);
        shardConfigHistory.put(nextConfigNum, nextShardConfig);
        return reBalanced;
    }

    private void join(Join joinCommand, int prevCN) {
        Map<Integer, Pair<Set<Address>, Set<Integer>>> latestGroupInfo = shardConfigHistory.get(prevCN).groupInfo;
        Map<Integer, Pair<Set<Address>, Set<Integer>>> newGroupInfo = new HashMap<>();
        int nextLoadPerGroup = numShards / (latestGroupInfo.size() + 1);
        int left = numShards - nextLoadPerGroup * (latestGroupInfo.size() + 1);
        Set<Integer> newGroupShardNums = new HashSet<>();
        for (Integer groupID : latestGroupInfo.keySet()) {
            Set<Integer> shardNums = latestGroupInfo.get(groupID).getRight();
            Set<Integer> newSharedNums = new HashSet<>(shardNums);
            int actualNextLoadPerGroup = (left > 0 ? nextLoadPerGroup + 1 : nextLoadPerGroup);
            while (newSharedNums.size() > actualNextLoadPerGroup) {
                Integer toTransfer = Collections.max(newSharedNums);
                newSharedNums.remove(toTransfer);
                newGroupShardNums.add(toTransfer);
            }
            left--;
            newGroupInfo.put(groupID, Pair.of(setDeepCopy(latestGroupInfo.get(groupID).getLeft()), newSharedNums));
        }
        newGroupInfo.put(joinCommand.groupId(), Pair.of(setDeepCopy(joinCommand.servers()), newGroupShardNums));
        ShardConfig nextShardConfig = new ShardConfig(nextConfigNum, newGroupInfo);
        shardConfigHistory.put(nextConfigNum, nextShardConfig);
    }

    private void joinHelper(Join joinCommand) {
        Map<Integer, Pair<Set<Address>, Set<Integer>>> latestGroupInfo = shardConfigHistory.get(nextConfigNum - 1).groupInfo;
        //System.out.println(latestGroupInfo.toString());
        Map<Integer, Pair<Set<Address>, Set<Integer>>> newGroupInfo = new HashMap<>();
        int nextAverageLoad = numShards / (latestGroupInfo.size() + 1);
        int reminder = numShards % (latestGroupInfo.size() + 1);
        HashMap<Integer, Integer> groupIdToNumShards = new HashMap<>();
        HashSet<Integer> toBeAssign = new HashSet<>();

        for (int groupId : latestGroupInfo.keySet()) {
            int actualNextLoad = (reminder > 0 ? nextAverageLoad + 1 : nextAverageLoad);
            groupIdToNumShards.put(groupId, actualNextLoad);
            reminder--;

            Set<Integer> shardNums = latestGroupInfo.get(groupId).getRight();
            Set<Integer> newSharedNums = new HashSet<>(shardNums);
            while (newSharedNums.size() > actualNextLoad) {
                Integer toTransfer = Collections.max(newSharedNums);
                newSharedNums.remove(toTransfer);
                toBeAssign.add(toTransfer);
            }
            newGroupInfo.put(groupId, Pair.of(setDeepCopy(latestGroupInfo.get(groupId).getLeft()), newSharedNums));
        }
        int actualNextLoad = (reminder > 0 ? nextAverageLoad + 1 : nextAverageLoad);
        groupIdToNumShards.put(joinCommand.groupId(), actualNextLoad);
        newGroupInfo.put(joinCommand.groupId(), Pair.of(setDeepCopy(joinCommand.servers()), new HashSet<>()));


        for (int groupId : groupIdToNumShards.keySet()) {
            while (groupIdToNumShards.get(groupId) > newGroupInfo.get(groupId).getRight().size()) {
                Integer toTransfer = Collections.max(toBeAssign);
                newGroupInfo.get(groupId).getRight().add(toTransfer);
                toBeAssign.remove(toTransfer);
            }
        }

        //System.out.println(newGroupInfo.toString());
        ShardConfig nextShardConfig = new ShardConfig(nextConfigNum, newGroupInfo);
        shardConfigHistory.put(nextConfigNum, nextShardConfig);
    }

    private void leave(int leaveGroupID) {
        Map<Integer, Pair<Set<Address>, Set<Integer>>> latestGroupInfo = shardConfigHistory.get(nextConfigNum - 1).groupInfo;
        Map<Integer, Pair<Set<Address>, Set<Integer>>> newGroupInfo = new HashMap<>();
        int nextLoadPerGroup = numShards / (latestGroupInfo.size() - 1);
        int left = numShards - nextLoadPerGroup * (latestGroupInfo.size() - 1);
        Set<Integer> groupToLeave = latestGroupInfo.get(leaveGroupID).getRight();
        Set<Integer> copyOfGroupToLeave = new HashSet<>(groupToLeave);
        for (Integer groupID : latestGroupInfo.keySet()) {
            Set<Integer> shardNums = latestGroupInfo.get(groupID).getRight();
            Set<Integer> newSharedNums = new HashSet<>(shardNums);
            int actualNextLoadPerGroup = (left > 0 ? nextLoadPerGroup + 1 : nextLoadPerGroup);
            if (groupID != leaveGroupID) {
                while (newSharedNums.size() < actualNextLoadPerGroup) {
                    Integer shardToTransfer = Collections.max(copyOfGroupToLeave);
                    copyOfGroupToLeave.remove(shardToTransfer);
                    newSharedNums.add(shardToTransfer);
                }
                newGroupInfo.put(groupID, Pair.of(setDeepCopy(latestGroupInfo.get(groupID).getLeft()), newSharedNums));
            }
            left--;
        }
        ShardConfig nextShardConfig = new ShardConfig(nextConfigNum, newGroupInfo);
        shardConfigHistory.put(nextConfigNum, nextShardConfig);
    }

    private boolean groupAlreadyExistInLatest(int joinGroupID) {
        if (shardConfigHistory.keySet().isEmpty()) return false;
        if (shardConfigHistory.containsKey(nextConfigNum - 1)) {
            if (shardConfigHistory.get(nextConfigNum - 1).groupInfo.keySet().contains(joinGroupID)) return true;
        }
        return false;
    }

    private HashSet<Address> setDeepCopy(Set<Address> from) {
        HashSet<Address> to = new HashSet<>();
        for (Address o : from) to.add(o);
        return to;
    }
}
