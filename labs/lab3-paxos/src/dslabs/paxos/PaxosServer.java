package dslabs.paxos;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Message;
import dslabs.framework.Node;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore.SingleKeyCommand;
import dslabs.shardkv.PaxosDecision;
import dslabs.shardmaster.ShardMaster.Query;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.paxos.HeartbeatCheckTimer.HEARTBEAT_CHECK_MILLIS;
import static dslabs.paxos.HeartbeatTimer.HEARTBEAT_MILLIS;
import static dslabs.paxos.P1ATimer.P1A_RETRY_TIMER;
import static dslabs.paxos.P2ATimer.P2A_RETRY_TIMER;
import static dslabs.shardkv.ShardStoreServer.DUMMY_SEQ_NUM;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class PaxosServer extends Node {
    private final Address[] servers;
    private final Address shardStoreServer;

    // Your code here...
    private final AMOApplication<Application> application;
    private HashMap<Integer, LogEntry> log;  // logNum -> LogEntry
    private boolean leader;
    private int slot_out;
    private int slot_in;
    private int seqNum;
    private Ballot ballot;
    private HashMap<Integer, HashSet<Address>> receivedP2BFrom;
    private boolean heartbeatReceivedThisInterval;
    private HashSet<Address> receivedPositiveP1BFrom;
    private boolean stopP1ATimer;
    private Address lastLeader;
    private HeartbeatCheckTimer timer;
    private HashSet<HashMap<Integer, LogEntry>> receivedLogs;
    private boolean heartbeatReplyReceivedThisInterval;
    private HashMap<Address, Integer> garbageCollection;
    private int firstNonCleared;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosServer(Address address, Address[] servers, Application app) {
        super(address);
        this.servers = servers;
        shardStoreServer = null;

        // Your code here...
        this.application = new AMOApplication<>(app);
        this.log = new HashMap<>();
        slot_in = 1;
        slot_out = 1;
        seqNum = -1;
        ballot = new Ballot(seqNum, address);
        receivedP2BFrom = new HashMap<>();
        stopP1ATimer = false;
        timer = new HeartbeatCheckTimer();
        receivedLogs = new HashSet<>();
        garbageCollection = new HashMap<>();
        firstNonCleared = 1;
    }

    // For lab4-2
    public PaxosServer(Address address, Address[] servers, Address shardStoreServer) {
        super(address);
        this.servers = servers;
        this.shardStoreServer = shardStoreServer;
        this.application = null;

        // Your code here...
        this.log = new HashMap<>();
        slot_in = 1;
        slot_out = 1;
        seqNum = -1;
        ballot = new Ballot(seqNum, address);
        receivedP2BFrom = new HashMap<>();
        stopP1ATimer = false;
        timer = new HeartbeatCheckTimer();
        receivedLogs = new HashSet<>();
        garbageCollection = new HashMap<>();
        firstNonCleared = 1;
    }

    @Override
    public void init() {
        // Your code here...
        if (servers.length == 1) {
            leader = true;
        } else {
            for (Address server : servers) {
                garbageCollection.put(server, 0);
            }
            startLeaderElection();
        }
        this.set(timer, HEARTBEAT_CHECK_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Interface Methods

        Be sure to implement the following methods correctly. The test code uses
        them to check correctness more efficiently.
       -----------------------------------------------------------------------*/

    /**
     * Return the status of a given slot in the server's local log.
     *
     * If this server has garbage-collected this slot, it should return {@link
     * PaxosLogSlotStatus#CLEARED} even if it has previously accepted or chosen
     * command for this slot. If this server has both accepted and chosen a
     * command for this slot, it should return {@link PaxosLogSlotStatus#CHOSEN}.
     *
     * Log slots are numbered starting with 1.
     *
     * @param logSlotNum
     *         the index of the log slot
     * @return the slot's status
     *
     * @see PaxosLogSlotStatus
     */
    public PaxosLogSlotStatus status(int logSlotNum) {
        // Your code here...
        if (logSlotNum < firstNonCleared()) {
            return PaxosLogSlotStatus.CLEARED;
        }
        return log.getOrDefault(logSlotNum, new LogEntry(null, PaxosLogSlotStatus.EMPTY, null, null)).paxosLogSlotStatus;
    }

    /**
     * Return the command associated with a given slot in the server's local
     * log.
     *
     * If the slot has status {@link PaxosLogSlotStatus#CLEARED} or {@link
     * PaxosLogSlotStatus#EMPTY}, this method should return {@code null}.
     * Otherwise, return the command this server has chosen or accepted,
     * according to {@link PaxosServer#status}.
     *
     * If clients wrapped commands in {@link dslabs.atmostonce.AMOCommand}, this
     * method should unwrap them before returning.
     *
     * Log slots are numbered starting with 1.
     *
     * @param logSlotNum
     *         the index of the log slot
     * @return the slot's contents or {@code null}
     *
     * @see PaxosLogSlotStatus
     */
    public Command command(int logSlotNum) {
        // Your code here...
        if (!log.containsKey(logSlotNum) || logSlotNum < firstNonCleared()) {
            return null;
        } else if (log.get(logSlotNum).paxosLogSlotStatus == PaxosLogSlotStatus.CHOSEN
                || log.get(logSlotNum).paxosLogSlotStatus == PaxosLogSlotStatus.ACCEPTED) {
            return log.get(logSlotNum).command == null ? null : log.get(logSlotNum).command.command();
        } else {
            return null;
        }
    }

    /**
     * Return the index of the first non-cleared slot in the server's local log.
     * The first non-cleared slot is the first slot which has not yet been
     * garbage-collected. By default, the first non-cleared slot is 1.
     *
     * Log slots are numbered starting with 1.
     *
     * @return the index in the log
     *
     * @see PaxosLogSlotStatus
     */
    public int firstNonCleared() {
        // Your code here...
        return this.firstNonCleared;
        //return 1;
    }

    /**
     * Return the index of the last non-empty slot in the server's local log,
     * according to the defined states in {@link PaxosLogSlotStatus}. If there
     * are no non-empty slots in the log, this method should return 0.
     *
     * Log slots are numbered starting with 1.
     *
     * @return the index in the log
     *
     * @see PaxosLogSlotStatus
     */
    public int lastNonEmpty() {
        // Your code here...
        int last = slot_in - 1;
        while (status(last) == PaxosLogSlotStatus.EMPTY && last > 0) {
            last--;
        }
        return last;
    }



    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handlePaxosRequest(PaxosRequest m, Address sender) {
        // Your code here...
        // As a non-leader, need to drop client request
        if (application != null && m.amoCommand().command().readOnly()) {
            Result result = application.executeReadOnly(m.amoCommand().command());
            send(new PaxosReply(new AMOResult(result, m.amoCommand().sequenceNum())), sender);
            return;
        }
        if (application != null && application.alreadyExecuted(m.amoCommand())) {
            AMOResult result = application.execute(m.amoCommand());
            if (result.sequenceNum() == m.amoCommand().sequenceNum()) {
                send(new PaxosReply(result), sender);
            }
        } else if (leader && !oldRequest(sender, m.amoCommand().sequenceNum())) {
            if (servers.length == 1) {
                log.put(slot_in, new LogEntry(ballot, PaxosLogSlotStatus.CHOSEN, m.amoCommand(), null));
                executeChosen();
            } else {
                LogEntry logEntry = new LogEntry(ballot, PaxosLogSlotStatus.ACCEPTED, m.amoCommand(), null);
                log.put(slot_in, logEntry);
                sendMsgExceptSelf(new P2A(ballot, m.amoCommand(), slot_in));
                set(new P2ATimer(slot_in, m.amoCommand(), ballot), P2A_RETRY_TIMER);
            }
            updateSlotIn();
        }
    }

    // Your code here...
    // -----------acceptors------------
    private void handleP2A(P2A m, Address sender) {
        // only accept it if the ballot in the message matches the acceptor’s ballot,
        // which means the acceptor considers the sender to be the current leader
        if (chosen(log, m.slotNum())) return;
        if (!leader) {
            if (m.ballot().compareTo(ballot) >= 0) {
                heardFromLeader(m.ballot(), sender);
                ballot = m.ballot();
                log.put(m.slotNum(), new LogEntry(ballot, PaxosLogSlotStatus.ACCEPTED, m.command(), null));
                send(new P2B(ballot, m.slotNum()), sender);
                updateSlotIn();
            }
        } else {
            checkLeaderValidity(m.ballot());
        }
    }


    // ---------------leader--------------
    private void handleP2B(P2B m, Address sender) {
        if (leader && !chosen(log, m.slotNum()) && m.ballot().compareTo(ballot) >= 0) {
            checkLeaderValidity(m.ballot());
            HashSet<Address> addresses = receivedP2BFrom.getOrDefault(m.slotNum(), new HashSet<>());
            addresses.add(sender);
            receivedP2BFrom.put(m.slotNum(), addresses);
            if (receivedP2BFrom.getOrDefault(m.slotNum(), new HashSet<>()).size() >= servers.length / 2) {
                log.put(m.slotNum(), new LogEntry(m.ballot(), PaxosLogSlotStatus.CHOSEN, log.get(m.slotNum()).command, null));
                executeChosen();
                garbageCollection.put(address(), slot_out);
                updateSlotIn();
            }
        }
    }

    // ---------acceptors---------
    private void handleHeartbeat(Heartbeat m, Address sender) {
        if (leader) {
            if (m.ballot().compareTo(ballot) > 0) {
                leader = false;
            } else {
                return;
            }
            //
        }
        if (m.ballot().compareTo(ballot) < 0) return;
        heardFromLeader(m.ballot(), sender);
        clearGarbage(Collections.min(m.garbageCollection().values()));
        mergeLog(m.log());
        updateSlotIn();
        executeChosen();
        send(new HeartbeatReply(slot_out), sender);
    }

    private void handleHeartbeatReply(HeartbeatReply m, Address sender) {
        if (leader) {
            heartbeatReplyReceivedThisInterval = true;
            garbageCollection.put(sender, m.slotOut());
            clearGarbage(Collections.min(garbageCollection.values()));
        }
    }

    private void clearGarbage(int minSlotOut) {
        int minSlotNotCleared = log.keySet().size() > 0 ? Collections.min(log.keySet()) : 1;
        for (; minSlotNotCleared < minSlotOut; minSlotNotCleared++) {
            // send back to ShardStoreServer
//            if (log.get(minSlotNotCleared) != null) {
//                handleMessage(new PaxosDecision(log.get(minSlotNotCleared).command), shardStoreServer);
//            } else {
//                System.out.println("NULL!!!!");
//            }
//            System.out.println(log.get(minSlotNotCleared).toString());
            log.remove(minSlotNotCleared);
        }
        firstNonCleared = Math.max(firstNonCleared, minSlotOut);
    }

    // ---------potential acceptors--------
    private void handleP1A(P1A m, Address sender) {
        if (m.ballot().compareTo(ballot) >= 0) {
            ballot = m.ballot();
            leader = false;
            send(new P1B(ballot, log), sender);
        }
    }

    // --------potential leader--------
    private void handleP1B(P1B m, Address sender) {
        if (leader) {
            checkLeaderValidity(m.ballot());
            return;
        }
        if (m.ballot().compareTo(ballot) < 0) {
            return;
        }

        receivedPositiveP1BFrom.add(sender);
        receivedLogs.add(m.log());

        if (receivedPositiveP1BFrom.size() >= servers.length / 2) {
            leader = true;
            //roleSettled = true;
            mergeLog();
            updateSlotIn();
            executeChosen();
            garbageCollection.put(address(), slot_out);
            receivedP2BFrom = new HashMap<>();
            receivedLogs = new HashSet<>();
            sendAcceptedP2A();
            beginHeartbeat();
//            this.set(new HeartbeatReplyCheckTimer(), HEARTBEAT_REPLY_CHECK_MILLIS);
        }
    }


    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    // Your code here...
    private void onP2ATimer(P2ATimer t) {
        if (leader && t.ballot().compareTo(ballot) == 0) {
            if (!chosen(log, t.slotNum())) {
                sendMsgExceptSelf(new P2A(t.ballot(), t.command(), t.slotNum()));
                set(t, P2A_RETRY_TIMER);
            }
        }
    }

    private void onP1ATimer(P1ATimer t) {
        if (!leader && !stopP1ATimer && ballot.compareTo(t.ballot()) == 0) {
            sendMsgExceptSelf(new P1A(t.ballot()));
            set(t, P1A_RETRY_TIMER);
        }
    }

    private void onHeartbeatTimer(HeartbeatTimer t) {
        // Your code here...
        if (leader && ballot.compareTo(t.ballot()) == 0) {
            sendMsgExceptSelf(new Heartbeat(log, ballot, garbageCollection));
            this.set(t, HEARTBEAT_MILLIS);
        }
    }

    private void onHeartbeatCheckTimer(HeartbeatCheckTimer t) {
        if (!leader) {
            if (heartbeatReceivedThisInterval) {
                heartbeatReceivedThisInterval = false;
                this.set(t, HEARTBEAT_CHECK_MILLIS);
            } else { //if (roleSettled) {
                // try to be leader
                startLeaderElection();
            }
        }
    }


    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...
    private void sendMsgExceptSelf(Message m) {
        for (Address otherServer : servers) {
            if (!Objects.equals(address(), otherServer)) {
                send(m, otherServer);
            }
        }
    }

    private void executeChosen() {
        int i = slot_out;
        while (log.containsKey(i) && chosen(log, i)) {
            AMOCommand amoCommand = log.get(i).command;
            if (amoCommand != null) {  // in the case of no-op
                if (this.application != null) {
                    //System.out.println(address() + " | " + i);
                    AMOResult result = application.execute(amoCommand);
//                    if (amoCommand.sequenceNum() >= 0) {
//                        System.out.println(application);
//                        System.out.println(amoCommand);
//                        System.out.println(result.toString());
//                        System.out.println();
//                    }
//                    if (amoCommand.command() instanceof Query) {
//                        if (amoCommand.clientID().toString().startsWith("server")) {
//                            System.out.println("SERVER!");
//                            System.out.println(amoCommand);
//                            System.out.println(result.toString());
//                        } else if (amoCommand.clientID().toString().startsWith("client")) {
//                            System.out.println("CLIENT!");
//                            System.out.println(amoCommand);
//                            System.out.println(result.toString());
//                        }
//                    }
                    send(new PaxosReply(result), amoCommand.clientID());
                } else {
                    handleMessage(new PaxosDecision(amoCommand), shardStoreServer);
                }
                // update result
                log.put(i, new LogEntry(log.get(i).ballot, PaxosLogSlotStatus.CHOSEN, log.get(i).command, null));
            }
            i++;
        }
        slot_out = i;
    }

    private void startLeaderElection() {
        seqNum = ballot.seqNum + 1;
        ballot = new Ballot(seqNum, address());
        stopP1ATimer = false;
        receivedPositiveP1BFrom = new HashSet<>();
        sendMsgExceptSelf(new P1A(ballot));
        set(new P1ATimer(ballot), P1A_RETRY_TIMER);
        //roleSettled = false;
    }

    // To merge the current log with the incoming log
    private void mergeLog(HashMap<Integer, LogEntry> other) {
        int maxSlotNum = Math.max(log.keySet().size() > 0 ? Collections.max(log.keySet()) : 0, other.keySet().size() > 0 ? Collections.max(other.keySet()) : 0);
        for (int i = slot_out; i <= maxSlotNum; i++) {
            if (other.containsKey(i) && chosen(other, i)) {
                if (!log.containsKey(i) || !chosen(log, i)) {
                    log.put(i, new LogEntry(other.get(i).ballot, PaxosLogSlotStatus.CHOSEN, other.get(i).command, null));
                }
            } else if (other.containsKey(i) && accepted(other, i)) {
                if (!log.containsKey(i) || (Objects.equals(log.get(i).command, other.get(i).command) && log.get(i).ballot.compareTo(other.get(i).ballot) < 0)) {
                    log.put(i, new LogEntry(other.get(i).ballot, PaxosLogSlotStatus.ACCEPTED, other.get(i).command, null));
                }
            }
        }
    }

    private void mergeLog() {
        int superMax = getSuperMax();
        for (int i = slot_out; i <= superMax; i++) {
            if (chosen(log, i)) {

            } else {
                LogEntry entry = otherLogChosen(i);
                if (entry.ballot != null) {// some other log chosen this slot
                    log.put(i, entry);
                } else { // nobody choose this slot, re-proposing with current ballot
                    LogEntry entry1 = otherLogAcceptedHighest(i);
                    log.put(i, new LogEntry(ballot, PaxosLogSlotStatus.ACCEPTED, entry1.command, null));
                }
            }
        }
    }

    private LogEntry otherLogAcceptedHighest(int i) {
        LogEntry highestBallot = new LogEntry(new Ballot(-1, address()), PaxosLogSlotStatus.ACCEPTED, null, null);
        receivedLogs.add(log);
        for (HashMap<Integer, LogEntry> log : receivedLogs) {
            if (accepted(log, i) && log.get(i).ballot.compareTo(highestBallot.ballot) > 0) {
                highestBallot = log.get(i);
            }
        }
        return highestBallot;
    }

    private LogEntry otherLogChosen(int i) {
        for (HashMap<Integer, LogEntry> log : receivedLogs) {
            if (chosen(log, i)) {
                return log.get(i);
            }
        }
        return new LogEntry(null, null, null, null);
    }

    private boolean chosen(HashMap<Integer, LogEntry> log, int i) {
        return (log.containsKey(i) && log.get(i).paxosLogSlotStatus == PaxosLogSlotStatus.CHOSEN) || i < slot_out;
    }

    private boolean accepted(HashMap<Integer, LogEntry> log, int i) {
        return log.containsKey(i) && log.get(i).paxosLogSlotStatus == PaxosLogSlotStatus.ACCEPTED;
    }

    private int getSuperMax() {
        int superMax = log.keySet().size() > 0 ? Collections.max(log.keySet()) : 0;
        for (HashMap<Integer, LogEntry> log : receivedLogs) {
            superMax = Math.max(superMax, log.keySet().size() > 0 ? Collections.max(log.keySet()) : 0);
        }
        return superMax;
    }

    private void updateSlotIn() {
        if (log.keySet().size() > 0) {
            slot_in = Math.max(Collections.max(log.keySet()) + 1, slot_in);
        }
    }

    private boolean checkLeaderValidity(Ballot other) {
        if (other.compareTo(ballot) > 0) {
            leader = false;
            return false;
        }
        return true;
    }

    private void sendAcceptedP2A() {
        for (int i = slot_out; i < slot_in; i++) {
            if (log.containsKey(i) && accepted(log, i)) {
                sendMsgExceptSelf(new P2A(ballot, log.get(i).command, i));
                set(new P2ATimer(i, log.get(i).command, log.get(i).ballot), P2A_RETRY_TIMER);
            } else if (log.get(i).command == null){ // holes
                sendMsgExceptSelf(new P2A(ballot, null, i));
                set(new P2ATimer(i, null, log.get(i).ballot), P2A_RETRY_TIMER);
            }
        }
    }

    private void heardFromLeader(Ballot ballot, Address sender) {
        if (ballot.compareTo(this.ballot) > 0) {
            this.ballot = ballot;
        }
        heartbeatReceivedThisInterval = true;
        //onHeartbeatCheckTimer(timer);
        if (!Objects.equals(lastLeader, sender)) {
            //roleSettled = true;
            lastLeader = sender;
            onHeartbeatCheckTimer(timer);
        }
    }

    private void beginHeartbeat() {
        sendMsgExceptSelf(new Heartbeat(log, ballot, garbageCollection));
        this.set(new HeartbeatTimer(ballot), HEARTBEAT_MILLIS);
    }

    private boolean oldRequest(Address client, int seqNum) {
        int i = firstNonCleared();
        for (; i < slot_in; i++) {
            if (log.containsKey(i) && log.get(i).command != null
                    && log.get(i).command.clientID().equals(client)
                    && log.get(i).command.sequenceNum() == seqNum
                    && accepted(log, i)) return true;
        }
        return false;
    }


    /* -------------------------------------------------------------------------
        Inner Classes
       -----------------------------------------------------------------------*/
    public static class Ballot implements Comparable<Ballot>, Serializable {
        private Integer seqNum;
        private Address address;

        public Ballot(Integer seqNum, Address address) {
            this.seqNum = seqNum;
            this.address = address;
        }

        public int compareTo(Ballot other) {
            if (!Objects.equals(seqNum, other.seqNum)) {
                return seqNum.compareTo(other.seqNum);
            } else {
                return address.compareTo(other.address);
            }
        }

        @Override
        public String toString() {
            return seqNum.toString() + "." + address.toString().substring("server".length());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Ballot ballot = (Ballot) o;
            return seqNum.equals(ballot.seqNum) &&
                    address.equals(ballot.address);
        }

        @Override
        public int hashCode() {
            return Objects.hash(seqNum, address);
        }
    }

    public static class LogEntry implements Serializable{
        private Ballot ballot;
        private PaxosLogSlotStatus paxosLogSlotStatus;
        private AMOCommand command;
        private AMOResult result;

        public LogEntry(Ballot ballot, PaxosLogSlotStatus status, AMOCommand command, AMOResult result) {
            this.ballot = ballot;
            this.paxosLogSlotStatus = status;
            this.command = command;
            this.result = result;
        }

        @Override
        public String toString() {
            String command = null;
            if (this.command != null) command = this.command.toString();
            String res = null;
            if (this.result != null) res = this.result.toString();

            return "{Ballot=" + ballot.toString() + "|Status=" + paxosLogSlotStatus.toString() + "|Command=" + command + "|Result=" + res + "}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LogEntry logEntry = (LogEntry) o;
            return Objects.equals(ballot, logEntry.ballot) &&
                    paxosLogSlotStatus == logEntry.paxosLogSlotStatus &&
                    Objects.equals(command, logEntry.command) &&
                    Objects.equals(result, logEntry.result);
        }

        @Override
        public int hashCode() {
            return Objects.hash(ballot, paxosLogSlotStatus, command, result);
        }
    }
}
