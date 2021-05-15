package dslabs.paxos;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Message;
import dslabs.framework.Node;
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

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class PaxosServer extends Node {
    private final Address[] servers;

    // Your code here...
    private final AMOApplication<Application> application;
    private HashMap<Integer, LogEntry> log;  // logNum -> LogEntry
    private boolean leader;
    private int slot_out;
    private int slot_in;
    private int seqNum;
    private Ballot ballot;
    private HashMap<Integer, HashSet<Address>> receivedP2BFrom;
    //private HashMap<Address, ClientReqEntry> clientRequests;
    private boolean heartbeatReceivedThisInterval;
    private HashSet<Address> receivedPositiveP1BFrom;
    private boolean stopP1ATimer;
    //private Address lastLeader;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosServer(Address address, Address[] servers, Application app) {
        super(address);
        this.servers = servers;

        // Your code here...
        this.application = new AMOApplication<>(app);
        this.log = new HashMap<>();
        slot_in = 1;
        slot_out = 1;
        seqNum = 0;
        ballot = new Ballot(seqNum, address);
        receivedP2BFrom = new HashMap<>();
        //clientRequests = new HashMap<>();
        stopP1ATimer = false;
    }


    @Override
    public void init() {
        // Your code here...
//        Address max = servers[0];
//        //System.out.println("max is "+max);
//        for (Address a: servers) {
//            if (a.compareTo(max) > 0) {
//                max = a;
//            }
//        }
//        //System.out.println("max is "+max);
//        if (Objects.equals(address(), max)) {
//            //System.out.println("leader = " + address());
//            leader = true;
//            ballot = new Ballot(seqNum, max);
//            //System.out.println("max is "+max);
//        } else {
//            leader = false;
//            ballot = new Ballot(seqNum, max);
//        }
//
//
//        for (Address otherServer : servers) {
//            if (!Objects.equals(address(), otherServer)) {
//                this.send(new Heartbeat(log), otherServer);
//                this.set(new HeartbeatTimer(otherServer), HEARTBEAT_MILLIS);
//            }
//        }
        if (servers.length == 1) {
            leader = true;
        } else {
            startLeaderElection();
        }
        this.set(new HeartbeatCheckTimer(), HEARTBEAT_CHECK_MILLIS);
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
        if (!log.containsKey(logSlotNum)) {
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
        return 1;
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
        return log.keySet().size() == 0 ? 0 : Collections.max(log.keySet());
    }

    private ClientReqEntry getSeqNumByClient(Address clientID) {
        ClientReqEntry res = new ClientReqEntry(-1, -1);
        for (int i : log.keySet()) {
            if (log.get(i).command == null) continue;
            if (Objects.equals(log.get(i).command.clientID(), clientID)) {
                if (res.seqNum < log.get(i).command.sequenceNum()) {
                    res = new ClientReqEntry(log.get(i).command.sequenceNum(), i);
                }
            }
        }
        return res;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handlePaxosRequest(PaxosRequest m, Address sender) {
        // Your code here...
        // As a non-leader, need to drop client request
        ClientReqEntry entry = getSeqNumByClient(m.amoCommand().clientID());
        if (leader && entry.seqNum < m.amoCommand().sequenceNum()) {
            //System.out.println("leader = " + address());
            //System.out.println(sender + " | " + m.amoCommand().sequenceNum());
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
        } else if (leader && entry.seqNum == m.amoCommand().sequenceNum()) {
            int slotNum = entry.slotNum;
            if (log.containsKey(slotNum) && slotNum < slot_out) {
                if (log.get(slotNum).result == null) {
                    AMOResult result = application.execute(m.amoCommand());
                    send(new PaxosReply(result), m.amoCommand().clientID());
                    // update result
                    log.put(slotNum, new LogEntry(log.get(slotNum).ballot, PaxosLogSlotStatus.CHOSEN, log.get(slotNum).command, result));
                }
                //System.out.println(sender + " not heard back from " + slotNum + "| now sending back result = " + log.get(slotNum).result);
                send(new PaxosReply(log.get(slotNum).result), sender);
            }
        }
    }

    // Your code here...
    // -----------acceptors------------
    private void handleP2A(P2A m, Address sender) {
        // only accept it if the ballot in the message matches the acceptor’s ballot,
        // which means the acceptor considers the sender to be the current leader
        //System.out.println("leader: " + ballot.toString() + " | acceptor: " + m.ballot().toString());
        if (!leader) {
            if (m.ballot().compareTo(ballot) >= 0) {
                ballot = m.ballot();
                //log.put(m.slotNum(), new LogEntry(ballot, PaxosLogSlotStatus.ACCEPTED, m.command(), null));
                send(new P2B(ballot, m.slotNum()), sender);
            }
        } else {
            checkLeaderValidity(m.ballot());
        }
    }

    // ---------------leader--------------
    private void handleP2B(P2B m, Address sender) {
        if (leader) {
            checkLeaderValidity(m.ballot());
            HashSet<Address> addresses = receivedP2BFrom.getOrDefault(m.slotNum(), new HashSet<>());
            addresses.add(sender);
            receivedP2BFrom.put(m.slotNum(), addresses);
            //System.out.println("slotNum " + m.slotNum() + " | is: " + receivedP2BFrom.get(m.slotNum()).toString());
            if (receivedP2BFrom.getOrDefault(m.slotNum(), new HashSet<>()).size() >= servers.length / 2) {
                log.put(m.slotNum(), new LogEntry(m.ballot(), PaxosLogSlotStatus.CHOSEN, log.get(m.slotNum()).command, null));
                executeChosen();
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
            //if (checkLeaderValidity(m.ballot())) return;
        }
        if (m.ballot().compareTo(ballot) > 0) {
            ballot = m.ballot();
        }
        heartbeatReceivedThisInterval = true;
        mergeLog(m.log());
        updateSlotIn();
        executeChosen();
//        System.out.println("leader: " + sender.toString() + " log ...");
//        System.out.println(m.log().toString());
//        System.out.println("acceptor " + address().toString() + " s log...");
//        System.out.println(log);
    }

    // ---------potential acceptors--------
    private void handleP1A(P1A m, Address sender) {
        if (m.ballot().compareTo(ballot) >= 0) {
            //System.out.println(address() + "'s ballot = " + ballot.toString() + " | P1A's ballot = " + m.ballot().toString());
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
        if (m.ballot().compareTo(ballot) > 0) {
            ballot = m.ballot();
            stopP1ATimer = true;
            return;
        }
        else if (m.ballot().compareTo(ballot) < 0) return;

        receivedPositiveP1BFrom.add(sender);
        mergeLog(m.log());

        if (receivedPositiveP1BFrom.size() >= servers.length / 2) {
            //System.out.println("--------- " + address() + " = leader ------------- ballot" + ballot.toString());
            leader = true;
            //seqNum++;
            //ballot = new Ballot(seqNum, address());
            updateSlotIn();
            executeChosen();
            receivedP2BFrom = new HashMap<>();
            sendAcceptedP2A();
            beginHeartbeat();
        }
    }


    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    // Your code here...
    private void onP2ATimer(P2ATimer t) {
        if (!receivedP2BFrom.containsKey(t.slotNum()) ||
                !(receivedP2BFrom.get(t.slotNum()).size() >= servers.length / 2)) {
            sendMsgExceptSelf(new P2A(t.ballot(), t.command(), t.slotNum()));
            set(t, P2A_RETRY_TIMER);
        }
    }

    private void onP1ATimer(P1ATimer t) {
        // unless one of the following 3 cases, keep trying
        if (!(receivedPositiveP1BFrom.contains(t.acceptor()) || // A. I received your response
            leader || // B. I am the leader
            stopP1ATimer)) { // C. I get a message with higher ballot
            send(new P1A(t.ballot()), t.acceptor());
            set(t, P1A_RETRY_TIMER);
        }
    }

    private void onHeartbeatTimer(HeartbeatTimer t) {
        // Your code here...
        if (leader && ballot.compareTo(t.ballot()) == 0) {
            this.send(new Heartbeat(log, ballot), t.acceptor());
            this.set(t, HEARTBEAT_MILLIS);
        }
    }

    private void onHeartbeatCheckTimer(HeartbeatCheckTimer t) {
        if (!leader) {
            if (heartbeatReceivedThisInterval) {
                heartbeatReceivedThisInterval = false;
                this.set(t, HEARTBEAT_CHECK_MILLIS);
            } else {
                // try to be leader
                //System.out.println("leader seems dead; " + address() + "starting election");
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
        while (log.containsKey(i) && log.get(i).paxosLogSlotStatus == PaxosLogSlotStatus.CHOSEN) {
            //System.out.println(i + " is chosen, executing.... ");
            AMOCommand command = log.get(i).command;
            if (command != null) {  // in the case of no-op
                AMOResult result = application.execute(command);
                //System.out.println(i + " , result = " + result + ", client = " + command.clientID());
                send(new PaxosReply(result), command.clientID());
                // update result
                log.put(i, new LogEntry(log.get(i).ballot, PaxosLogSlotStatus.CHOSEN, log.get(i).command, result));
                //System.out.println("send to " + command.clientID().toString());
            } else {
                //System.out.println(i + " is hole!");
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
        for (Address otherServer : servers) {
            if (!Objects.equals(address(), otherServer)) {
                send(new P1A(ballot), otherServer);
                set(new P1ATimer(otherServer, ballot), P1A_RETRY_TIMER);
            }
        }
    }

    // To merge the current log with the incoming log
    private void mergeLog(HashMap<Integer, LogEntry> other) {
//        for (Integer slot_num : other.keySet()) {
//            //System.out.print("slot = " + slot_num + " | ");
//            if (other.get(slot_num).paxosLogSlotStatus == PaxosLogSlotStatus.CHOSEN) {
//                log.put(slot_num, new LogEntry(other.get(slot_num).ballot, PaxosLogSlotStatus.CHOSEN, other.get(slot_num).command, null));
//                if (other.get(slot_num).command != null) {
//                    clientRequests.put(other.get(slot_num).command.clientID(),
//                            new ClientReqEntry(other.get(slot_num).command.sequenceNum(), slot_num));
//                }
//                //System.out.println(" been chosen");
//            } else if (!log.containsKey(slot_num) || (log.containsKey(slot_num) && log.get(slot_num).ballot.compareTo(other.get(slot_num).ballot) < 0)) {
//                log.put(slot_num, new LogEntry(other.get(slot_num).ballot, PaxosLogSlotStatus.ACCEPTED, other.get(slot_num).command, null));
//                //System.out.println(" been accepted");
//                if (other.get(slot_num).command != null) {
//                    clientRequests.put(other.get(slot_num).command.clientID(),
//                            new ClientReqEntry(other.get(slot_num).command.sequenceNum(), slot_num));
//                }
//            }
//        }
//
//        if (log.keySet().size() > 0) {
//            for (int i = slot_out; i < Collections.max(log.keySet()); i++) {
//                if (!log.keySet().contains(i)) { // holes
//                    //System.out.println("filling holes");
//                    log.put(i, new LogEntry(ballot, PaxosLogSlotStatus.ACCEPTED, null, null));
//                }
//            }
//        }
        int maxSlotNum = Math.max(log.keySet().size() > 0 ? Collections.max(log.keySet()) : 0, other.keySet().size() > 0 ? Collections.max(other.keySet()) : 0);
        for (int i = slot_out; i <= maxSlotNum; i++) {
            if (other.containsKey(i) && other.get(i).paxosLogSlotStatus == PaxosLogSlotStatus.CHOSEN) {
                if (!log.containsKey(i) || log.get(i).paxosLogSlotStatus != PaxosLogSlotStatus.CHOSEN) {
                    log.put(i, new LogEntry(other.get(i).ballot, PaxosLogSlotStatus.CHOSEN, other.get(i).command, null));
                }
            } else if (other.containsKey(i) && other.get(i).paxosLogSlotStatus == PaxosLogSlotStatus.ACCEPTED) {
                if (!log.containsKey(i) || (Objects.equals(log.get(i).command, other.get(i).command) && log.get(i).ballot.compareTo(other.get(i).ballot) < 0)) {
                    log.put(i, new LogEntry(other.get(i).ballot, PaxosLogSlotStatus.ACCEPTED, other.get(i).command, null));
                }
            } else if (!other.containsKey(i) && !log.containsKey(i)) {
                log.put(i, new LogEntry(ballot, PaxosLogSlotStatus.ACCEPTED, null, null));
            } else if (other.containsKey(i) && log.containsKey(i) && !Objects.equals(log.get(i).command, other.get(i).command)) {
                log.put(i, new LogEntry(ballot, PaxosLogSlotStatus.ACCEPTED, null, null));

            }
        }
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
        for (Address otherServer : servers) {
            if (!Objects.equals(address(), otherServer)) {
                for (int i = slot_out; i < slot_in; i++) {
                    //System.out.println("sending out P2A " + i);
                    if (log.containsKey(i) && log.get(i).paxosLogSlotStatus == PaxosLogSlotStatus.ACCEPTED) {
                        send(new P2A(ballot, log.get(i).command, i), otherServer);
                        set(new P2ATimer(i, log.get(i).command, log.get(i).ballot), P2A_RETRY_TIMER);
                    } else if (log.get(i).command == null){ // holes
                        send(new P2A(ballot, null, i), otherServer);
                        set(new P2ATimer(i, null, log.get(i).ballot), P2A_RETRY_TIMER);
                    }
                }
            }
        }
    }

    private void beginHeartbeat() {
        for (Address otherServer : servers) {
            if (!Objects.equals(address(), otherServer)) {
                this.send(new Heartbeat(log, ballot), otherServer);
                this.set(new HeartbeatTimer(otherServer, ballot), HEARTBEAT_MILLIS);
            }
        }
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

    public static class ClientReqEntry implements Serializable {
        private int seqNum;
        private int slotNum;

        public ClientReqEntry(int seqNum, int slotNum) {
            this.seqNum = seqNum;
            this.slotNum = slotNum;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ClientReqEntry that = (ClientReqEntry) o;
            return seqNum == that.seqNum && slotNum == that.slotNum;
        }

        @Override
        public int hashCode() {
            return Objects.hash(seqNum, slotNum);
        }
    }
}
