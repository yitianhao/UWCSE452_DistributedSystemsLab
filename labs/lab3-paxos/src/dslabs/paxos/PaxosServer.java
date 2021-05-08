package dslabs.paxos;

import com.sun.jdi.request.ClassUnloadRequest;
import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Node;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.paxos.HeartbeatCheckTimer.HEARTBEAT_CHECK_MILLIS;
import static dslabs.paxos.HeartbeatTimer.HEARTBEAT_MILLIS;
import static dslabs.paxos.P2ATimer.P2A_RETRY_TIMER;
import static java.lang.Integer.max;
import static java.lang.Integer.parseInt;


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
    //private HashMap<Integer, AcceptedEntry> accepted;
    private HashMap<Integer, HashSet<Address>> receivedPositiveP2BFrom;
    private HashMap<Integer, HashSet<Address>> receivedNegativeP2BFrom;
    private HashMap<Address, Integer> clientRequests;
    private boolean heartbeatReceivedThisInterval;
    private HashSet<Address> receivedPositiveP1BFrom;
    private HashSet<Address> receivedNegativeP1BFrom;
    //private HashMap<Integer, AcceptedEntry> proposals;


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
        receivedPositiveP2BFrom = new HashMap<>();
        receivedNegativeP2BFrom = new HashMap<>();
        receivedPositiveP1BFrom = new HashSet<>();
        receivedNegativeP1BFrom = new HashSet<>();
        clientRequests = new HashMap<>();
    }


    @Override
    public void init() {
        // Your code here...
        startLeaderElection();


//        Address max = servers[0];
//        //System.out.println("max is "+max);
//        for (Address a: servers) {
//            if (a.compareTo(max) > 0) {
//                max = a;
//            }
//        }
//        //System.out.println("max is "+max);
//        if (Objects.equals(address(), max)) {
//            leader = true;
//            ballot = new Ballot(seqNum, max);
//            //System.out.println("max is "+max);
//        } else {
//            ballot = new Ballot(seqNum, max);
//        }
//
//
//        if (leader) {
//            for (Address otherServer : servers) {
//                if (!Objects.equals(address(), otherServer)) {
//                    this.send(new Heartbeat(log, slot_out, slot_in), otherServer);
//                    this.set(new HeartbeatTimer(otherServer), HEARTBEAT_MILLIS);
//                }
//            }
//        }
//
//        if (!leader) {
//            this.set(new HeartbeatCheckTimer(), HEARTBEAT_CHECK_MILLIS);
//        }
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
        return log.getOrDefault(logSlotNum, new LogEntry(null, PaxosLogSlotStatus.EMPTY, null)).paxosLogSlotStatus;
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
            return log.get(logSlotNum).command.command();
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
        return slot_in - 1;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handlePaxosRequest(PaxosRequest m, Address sender) {
        // Your code here...
        // 1. As a non-leader, need to drop client request
        // 2. if the command has already been proposed, decided, or executed, drop the client request
        if (leader && clientRequests.getOrDefault(sender, -1) < m.amoCommand().sequenceNum()) {
            LogEntry logEntry = new LogEntry(ballot, PaxosLogSlotStatus.ACCEPTED, m.amoCommand());
            log.put(slot_in, logEntry);
            clientRequests.put(sender, m.amoCommand().sequenceNum());
            for (Address otherServer : servers) {
                if (!Objects.equals(address(), otherServer)) {
                    send(new P2A(ballot, slot_in, m.amoCommand()), otherServer);
                    set(new P2ATimer(slot_in, otherServer, m.amoCommand()), P2A_RETRY_TIMER);
                }
            }
            slot_in++;
        }
    }

    // Your code here...
    // -----------acceptors------------
    private void handleP2A(P2A m, Address sender) {
        // only accept it if the ballot in the message matches the acceptor’s ballot,
        // which means the acceptor considers the sender to be the current leader
        if (m.ballot().compareTo(ballot) == 0) { // accept
            log.put(m.slotNum(), new LogEntry(ballot, PaxosLogSlotStatus.ACCEPTED, m.command()));
            log.put(m.slotNum(),
                    new LogEntry(m.ballot(), PaxosLogSlotStatus.ACCEPTED, m.command()));
            slot_in = m.slotNum() + 1;
            send(new P2B(m.ballot(), m.slotNum()), sender);
        } else {
            send(new P2B(null, null), sender); // Q5: delete this?
        }
    }


    // ---------------leader--------------
    private void handleP2B(P2B m, Address sender) {
        if (m.ballot() != null) {
            HashSet<Address> addresses = receivedPositiveP2BFrom.getOrDefault(m.slotNum(), new HashSet<>());
            addresses.add(sender);
            receivedPositiveP2BFrom.put(m.slotNum(), addresses);
        } else {
            HashSet<Address> addresses = receivedNegativeP2BFrom.getOrDefault(m.slotNum(), new HashSet<>());
            addresses.add(sender);
            receivedNegativeP2BFrom.put(m.slotNum(), addresses);
        }
        if (receivedPositiveP2BFrom.getOrDefault(m.slotNum(), new HashSet<>()).size() >= servers.length / 2) {
            log.put(m.slotNum(), new LogEntry(m.ballot(), PaxosLogSlotStatus.CHOSEN, log.get(m.slotNum()).command));
            executeChosen();
        }

        if (receivedNegativeP2BFrom.getOrDefault(m.slotNum(), new HashSet<>()).size() >= servers.length / 2) {
            leader = false;
            // start electing
        }
    }


    // ---------potential acceptors--------
    private void handleP1A(P1A m, Address sender) {
        if (ballot.compareTo(m.ballot()) < 0) {
            // ok!
            ballot = m.ballot();
            leader = false;
            send(new P1B(ballot, log), sender);
        } else {
            // nope!
            send(new P1B(null, null), sender);
        }
    }

    // --------potential leader--------
    // update based on section slides p63
    private void handleP1B(P1B m, Address sender) {
        if (m.ballot() == null) {
            // don't resend P1A
            receivedNegativeP1BFrom.add(sender);
        } else {
            if (m.ballot().compareTo(ballot) == 0) {
                receivedPositiveP1BFrom.add(sender);
                for (Integer slot_num : m.log().keySet()) {
                    if (m.log().get(slot_num).paxosLogSlotStatus == PaxosLogSlotStatus.CHOSEN) {
                        log.put(slot_num, m.log().get(slot_num));
                    } else if (!log.containsKey(slot_num) || log.get(slot_num).ballot.compareTo(m.log().get(slot_num).ballot) < 0) {
                        log.put(slot_num, new LogEntry(m.log().get(slot_num).ballot, PaxosLogSlotStatus.ACCEPTED, m.log().get(slot_num).command));
                    }
                }
            }
        }

        if (log.keySet().size() > 0) {
            slot_in = Math.max(Collections.max(log.keySet()) + 1, slot_in);
        }

        // -------leader has been elected----------
        if (receivedPositiveP1BFrom.size() >= servers.length / 2) {
            leader = true;
            executeChosen();
            // Send out phase 2 (P2A) messages for all values that have been accepted already
            for (Address otherServer : servers) {
                if (!Objects.equals(address(), otherServer)) {
                    for (int i = slot_out; i < slot_in; i++) {
                        if (log.containsKey(i) &&
                                (log.get(i).paxosLogSlotStatus == PaxosLogSlotStatus.ACCEPTED
                                 || log.get(i).paxosLogSlotStatus == PaxosLogSlotStatus.CHOSEN)) {
                            send(new P2A(ballot, i, log.get(i).command), otherServer);
                            set(new P2ATimer(i, otherServer, log.get(i).command), P2A_RETRY_TIMER);
                        } else { // holes
                            send(new P2A(ballot, i, null), otherServer);
                            set(new P2ATimer(i, otherServer, null), P2A_RETRY_TIMER);
                        }
                    }
                }
            }
            // begin sending out heartbeats
            for (Address otherServer : servers) {
                if (!Objects.equals(address(), otherServer)) {
                    this.send(new Heartbeat(log, slot_out, slot_in), otherServer);
                    this.set(new HeartbeatTimer(otherServer), HEARTBEAT_MILLIS);
                }
            }
            // ****Q2: how does other servers know that you are the leader now?
            // i.e., when can a non-leader be assure that he can send out HeartbeatCheck
        }
    }

    // ---------acceptors---------
    private void handleHeartbeat(Heartbeat m, Address sender) {
        // check the condition of accepting the log ?????
        if (!leader) {
            heartbeatReceivedThisInterval = true;
            if (!Objects.equals(log, m.log())) {
                log = m.log();
                slot_out = m.slot_out();
                slot_in = m.slot_in();
                executeChosen();
            }
        }
    }


    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    // Your code here...
    private void onP2ATimer(P2ATimer t) {
        if (receivedPositiveP2BFrom.containsKey(t.slotNum()) &&
            (receivedPositiveP2BFrom.get(t.slotNum()).contains(t.acceptor()) ||
            receivedPositiveP2BFrom.get(t.slotNum()).size() >= servers.length / 2)) {
            // don't resend
        } else if (receivedNegativeP2BFrom.containsKey(t.slotNum()) &&
                (receivedNegativeP2BFrom.get(t.slotNum()).contains(t.acceptor()) ||
                receivedNegativeP2BFrom.get(t.slotNum()).size() >= servers.length / 2)) {
            // don't resend
        } else {
            send(new P2A(ballot, t.slotNum(), t.command()), t.acceptor());
            set(t, P2A_RETRY_TIMER);
        }
    }

    private void onHeartbeatTimer(HeartbeatTimer t) {
        // Your code here...
        this.send(new Heartbeat(log, slot_out, slot_in), t.acceptor());
        this.set(t, HEARTBEAT_MILLIS);
    }

    private void onHeartbeatCheckTimer(HeartbeatCheckTimer t) {
        // Your code here...
        if (heartbeatReceivedThisInterval) {
            heartbeatReceivedThisInterval = false;
            this.set(t, HEARTBEAT_CHECK_MILLIS);
        } else {
            // try to be leader
            startLeaderElection();
        }
    }



    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...
    private void executeChosen() {
        for (int i = slot_out; i < slot_in; i++) {
            if (log.get(i).paxosLogSlotStatus == PaxosLogSlotStatus.CHOSEN) {
                AMOCommand command = log.get(i).command;
                if (command != null) {  // in the case of no-op
                    AMOResult result = application.execute(command);
                    send(new PaxosReply(result), command.clientID());
                }
            } else {
                slot_out = i;
                break;
            }
        }
    }

    private void startLeaderElection() {
        for (Address otherServer : servers) {
            if (!Objects.equals(address(), otherServer)) {
                send(new P1A(ballot), otherServer);
                // Q1: no P1A timer
            }
        }
    }

    /* -------------------------------------------------------------------------
        Inner Classes
       -----------------------------------------------------------------------*/
    public static class Ballot implements Comparable<Ballot> {
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

        private Double convertToDouble(Integer seqNum, Address address) {
            return seqNum + 0.1 * parseInt(address.toString().substring(("server").length()));
        }
    }

    public static class LogEntry {
        private Ballot ballot;
        private PaxosLogSlotStatus paxosLogSlotStatus;
        private AMOCommand command;

        public LogEntry(Ballot ballot, PaxosLogSlotStatus status, AMOCommand command) {
            this.ballot = ballot;
            this.paxosLogSlotStatus = status;
            this.command = command;
        }
    }

    public static class AcceptedEntry {
        private Ballot ballot;
        private AMOCommand command;

        public AcceptedEntry(Ballot ballot, AMOCommand command) {
            this.ballot = ballot;
            this.command = command;
        }
    }
}
