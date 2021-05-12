package dslabs.paxos;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
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
    private HashMap<Integer, HashSet<Address>> receivedPositiveP2BFrom;
    //private HashMap<Integer, HashSet<Address>> receivedNegativeP2BFrom;
    private HashMap<Address, Integer> clientRequests;
    private boolean heartbeatReceivedThisInterval;
    private HashSet<Address> receivedPositiveP1BFrom;
    //private HashSet<Address> receivedNegativeP1BFrom;
    private Address lastLeader;
    private Ballot highestBallotSeen;

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
        seqNum = -1;
        ballot = new Ballot(seqNum, address);
        receivedPositiveP2BFrom = new HashMap<>();
        //receivedNegativeP2BFrom = new HashMap<>();
        receivedPositiveP1BFrom = new HashSet<>();
        //receivedNegativeP1BFrom = new HashSet<>();
        clientRequests = new HashMap<>();
        lastLeader = null;
        highestBallotSeen = ballot;
    }


    @Override
    public void init() {
        // Your code here...
        startLeaderElection();
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
            //System.out.println("adding new slot" + slot_in);
            clientRequests.put(sender, m.amoCommand().sequenceNum());
            for (Address otherServer : servers) {
                if (!Objects.equals(address(), otherServer)) {
                    send(new P2A(ballot, slot_in, m.amoCommand()), otherServer);
                    set(new P2ATimer(slot_in, otherServer, m.amoCommand()), P2A_RETRY_TIMER);
                }
            }
            updateSlotIn();
        }
    }

    // Your code here...
    // -----------acceptors------------
    private void handleP2A(P2A m, Address sender) {
        // only accept it if the ballot in the message matches the acceptor’s ballot,
        // which means the acceptor considers the sender to be the current leader
        if (!leader) {
            // case1: equals
            // case2: greater: did not hear of P1A but gets P2A with a higher ballot number
            if (m.ballot().compareTo(ballot) >= 0) {
                //System.out.println(address() + " accepts P2A " + m.slotNum() + " | sent by " + sender);
                ballot = m.ballot();
                log.put(m.slotNum(), new LogEntry(m.ballot(), PaxosLogSlotStatus.ACCEPTED, m.command()));
                send(new P2B(ballot, m.slotNum()), sender);
            }
        }
    }


    // ---------------leader--------------
    private void handleP2B(P2B m, Address sender) {
        if (leader) {
            if (!checkLeaderValidity(m.ballot())) {
                //System.out.println(address() + " is not more leader ");
                return;
            }
            HashSet<Address> addresses = receivedPositiveP2BFrom.getOrDefault(m.slotNum(), new HashSet<>());
            addresses.add(sender);
            receivedPositiveP2BFrom.put(m.slotNum(), addresses);

            if (receivedPositiveP2BFrom.getOrDefault(m.slotNum(), new HashSet<>()).size() >= servers.length / 2) {
                //System.out.println(m.slotNum() + " is chosen ");
                log.put(m.slotNum(), new LogEntry(m.ballot(), PaxosLogSlotStatus.CHOSEN, log.get(m.slotNum()).command));
                executeChosen();
                updateSlotIn();
            }
        }
    }


    // ---------potential acceptors--------
    private void handleP1A(P1A m, Address sender) {
        if (ballot.compareTo(m.ballot()) < 0) {
            // ok!
            //System.out.println(address() + " is voting for " + sender);
            ballot = m.ballot();
            leader = false;
            send(new P1B(ballot, log), sender);
        }
    }

    // --------potential leader--------
    // update based on section slides p63
    private void handleP1B(P1B m, Address sender) {
        if (m.ballot().compareTo(ballot) == 0) {
            receivedPositiveP1BFrom.add(sender);
            mergeLog(m.log());

            //System.out.println("There are " + servers.length + " servers");

            // -------leader has been elected----------
            if (receivedPositiveP1BFrom.size() >= servers.length / 2) {
                //System.out.println(address() + " is the leader!");
                leader = true;
                receivedPositiveP2BFrom = new HashMap<>();
                seqNum++;
                ballot = new Ballot(seqNum, address());
                //System.out.println("***Ballot = " + ballot.seqNum);
                updateSlotIn();
                executeChosen();
                //System.out.println("slotIN = " + slot_in + " | slotOUT = " + slot_out);
                // Send out phase 2 (P2A) messages for all values that have been accepted already
                for (Address otherServer : servers) {
                    if (!Objects.equals(address(), otherServer)) {
                        for (int i = slot_out; i < slot_in; i++) {
                            //System.out.println("sending out P2A " + i);
                            if (log.containsKey(i) && log.get(i).paxosLogSlotStatus == PaxosLogSlotStatus.ACCEPTED) {
                                send(new P2A(ballot, i, log.get(i).command), otherServer);
                                set(new P2ATimer(i, otherServer, log.get(i).command), P2A_RETRY_TIMER);
                            } else if (log.get(i).command == null){ // holes
                                send(new P2A(ballot, i, null), otherServer);
                                set(new P2ATimer(i, otherServer, null), P2A_RETRY_TIMER);
                            }
                        }
                    }
                }
                // begin sending out heartbeats
                for (Address otherServer : servers) {
                    if (!Objects.equals(address(), otherServer)) {
                        this.send(new Heartbeat(log, ballot), otherServer);
                        this.set(new HeartbeatTimer(otherServer),
                                HEARTBEAT_MILLIS);
                    }
                }
            }
        }
    }

    // ---------acceptors---------
    private void handleHeartbeat(Heartbeat m, Address sender) {
        if (!leader) {
            if (m.ballot().compareTo(ballot) > 0) return;
            if (!Objects.equals(lastLeader, sender)) {
                this.set(new HeartbeatCheckTimer(), HEARTBEAT_CHECK_MILLIS);
                seqNum++;
                ballot = new Ballot(seqNum, address());
            }
            //System.out.println("acceptor = " + address() + " | ballot = " + ballot.seqNum);
            lastLeader = sender;
            heartbeatReceivedThisInterval = true;
            //System.out.println("-----now leader is : " + sender);
            mergeLog(m.log());
            updateSlotIn();
            executeChosen(); // will update slot_out
            //System.out.println(address() + " slot_in is: " + slot_in );
        }
    }


    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    // Your code here...
    private void onP2ATimer(P2ATimer t) {
        if (leader) {
            if (receivedPositiveP2BFrom.containsKey(t.slotNum()) &&
                    (receivedPositiveP2BFrom.get(t.slotNum()).contains(t.acceptor()) ||
                     receivedPositiveP2BFrom.get(t.slotNum()).size() >= servers.length / 2)) {
                // don't resend
            } else {
                send(new P2A(ballot, t.slotNum(), t.command()), t.acceptor());
                set(t, P2A_RETRY_TIMER);
            }
        }
    }

    private void onP1ATimer(P1ATimer t) {
        if (receivedPositiveP1BFrom.contains(t.acceptor()) || receivedPositiveP1BFrom.size() >= servers.length / 2) {

        } else {
            send(new P1A(ballot), t.acceptor());
            set(t, P1A_RETRY_TIMER);
        }
    }

    private void onHeartbeatTimer(HeartbeatTimer t) {
        if (leader) {
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
                 //System.out.println("leader " + lastLeader + " is dead; " + address() + "starting election");
                startLeaderElection();
            }
        }
    }



    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...
    private void executeChosen() {
        int i = slot_out;
        while (log.containsKey(i) && log.get(i).paxosLogSlotStatus == PaxosLogSlotStatus.CHOSEN) {
            //System.out.println(i + " is chosen, executing.... ");
            AMOCommand command = log.get(i).command;
            if (command != null) {  // in the case of no-op
                AMOResult result = application.execute(command);
                //System.out.println("!!!slot  = " + i + " | client = " + command.clientID() + " | seqNum = " + command.sequenceNum());
                send(new PaxosReply(result), command.clientID());
                //System.out.println("send to " + command.clientID().toString());
            } else {
                //System.out.println("holes!");
            }
            i++;
        }
        slot_out = i;
    }

    private void startLeaderElection() {
        receivedPositiveP1BFrom = new HashSet<>();
        for (Address otherServer : servers) {
            if (!Objects.equals(address(), otherServer)) {
                send(new P1A(ballot), otherServer);
                set(new P1ATimer(otherServer), P1A_RETRY_TIMER);
            }
        }
    }

    // To merge the current log with the incoming log
    private void mergeLog(HashMap<Integer, LogEntry> other) {
        for (Integer slot_num : other.keySet()) {
            //System.out.print("slot = " + slot_num + " | ");
            if (other.get(slot_num).paxosLogSlotStatus == PaxosLogSlotStatus.CHOSEN) {
                log.put(slot_num, new LogEntry(other.get(slot_num).ballot, PaxosLogSlotStatus.CHOSEN, other.get(slot_num).command));
                clientRequests.put(other.get(slot_num).command.clientID(), other.get(slot_num).command.sequenceNum());
                //System.out.println(" been chosen");
            } else if (!log.containsKey(slot_num) || log.get(slot_num).ballot.compareTo(other.get(slot_num).ballot) < 0) {
                log.put(slot_num, new LogEntry(other.get(slot_num).ballot, PaxosLogSlotStatus.ACCEPTED, other.get(slot_num).command));
                //System.out.println(" been accepted");
                clientRequests.put(other.get(slot_num).command.clientID(), other.get(slot_num).command.sequenceNum());
            }
        }

        if (log.keySet().size() > 0) {
            for (int i = slot_out; i < Collections.max(log.keySet()); i++) {
                if (!log.keySet().contains(i)) { // holes
                    //System.out.println("filling holes");
                    log.put(i, new LogEntry(ballot, PaxosLogSlotStatus.ACCEPTED, null));
                }
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
    }

    public static class LogEntry implements Serializable {
        private Ballot ballot;
        private PaxosLogSlotStatus paxosLogSlotStatus;
        private AMOCommand command;

        public LogEntry(Ballot ballot, PaxosLogSlotStatus status, AMOCommand command) {
            this.ballot = ballot;
            this.paxosLogSlotStatus = status;
            this.command = command;
        }
    }

}
