package dslabs.paxos;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Node;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.paxos.HeartbeatTimer.HEARTBEAT_MILLIS;
import static dslabs.paxos.P2ATimer.P2A_RETRY_TIMER;
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
    private HashMap<Integer, AcceptedEntry> accepted;
    private HashMap<Integer, HashSet<Address>> receivedP2BFrom;
    private HashMap<Address, Integer> clientRequests;
    private boolean heartbeatReceivedThisInterval;

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
        accepted = new HashMap<>();
        receivedP2BFrom = new HashMap<>();
        clientRequests = new HashMap<>();
    }


    @Override
    public void init() {
        // Your code here...
        Address max = servers[0];
        //System.out.println("max is "+max);
        for (Address a: servers) {
            if (a.compareTo(max) > 0) {
                max = a;
            }
        }
        //System.out.println("max is "+max);
        if (Objects.equals(address(), max)) {
            leader = true;
            ballot = new Ballot(seqNum, max);
            //System.out.println("max is "+max);
        } else {
            ballot = new Ballot(seqNum, max);
        }


        for (Address otherServer : servers) {
            if (!Objects.equals(address(), otherServer)) {
                this.send(new Heartbeat(log, slot_out, slot_in), otherServer);
                this.set(new HeartbeatTimer(otherServer), HEARTBEAT_MILLIS);
            }
        }
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
        // As a non-leader, need to drop client request

//        if (Objects.equals(address(), "server5")) {
//            System.out.println("leader is :" + address());
//        }

        if (leader && clientRequests.getOrDefault(sender, -1) < m.amoCommand().sequenceNum()) {
            //System.out.println(sender + " | " + m.amoCommand().sequenceNum());
            LogEntry logEntry = new LogEntry(ballot, PaxosLogSlotStatus.ACCEPTED, m.amoCommand());
            log.put(slot_in, logEntry);
            clientRequests.put(sender, m.amoCommand().sequenceNum());
            for (Address otherServer : servers) {
                if (!Objects.equals(address(), otherServer)) {
                    //System.out.println("handlePaxosRequest");
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
        //System.out.println("leader: " + ballot.toString() + " | acceptor: " + m.ballot().toString());
        if (m.ballot().compareTo(ballot) == 0) { // accept
            //System.out.println("acceptors accepted: handleP2A");
            accepted.put(m.slotNum(), new AcceptedEntry(m.ballot(), m.command()));
            log.put(m.slotNum(),
                    new LogEntry(m.ballot(), PaxosLogSlotStatus.ACCEPTED, m.command()));
            slot_in = m.slotNum() + 1;
            send(new P2B(m.ballot(), m.slotNum()), sender);
        } else {
            send(new P2B(null, null), sender); // Q5: delete this?
        }
    }

    private void handleHeartbeat(Heartbeat m, Address sender) {
        if (!leader) {
            heartbeatReceivedThisInterval = true;
            if (!Objects.equals(log, m.log())) {
                log = m.log();
                slot_out = m.slot_out();
                slot_in = m.slot_in();
                for (int i = slot_out; i < slot_in; i++) {
                    if (log.get(i).paxosLogSlotStatus == PaxosLogSlotStatus.CHOSEN) {
                        AMOCommand command = log.get(i).command;
                        AMOResult result = application.execute(command);
                        send(new PaxosReply(result), command.clientID());
                        //System.out.println("executed");
                    } else {
                        slot_out = i;
                        break;
                    }
                }
            }
        }
    }

    // ---------------leader--------------
    private void handleP2B(P2B m, Address sender) {
        //System.out.println("handleP2B");
        if (m.ballot() != null) {
            HashSet<Address> addresses = receivedP2BFrom.getOrDefault(m.slotNum(), new HashSet<>());
            addresses.add(sender);
            receivedP2BFrom.put(m.slotNum(), addresses);
        }
        //System.out.println("slotNum " + m.slotNum() + " | is: " + receivedP2BFrom.get(m.slotNum()).toString());
        if (receivedP2BFrom.getOrDefault(m.slotNum(), new HashSet<>()).size() >= servers.length / 2) {
            log.put(m.slotNum(), new LogEntry(m.ballot(), PaxosLogSlotStatus.CHOSEN, log.get(m.slotNum()).command));
            //System.out.println(slot_in + "out: "+ slot_out);
            for (int i = slot_out; i < slot_in; i++) {
                if (log.get(i).paxosLogSlotStatus == PaxosLogSlotStatus.CHOSEN) {
                    AMOCommand command = log.get(i).command;
                    AMOResult result = application.execute(command);
                    send(new PaxosReply(result), command.clientID());
                    //System.out.println("executed");
                } else {
                    slot_out = i;
                    break;
                }
            }
        }
        /**
         * Q4
         * Need a way to sync logs between leader and all the acceptors to learn about missed decisions.
         * They can also sync up with heartbeat messages.
         * Alternatively, when the leader sends an accept message, attach the leader’s log with the request. When acceptor replies,
         * attach the acceptor’s log with the response. Then update/merge each side’s log.
         * (Although you should get normal p2a working first.) Lots of different possible protocols and implementations to consider!
         */
    }


    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    // Your code here...
    private void onP2ATimer(P2ATimer t) {
        if (!receivedP2BFrom.containsKey(t.slotNum()) ||
                (!(receivedP2BFrom.get(t.slotNum()).size() >= servers.length / 2) &&
                !receivedP2BFrom.get(t.slotNum()).contains(t.acceptor()))) {
            send(new P2A(ballot, t.slotNum(), t.command()), t.acceptor());
            set(t, P2A_RETRY_TIMER);
        }
    }

    private void onHeartbeatTimer(HeartbeatTimer t) {
        // Your code here...
        this.send(new Heartbeat(log, slot_out, slot_in), t.acceptor());
        this.set(t, HEARTBEAT_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...

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

    private static class AcceptedEntry {
        private Ballot ballot;
        private AMOCommand command;

        public AcceptedEntry(Ballot ballot, AMOCommand command) {
            this.ballot = ballot;
            this.command = command;
        }
    }
}
