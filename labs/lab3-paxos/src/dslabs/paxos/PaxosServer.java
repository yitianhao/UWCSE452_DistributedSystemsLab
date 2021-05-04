package dslabs.paxos;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Node;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.ToString;

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
    private HashSet<Address> majority;
    private Address client;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosServer(Address address, Address[] servers, Application app) {
        super(address);
        this.servers = servers;

        // Your code here...
        this.application = new AMOApplication<>(app);
        this.log = new HashMap<>();
        slot_in = 0;
        slot_out = 0;
        seqNum = 0;
        ballot = new Ballot(seqNum, address);
        accepted = new HashMap<>();
        majority = new HashSet<>();
//        for (Address a : servers) {
//            System.out.println(a);
//
    }


    @Override
    public void init() {
        // Your code here...
        if (Objects.equals(address(), servers[servers.length - 1])) {
            leader = true;
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
        return null;
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
        return null;
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
        return 0;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handlePaxosRequest(PaxosRequest m, Address sender) {
        // Your code here...
        // As a non-leader, need to drop client request
        if (leader) {
            for (Address otherServer : servers) {
                if (!Objects.equals(address(), otherServer)) {
                    LogEntry logEntry = new LogEntry(ballot, PaxosLogSlotStatus.ACCEPTED, m.amoCommand());
                    log.put(slot_in, logEntry);
                    send(new P2A(ballot, slot_in, m.amoCommand()), otherServer);
                    // onP2ATimer???
                }
            }
        }
        client = sender;
    }

    // Your code here...
    // acceptors
    private void handleP2A(P2A m, Address sender) {
        if (m.ballot().compareTo(ballot) == 0) { // accept
            accepted.put(m.slotNum(), new AcceptedEntry(m.ballot(), m.command()));
            send(new P2B(m.ballot(), m.slotNum()), sender);
        } else {
            send(new P2B(null, null), sender);
        }
    }

    private void handleP2B(P2B m, Address sender) {
        if (m.ballot() != null) {
            majority.add(sender);
        }
        if (majority.size() >= servers.length / 2) {
            if (m.slotNum() != slot_in) {
                System.out.println("!!!!!!!!!!!!");
            }
            Command command = log.get(m.slotNum()).command;
            log.put(m.slotNum(), new LogEntry(m.ballot(), PaxosLogSlotStatus.CHOSEN, command));
            AMOResult result = application.execute(command);
            send(new PaxosReply(result), client);
        }
        /**
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
//            if (Objects.equals(address, other.address)) {
//                return seqNum.compareTo(other.seqNum);
//            } else if (Objects.equals(seqNum, other.seqNum)){
//                return address.compareTo(other.address);
//            } else {
//                return convertToDouble(seqNum, address).compareTo(convertToDouble(other.seqNum, other.address));
//            }
        }

        private Double convertToDouble(Integer seqNum, Address address) {
            return seqNum + 0.1 * parseInt(address.toString().substring(("server").length()));
        }
    }

    private static class LogEntry {
        private Ballot ballot;
        private PaxosLogSlotStatus paxosLogSlotStatus;
        private Command command;

        public LogEntry(Ballot ballot, PaxosLogSlotStatus status, Command command) {
            this.ballot = ballot;
            this.paxosLogSlotStatus = status;
            this.command = command;
        }
    }

    private static class AcceptedEntry {
        private Ballot ballot;
        private Command command;

        public AcceptedEntry(Ballot ballot, Command command) {
            this.ballot = ballot;
            this.command = command;
        }
    }
}
