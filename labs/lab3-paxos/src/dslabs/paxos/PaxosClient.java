package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.paxos.ClientTimer.CLIENT_RETRY_MILLIS;
import static java.lang.Integer.parseInt;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public final class PaxosClient extends Node implements Client {
    private final Address[] servers;

    // Your code here...
    private PaxosRequest paxosRequest;
    private PaxosReply paxosReply;
    private int seqNum = 0;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosClient(Address address, Address[] servers) {
        super(address);
        this.servers = servers;
    }

    @Override
    public synchronized void init() {
        // No need to initialize
    }

    /* -------------------------------------------------------------------------
        Public methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command operation) {
        // Your code here...
        // client should send requests to all servers
        AMOCommand command = new AMOCommand(operation, this.address(), seqNum);

        paxosRequest = new PaxosRequest(command);
        paxosReply = null;

        for (Address server : servers) {
            this.send(new PaxosRequest(command), server);
            this.set(new ClientTimer(command), CLIENT_RETRY_MILLIS);
        }
    }

    @Override
    public synchronized boolean hasResult() {
        // Your code here...
        return paxosReply != null;
    }

    @Override
    public synchronized Result getResult() throws InterruptedException {
        // Your code here...
        while (paxosReply == null) {
            wait();
        }

        seqNum++;
        return paxosReply.result().result();
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
        // Your code here...
        if (m != null && m.result() != null && seqNum == m.result().sequenceNum()) {
            paxosReply = m;
            notify();
        }
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        // Your code here...
        if (seqNum == t.command().sequenceNum() && paxosReply == null) {
            for (Address server : servers) {
                this.send(new PaxosRequest(t.command()), server);
                this.set(t, CLIENT_RETRY_MILLIS);
            }
        }
    }
}
