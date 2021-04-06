package dslabs.clientserver;

import com.google.common.base.Objects;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore.SingleKeyCommand;
import java.util.HashMap;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import lombok.ToString;

import static dslabs.clientserver.ClientTimer.CLIENT_RETRY_MILLIS;


/**
 * Simple client that sends requests to a single server and returns responses.
 *
 * See the documentation of {@link Client} and {@link Node} for important
 * implementation notes.
 */
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class SimpleClient extends Node implements Client {
    private final Address serverAddress;

    // Your code here...
    private Request request;
    private Reply reply;
    private int seqNum = 0;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public SimpleClient(Address address, Address serverAddress) {
        super(address);
        this.serverAddress = serverAddress;
    }

    @Override
    public synchronized void init() {
        // No initialization necessary
    }

    /* -------------------------------------------------------------------------
        Client Methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command command) {
        // Your code here...
        if (!(command instanceof SingleKeyCommand)) {
            throw new IllegalArgumentException();
        }

        AMOCommand amoCommand = new AMOCommand(command, this.address(), seqNum);

        if (reply != null && reply.result().sequenceNum() == seqNum && reply.result().clientID() == this.address()) {
            notify();
            return;
        }

        request = new Request(amoCommand);
        reply = null;
        //System.out.println("send command: command = " + amoCommand.command().toString() + " | seq = " + seqNum);

        this.send(new Request(amoCommand), serverAddress);
        this.set(new ClientTimer(amoCommand), CLIENT_RETRY_MILLIS);
    }

    @Override
    public synchronized boolean hasResult() {
        // Your code here...
        return reply != null;
    }

    @Override
    public synchronized Result getResult() throws InterruptedException {
        // Your code here...
        while (reply == null) {
            wait();
        }
        seqNum++;

        return reply.result().result();
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private synchronized void handleReply(Reply m, Address sender) {
        // Your code here...
        if (m != null && m.result() != null &&
                seqNum == m.result().sequenceNum() /*&&
                Objects.equal(sender, m.result().serverID())*/) {
            reply = m;
            //System.out.println("received reply: reply = " + reply.result().toString() + " | seq = " + reply.result().sequenceNum());
            notify();
        }
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        // Your code here...
        if (seqNum == t.amoCommand().sequenceNum() && reply == null) {
            this.send(new Request(request.command()), serverAddress);
            this.set(t, CLIENT_RETRY_MILLIS);
        }
    }
}
