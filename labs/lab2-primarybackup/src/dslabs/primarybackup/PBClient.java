package dslabs.primarybackup;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import lombok.ToString;

import static dslabs.primarybackup.ClientTimer.CLIENT_RETRY_MILLIS;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBClient extends Node implements Client {
    private final Address viewServer;

    // Your code here...
    private Request request;
    private Reply reply;
    private int seqNum = 0;
    private View currentView;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PBClient(Address address, Address viewServer) {
        super(address);
        this.viewServer = viewServer;
    }

    @Override
    public synchronized void init() {
        // Your code here...
        this.send(new GetView(), viewServer);
    }

    /* -------------------------------------------------------------------------
        Client Methods
       -----------------------------------------------------------------------*/
    @SneakyThrows
    @Override
    public synchronized void sendCommand(Command command) {
        // Your code here...
//        if (!(command instanceof SingleKeyCommand)) {
//            throw new IllegalArgumentException();
//        }

        AMOCommand amoCommand = new AMOCommand(command, this.address(), seqNum);

        request = new Request(amoCommand);
        reply = null;

        while (currentView == null) {
            wait();
            this.set(new ClientTimer(amoCommand), CLIENT_RETRY_MILLIS);
        }
        this.send(new Request(amoCommand), currentView.primary());
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
        if (m != null && m.result() != null && seqNum == m.result().sequenceNum()) {
            reply = m;
            notify();
        }
    }

    private synchronized void handleViewReply(ViewReply m, Address sender) {
        // Your code here...
        if (currentView != null) {
            currentView = m.view();
            notify();
        }
    }

    // Your code here...

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        // Your code here...
        if (seqNum == t.amoCommand().sequenceNum() && reply == null) {
            this.send(new Request(request.command()), currentView.primary());
            this.set(t, CLIENT_RETRY_MILLIS);
        } else if (currentView == null) {
            this.send(new GetView(), viewServer);
            this.set(t, CLIENT_RETRY_MILLIS);
        }
        // the client should not talk to the ViewServer
        // when the current primary seems to be dead (i.e., on ClientTimer)???
    }
}
