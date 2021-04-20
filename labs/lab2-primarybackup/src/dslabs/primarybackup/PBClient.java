package dslabs.primarybackup;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import dslabs.framework.Timer;
import dslabs.kvstore.KVStore.SingleKeyCommand;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import lombok.ToString;

import static dslabs.primarybackup.ClientTimer.CLIENT_RETRY_MILLIS;
import static dslabs.primarybackup.PingTimer.PING_MILLIS;
import static dslabs.primarybackup.PrimarySeemsDeadTimer.PRIMARY_SEEMS_DEAD_MILLIS;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBClient extends Node implements Client {
    private final Address viewServer;

    // Your code here...
    private Request request;
    private Reply reply;
    private int seqNum = 0;
    private View currentView;
    private boolean viewRelied;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PBClient(Address address, Address viewServer) {
        super(address);
        this.viewServer = viewServer;
    }

    @SneakyThrows
    @Override
    public synchronized void init() {
        // Your code here...
        this.send(new GetView(), viewServer);
        AMOCommand amoCommand = new AMOCommand(null, address(), seqNum); // need improvement
        this.set(new ClientTimer(amoCommand), CLIENT_RETRY_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Client Methods
       -----------------------------------------------------------------------*/
    @SneakyThrows
    @Override
    public synchronized void sendCommand(Command command) {
        // Your code here...
        if (!(command instanceof SingleKeyCommand)) {
            throw new IllegalArgumentException();
        }

        AMOCommand amoCommand = new AMOCommand(command, this.address(), seqNum);

        request = new Request(amoCommand);
        reply = null;

        if (currentView != null && currentView.primary() != null) {
            this.send(new Request(amoCommand), currentView.primary());
            this.set(new ClientTimer(amoCommand), CLIENT_RETRY_MILLIS);
            this.set(new PrimarySeemsDeadTimer(), PRIMARY_SEEMS_DEAD_MILLIS);
        }
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
        if (m != null && m.view() != null) {
            if (currentView != null) {
                if (currentView.viewNum() < m.view().viewNum()) {
                    currentView = m.view();
                }
            } else {
                currentView = m.view();
            }
            viewRelied = true;
        }
    }

    // Your code here...

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        // Your code here...
        if (currentView == null || currentView.primary() == null || !viewRelied) {
            this.send(new GetView(), viewServer);
            this.set(t, CLIENT_RETRY_MILLIS);
        } else if (seqNum == t.amoCommand().sequenceNum() && reply == null) {
            this.send(new Request(request.command()), currentView.primary());
            this.set(t, CLIENT_RETRY_MILLIS);
        }
        // the client should not talk to the ViewServer
        // when the current primary seems to be dead (i.e., on ClientTimer)???
    }

    private synchronized void onPrimarySeemsDeadTimer(PrimarySeemsDeadTimer t) {
        viewRelied = false;
        this.send(new GetView(), viewServer);
        AMOCommand amoCommand = new AMOCommand(null, address(), seqNum); // need improvement
        this.set(new ClientTimer(amoCommand), CLIENT_RETRY_MILLIS);
    }
}
