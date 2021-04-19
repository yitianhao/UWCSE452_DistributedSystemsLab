package dslabs.primarybackup;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Node;
import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import lombok.ToString;

import static dslabs.primarybackup.ClientTimer.CLIENT_RETRY_MILLIS;
import static dslabs.primarybackup.ForwardRequestTimer.FORWARD_RETRY_MILLIS;
import static dslabs.primarybackup.PingTimer.PING_MILLIS;
import static dslabs.primarybackup.ViewServer.STARTUP_VIEWNUM;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBServer extends Node {
    private final Address viewServer;

    // Your code here...
    private AMOApplication<Application> application;
    //private View viewServerView;
    private View myView;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    PBServer(Address address, Address viewServer, Application app) {
        super(address);
        this.viewServer = viewServer;

        // Your code here...
        this.application = new AMOApplication(app);
        myView = new View(STARTUP_VIEWNUM, null, null);
        //viewServerView = new View(STARTUP_VIEWNUM, null, null);
    }

    @SneakyThrows
    @Override
    public void init() {
        // Your code here...
        send(new Ping(ViewServer.STARTUP_VIEWNUM), viewServer);
        set(new PingTimer(), PING_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handleRequest(Request m, Address sender) {
        // Your code here...
//        if (!Objects.equals(sender, myView.primary())) { // send by client
//            forwardToBackup(m.command());
//            AMOResult result = application.execute(m.command());
//            send(new Reply(result), sender);
//        } else {
            AMOResult result = application.execute(m.command());
            send(new Reply(result), sender);
        // }
    }

    private void handleViewReply(ViewReply m, Address sender) {
        // Your code here...
        myView = m.view();
    }

    // Your code here...
    private void handleTransferredState(TransferredState m, Address sender) {
        this.application = (AMOApplication<Application>) m.application();
    }

    private synchronized void handleReply(Reply m, Address sender) {
        // Your code here...
        if (m != null && m.result() != null) {

        }
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private void onPingTimer(PingTimer t) {
        // Your code here...
        this.send(new Ping(myView.viewNum()), viewServer);
        this.set(t, PING_MILLIS);
    }

    // Your code here...
    private void onForwardRequestTimer(ForwardRequestTimer t) {
        this.send(new Request(t.amoCommand()), myView.backup());
        this.set(t, FORWARD_RETRY_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...
    private void forwardToBackup(AMOCommand amoCommand) {
        this.send(new Request(amoCommand), myView.backup());
        this.set(new ForwardRequestTimer(amoCommand), FORWARD_RETRY_MILLIS);
    }
}
