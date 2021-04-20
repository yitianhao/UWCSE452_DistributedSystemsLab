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
import static dslabs.primarybackup.ForwardedRequestTimer.FORWARDED_RETRY_MILLIS;
import static dslabs.primarybackup.PingTimer.PING_MILLIS;
import static dslabs.primarybackup.TransferredStateTimer.TRANSFERRED_RETRY_MILLIS;
import static dslabs.primarybackup.ViewServer.STARTUP_VIEWNUM;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBServer extends Node {
    private final Address viewServer;

    // Your code here...
    private AMOApplication<Application> application;
    //private View viewServerView;
    private View myView;
    private BackupAck backupAck;
    private boolean stateTransferDone;

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
        if (Objects.equals(myView.primary(), address())) {
            if (myView.backup() != null) {
                backupAck = null;
                send(new ForwardedRequest(m.command(), sender), myView.backup());
                set(new ForwardedRequestTimer(m.command(), sender), FORWARDED_RETRY_MILLIS);
            } else {
                AMOResult result = application.execute(m.command());
                send(new Reply(result), sender);
            }
        } else {
            // do nothing or error message
        }
    }

    private void handleViewReply(ViewReply m, Address sender) {
        // Your code here...
        View oldView = myView;
        myView = m.view();
        if (Objects.equals(myView.primary(), address())
                && myView.backup() != null && !Objects.equals(oldView.backup(), myView.backup())) {
            send(new TransferredState(application), myView.backup());
            set(new TransferredStateTimer(), TRANSFERRED_RETRY_MILLIS);
        }
    }

    // Your code here...
    private void handleForwardedRequest(ForwardedRequest m, Address sender) {
        if (Objects.equals(myView.backup(), address())) {
            AMOResult result = application.execute(m.command());
            send(new BackupAck(m.command(), m.client()), sender);
        }
    }

    private void handleBackupAck(BackupAck m, Address sender) {
        if (Objects.equals(myView.primary(), address())) {
            backupAck = m;
            AMOResult result = application.execute(m.command());
            send(new Reply(result), m.client());
        }
    }

    private void handleTransferredState(TransferredState m, Address sender) {
        this.application = (AMOApplication<Application>) m.application();
        send(new StateTransferAck(), sender);
    }

    private void handleStateTransferAck(StateTransferAck m, Address sender) {
        stateTransferDone = true;
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
    private void onForwardedRequestTimer(ForwardedRequestTimer t) {
        if (backupAck == null) {
            this.send(new ForwardedRequest(t.amoCommand(), t.client()), myView.backup());
            this.set(t, FORWARDED_RETRY_MILLIS);
        }
    }

    private void onTransferredStateTimer(TransferredStateTimer t) {
        if (!stateTransferDone) {
            this.send(new TransferredState(application), myView.backup());
            this.set(t, TRANSFERRED_RETRY_MILLIS);
        }
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...

}
