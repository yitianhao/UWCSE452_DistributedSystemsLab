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
    private AMOApplication<Application> initApplication;
    private View myView;
    private BackupAck backupAck;
    private boolean backupAckStarted;
    private boolean stateTransferDone;
    private boolean stateTransferStarted;
    private int stateTransferSeqNum = 0;
    private StateTransferAck prevStateTransferAck;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    PBServer(Address address, Address viewServer, Application app) {
        super(address);
        this.viewServer = viewServer;

        // Your code here...
        this.application = new AMOApplication(app);
        initApplication = application;
        myView = new View(STARTUP_VIEWNUM, null, null);
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
        if (Objects.equals(myView.primary(), address())
                && (stateTransferDone || !stateTransferStarted) // cannot accept new client request if state transfer not done
                && (backupAck != null || !backupAckStarted)) { // cannot accept new client request if previous request not done
            if (myView.backup() != null) {
                backupAck = null;
                backupAckStarted = true;
                send(new ForwardedRequest(m.command(), sender, myView.viewNum()), myView.backup());
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
        if (myView.viewNum() >= m.view().viewNum()) {

        } else {
            if ((stateTransferDone || !stateTransferStarted)) {
                if (Objects.equals(m.view().primary(), address()) && m.view().backup() != null && (backupAck != null || !backupAckStarted)) {
                    stateTransferDone = false;
                    stateTransferStarted = true;
                    //stateTransferSeqNum++;
                    send(new TransferredState(application, m.view(), stateTransferSeqNum), m.view().backup());
                    set(new TransferredStateTimer(m.view()), TRANSFERRED_RETRY_MILLIS);
                } else {
                    myView = m.view();
                }
            }
        }
    }

    // Your code here...
    private void handleForwardedRequest(ForwardedRequest m, Address sender) {
        if (Objects.equals(myView.backup(), address())
                && (stateTransferDone || !stateTransferStarted)
                && m.primary_view_num() == myView.viewNum()) {
            backupAck = null;
            AMOResult result = application.execute(m.command());
            send(new BackupAck(m.command(), m.client()), sender);
        }
    }

    private void handleBackupAck(BackupAck m, Address sender) {
        if (backupAckStarted && backupAck == null
                && Objects.equals(myView.primary(), address())) {
            // in the case that backup fails, change a backup, the primary should not accept the backup's ack
            // probably let the client resent a request and handle by the new view instead
            if (Objects.equals(sender, myView.backup())) {
                backupAck = m;
                backupAckStarted = false;
                if ((!stateTransferStarted || stateTransferDone)) {
                    AMOResult result = application.execute(m.command());
                    send(new Reply(result), m.client());
                }
            }
        }
    }

    private void handleTransferredState(TransferredState m, Address sender) {
        // I am the future backup
        //System.out.println(stateTransferSeqNum + " | " + m.stateTransferSeqNum());
        if (Objects.equals(m.view().backup(), address())
                && !Objects.equals(myView.primary(), address())) {
            if (stateTransferSeqNum < m.stateTransferSeqNum() || prevStateTransferAck == null) {
                this.application = (AMOApplication<Application>) m.application();
                myView = m.view();
                stateTransferSeqNum = m.stateTransferSeqNum();
                prevStateTransferAck = new StateTransferAck(m.view(), stateTransferSeqNum);
                send(new StateTransferAck(m.view(), stateTransferSeqNum), sender);
            } else {
                send(prevStateTransferAck, sender);
            }
        }
    }

    private void handleStateTransferAck(StateTransferAck m, Address sender) {
        // I am the future primary
        if (stateTransferStarted && !stateTransferDone
                && Objects.equals(m.view().primary(), address())
                && myView.viewNum() < m.view().viewNum()
                && stateTransferSeqNum == m.stateTransferSeqNum()) {
            stateTransferDone = true;
            stateTransferStarted = false;
            stateTransferSeqNum++;
            myView = m.view();
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
    private void onForwardedRequestTimer(ForwardedRequestTimer t) {
        if (backupAckStarted && backupAck == null
                && Objects.equals(myView.primary(), address())
                && myView.backup() != null) {
            backupAck = null;
            this.send(new ForwardedRequest(t.amoCommand(), t.client(), myView.viewNum()), myView.backup());
            this.set(t, FORWARDED_RETRY_MILLIS);
        }
        //System.out.println("onForwardedRequestTimer");
    }

    private void onTransferredStateTimer(TransferredStateTimer t) {
        View newView = t.newView();
        if (stateTransferStarted && !stateTransferDone
                && Objects.equals(newView.primary(), address())
                && newView.backup() != null) {
            stateTransferDone = false;
            stateTransferStarted = true;
            this.send(new TransferredState(application, newView, stateTransferSeqNum), newView.backup());
            this.set(t, TRANSFERRED_RETRY_MILLIS);
        }
//        System.out.println("onTransferredStateTimer | newView.backup() = " + newView.backup() +
//                " | I am new primary : " + Objects.equals(newView.primary(), address()) +
//                " | not finished transfer " + (stateTransferStarted && !stateTransferDone));
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...

}
