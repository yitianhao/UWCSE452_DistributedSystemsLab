package dslabs.primarybackup;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Node;
import dslabs.framework.Result;
import java.io.Serializable;
import java.util.HashMap;
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
    //private AMOApplication<Application> initApplication;
    private View myView;
    private boolean msgForwarding;
    private boolean stateTransferring;
    private int stateTransferSeqNum = 0;
    private StateTransferAck prevStateTransferAck;

    private static class Tuple implements Serializable {
        private Integer seqNum;
        private Result result;

        public Tuple(Integer seqNum, Result result) {
            this.result = result;
            this.seqNum = seqNum;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Tuple tuple = (Tuple) o;
            return seqNum.equals(tuple.seqNum) && result.equals(tuple.result);
        }

        @Override
        public int hashCode() {
            return Objects.hash(seqNum, result);
        }
    }
    private HashMap<Address, Tuple> bookkeeping = new HashMap<>();

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    PBServer(Address address, Address viewServer, Application app) {
        super(address);
        this.viewServer = viewServer;

        // Your code here...
        this.application = new AMOApplication(app);
        //initApplication = application;
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
        if (Objects.equals(myView.primary(), address()) && !stateTransferring) {
            // A. already executed
            if (bookkeeping.containsKey(sender) && bookkeeping.get(sender).seqNum >= m.command().sequenceNum()) {
                AMOResult result = (AMOResult) bookkeeping.get(sender).result;
                send(new Reply(result), sender);
            } else {
                // B. not executed before, no backup
                if (myView.backup() == null) {
                    AMOResult result = application.execute(m.command());
                    bookkeeping.put(sender, new Tuple(m.command().sequenceNum(), result));
                    send(new Reply(result), sender);
                // C. not executed before, has backup
                } else {
                    if (!msgForwarding) {
                        msgForwarding = true;
                        send(new ForwardedRequest(m.command(), sender, myView.viewNum()), myView.backup());
                        set(new ForwardedRequestTimer(m.command(), sender),
                                FORWARDED_RETRY_MILLIS);
                    }
                }
            }
        }
    }

    private void handleViewReply(ViewReply m, Address sender) {
        // Your code here...
        if (myView.viewNum() < m.view().viewNum() && !stateTransferring) {
            // backup dead
            if (Objects.equals(m.view().primary(), address()) && Objects.equals(myView.primary(), address()) && m.view().backup() != null) {
                stateTransfer(m.view());
            // primary dead
            } else if (Objects.equals(m.view().primary(), address()) && m.view().backup() != null && !msgForwarding) {
                stateTransfer(m.view());
            } else {
                myView = m.view();
            }
        }
    }

    // Your code here...
    private void handleForwardedRequest(ForwardedRequest m, Address sender) {
        if (Objects.equals(myView.backup(), address())
                && (!stateTransferring)
                && m.primary_view_num() == myView.viewNum()) {
            AMOResult result = application.execute(m.command());
            send(new BackupAck(m.command(), m.client(), myView.viewNum()), sender);
        }
    }

    private void handleBackupAck(BackupAck m, Address sender) {
        if (msgForwarding && Objects.equals(myView.primary(), address())
                && Objects.equals(sender, myView.backup())
                && myView.viewNum() == m.backup_view_num()) {
            // in the case that backup fails, change a backup, the primary should not accept the backup's ack
            // probably let the client resent a request and handle by the new view instead
            msgForwarding = false;
            if (!stateTransferring) {
                AMOResult result = application.execute(m.command());
                send(new Reply(result), m.client());
                bookkeeping.put(m.client(), new Tuple(m.command().sequenceNum(), result));
            }
        }
    }

    private void handleTransferredState(TransferredState m, Address sender) {
        // I am the future backup
        //System.out.println(stateTransferSeqNum + " | " + m.stateTransferSeqNum());
        if (Objects.equals(m.view().backup(), address())) {
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
//        if (myView.viewNum() < m.view().viewNum() && !stateTransferring) {
//            if (Objects.equals(m.view().primary(), address()) && m.view().backup() != null && !msgForwarding) {
        // I am the future primary
        if (stateTransferring
                && Objects.equals(m.view().primary(), address())
                && myView.viewNum() < m.view().viewNum()
                && stateTransferSeqNum == m.stateTransferSeqNum()) {
            stateTransferring = false;
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
        if (msgForwarding && Objects.equals(myView.primary(), address())
                && myView.backup() != null) {
            this.send(new ForwardedRequest(t.amoCommand(), t.client(), myView.viewNum()), myView.backup());
            this.set(t, FORWARDED_RETRY_MILLIS);
        }
        //System.out.println("onForwardedRequestTimer");
    }

    private void onTransferredStateTimer(TransferredStateTimer t) {
        View newView = t.newView();
        if (stateTransferring
                && Objects.equals(newView.primary(), address())
                && newView.backup() != null) {
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
    private void stateTransfer(View view) {
        stateTransferring = true;
        send(new TransferredState(application, view, stateTransferSeqNum), view.backup());
        set(new TransferredStateTimer(view), TRANSFERRED_RETRY_MILLIS);
    }

}
