package dslabs.primarybackup;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Node;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import lombok.ToString;

import static dslabs.primarybackup.ClientTimer.CLIENT_RETRY_MILLIS;
import static dslabs.primarybackup.PingTimer.PING_MILLIS;
import static dslabs.primarybackup.ViewServer.STARTUP_VIEWNUM;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBServer extends Node {
    private final Address viewServer;

    // Your code here...
    private final AMOApplication<Application> application;
    private View viewServerView;
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
    }

    @SneakyThrows
    @Override
    public void init() {
        // Your code here...
        this.send(new Ping(myView.viewNum()), viewServer);
        this.set(new PingTimer(), PING_MILLIS);
        System.out.println("server init called");
//        while (viewServerView == null) {
//            wait();
//        }
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handleRequest(Request m, Address sender) {
        // Your code here...
        AMOResult result = application.execute(m.command());
        send(new Reply(result), sender);
    }

    private void handleViewReply(ViewReply m, Address sender) {
        // Your code here...
        viewServerView = m.view();
        System.out.println("server viewServerView: " + viewServerView);
    }

    // Your code here...

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private void onPingTimer(PingTimer t) {
        // Your code here...
        System.out.println(myView.toString());
        this.send(new Ping(myView.viewNum()), viewServer);
        this.set(t, PING_MILLIS);
    }

    // Your code here...

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...
}
