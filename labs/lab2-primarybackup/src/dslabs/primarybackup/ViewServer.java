package dslabs.primarybackup;

import dslabs.framework.Address;
import dslabs.framework.Node;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.primarybackup.PingCheckTimer.PING_CHECK_MILLIS;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class ViewServer extends Node {
    static final int STARTUP_VIEWNUM = 0;
    private static final int INITIAL_VIEWNUM = 1;

    // Your code here...
    private static View currentView;
    private static HashSet<Address> idleServers;
    private static HashSet<Address> secondMostRecentPingedServers;
    private static HashSet<Address> mostRecentPingedServers;
    private static int primaryViewNum;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public ViewServer(Address address) {
        super(address);
    }

    @Override
    public void init() {
        set(new PingCheckTimer(), PING_CHECK_MILLIS);
        // Your code here...
        currentView = new View(STARTUP_VIEWNUM, null, null);
        idleServers = new HashSet<>();
        secondMostRecentPingedServers = new HashSet<>();
        mostRecentPingedServers = new HashSet<>();
        primaryViewNum = STARTUP_VIEWNUM;
        System.out.println("viewserver init called | current view" + currentView.toString());
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    // A Ping lets the ViewServer know that the server is alive;
    // informs the server of the current view;
    // and informs the ViewServer of the most recent view that the server knows about.
    private void handlePing(Ping m, Address sender) {
        // Your code here...
        // A. first start up
        System.out.println("viewserver: entered handlePing");
        if (m.viewNum() == STARTUP_VIEWNUM && currentView.primary() == null) {
            currentView = new View(INITIAL_VIEWNUM, sender, null);
        // B. only until the primary from the current view acknowledges that it is operating in the current view,
        // can the ViewServer change the current view
            System.out.println("first server started");
        } else if (Objects.equals(sender, currentView.primary())) {
            primaryViewNum = m.viewNum();
            if (currentView.backup() == null && primaryViewNum == currentView.viewNum()) viewTransition("backup null");
        } else {
            if (!Objects.equals(sender, currentView.backup())) idleServers.add(sender);
            if (currentView.backup() == null && primaryViewNum == currentView.viewNum()) viewTransition("backup null");
        }
        mostRecentPingedServers.add(sender);
        send(new ViewReply(currentView), sender);
    }

    private void handleGetView(GetView m, Address sender) {
        // Your code here...
        System.out.println("viewserver reply with " + currentView.toString());
        send(new ViewReply(currentView), sender);
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private void onPingCheckTimer(PingCheckTimer t)
            throws InterruptedException {
        // Your code here...
        secondMostRecentPingedServers.removeIf(mostRecentPingedServers::contains);  // what left are dead
        idleServers.removeIf(secondMostRecentPingedServers::contains);  // remove dead servers
        // if primary or backup dead...
        if (primaryViewNum == currentView.viewNum() && secondMostRecentPingedServers.contains(currentView.primary())) {
            viewTransition("primary fails");
        }else if (primaryViewNum == currentView.viewNum() && secondMostRecentPingedServers.contains(currentView.backup())) {
            viewTransition("backup fails");
        }
        secondMostRecentPingedServers.clear();
        secondMostRecentPingedServers.addAll(mostRecentPingedServers);
        mostRecentPingedServers.clear();
        set(t, PING_CHECK_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...

    private void viewTransition(String msg) {
        if (msg.equals("backup null") && !idleServers.isEmpty()) {
            Address idleServer = idleServers.iterator().next();
            currentView = new View(currentView.viewNum() + 1, currentView.primary(), idleServer);
            idleServers.remove(idleServer);
        } else if (msg.equals("primary fails")) {
            if (idleServers.isEmpty()) {
                // what is backup is null?
                // the ViewServer is forced to stick with current view, otherwise we would lose the work we've done so far
                if (currentView.backup() != null) {
                    currentView = new View(currentView.viewNum() + 1, currentView.backup(), null);
                }
            } else {
                Address idleServer = idleServers.iterator().next();
                currentView = new View(currentView.viewNum() + 1, currentView.backup(), idleServer);
                idleServers.remove(idleServer);
            }
        } else if (msg.equals("backup fails")) {
            if (idleServers.isEmpty()) {
                currentView = new View(currentView.viewNum() + 1, currentView.primary(), null);
            } else {
                Address idleServer = idleServers.iterator().next();
                currentView = new View(currentView.viewNum() + 1, currentView.primary(), idleServer);
                idleServers.remove(idleServer);
            }
        }
    }
}
