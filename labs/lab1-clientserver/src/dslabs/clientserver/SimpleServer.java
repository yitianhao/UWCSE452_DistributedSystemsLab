package dslabs.clientserver;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Node;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Simple server that receives requests and returns responses.
 *
 * See the documentation of {@link Node} for important implementation notes.
 */
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class SimpleServer extends Node {
    // Your code here...
    private final AMOApplication<Application> application;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public SimpleServer(Address address, Application app) {
        super(address);

        // Your code here...
        this.application = new AMOApplication(app, address);
    }

    @Override
    public void init() {
        // No initialization necessary
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handleRequest(Request m, Address sender) {
        // Your code here...
        //System.out.println("in server: command = " + m.command().toString())
        if (m.command().readOnly()) {
            Result result = application.executeReadOnly(m.command());
            send(new Reply(new AMOResult(result, sender, m.command().clientID(), m.command().sequenceNum())), sender);
        } else {
            AMOResult result = application.execute(m.command());
            send(new Reply(result), sender);
        }
    }
}
