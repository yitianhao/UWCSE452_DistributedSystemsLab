package dslabs.atmostonce;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import java.io.Serializable;
import java.util.HashMap;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@EqualsAndHashCode
@ToString
@RequiredArgsConstructor
public final class AMOApplication<T extends Application>
        implements Application {
    @Getter @NonNull private final T application;

    // Your code here...
    private static class Tuple<I, R> implements Serializable {
        private I seqNum;
        private R result;

        public Tuple(I seqNum, R result) {
            this.result = result;
            this.seqNum = seqNum;
        }
    }
    private HashMap<Address, Tuple<Integer, Result>> bookkeeping = new HashMap<>();
    private Address serverAddress;
    public AMOApplication(T app, Address address) {
        application = app;
        this.serverAddress = address;
    }

    @Override
    public AMOResult execute(Command command) {
        if (!(command instanceof AMOCommand)) {
            throw new IllegalArgumentException();
        }

        AMOCommand amoCommand = (AMOCommand) command;

        // Your code here...
        //String uid = amoCommand.clientID() + "#" + amoCommand.sequenceNum();
        if (alreadyExecuted(amoCommand)) {
            // System.out.println("----duplicate seqNum = " + amoCommand.sequenceNum());
            // bookkeeping.put(amoCommand.clientID(), null);
            return new AMOResult(bookkeeping.get(amoCommand.clientID()).result, this.serverAddress, amoCommand.clientID(),amoCommand.sequenceNum());
        } else {
            Result res = application.execute(amoCommand.command());
            bookkeeping.put(amoCommand.clientID(), new Tuple<>(amoCommand.sequenceNum(), res));
            //System.out.println("did** : " + res.toString());
            return new AMOResult(res, this.serverAddress, amoCommand.clientID(), amoCommand.sequenceNum());
        }
    }

    public Result executeReadOnly(Command command) {
        if (!command.readOnly()) {
            throw new IllegalArgumentException();
        }

        if (command instanceof AMOCommand) {
            return execute(command);
        }

        return application.execute(command);
    }

    public boolean alreadyExecuted(AMOCommand command) {
        // Your code here...
        return bookkeeping.containsKey(command.clientID()) &&
                bookkeeping.get(command.clientID()).seqNum >= command.sequenceNum();
    }
}
