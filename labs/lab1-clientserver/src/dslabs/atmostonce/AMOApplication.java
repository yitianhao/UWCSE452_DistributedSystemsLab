package dslabs.atmostonce;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore;
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
    private HashMap<String, Result> bookkeeping = new HashMap<>();
    public AMOApplication(T app, Address address) {
        application = app;
    }

    @Override
    public AMOResult execute(Command command) {
        if (!(command instanceof AMOCommand)) {
            throw new IllegalArgumentException();
        }

        AMOCommand amoCommand = (AMOCommand) command;

        // Your code here...
        String uid = amoCommand.clientID() + "#" + amoCommand.sequenceNum();
        if (alreadyExecuted(uid)) {
            //System.out.println("duplicate seqNum = " + amoCommand.sequenceNum());
            return new AMOResult(bookkeeping.get(uid), amoCommand.clientID(), amoCommand.sequenceNum());
        } else {
            Result res = application.execute(amoCommand.command());
            bookkeeping.put(uid, res);
            //System.out.println("did** : " + res.toString());
            return new AMOResult(res, amoCommand.clientID(), amoCommand.sequenceNum());
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

    public boolean alreadyExecuted(String uid) {
        // Your code here...
        return bookkeeping.containsKey(uid);
    }
}
