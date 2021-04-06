package dslabs.atmostonce;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Objects;
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
    private Address serverAddress;
    public AMOApplication(T app, Address address) {
        application = app;
        // this.serverAddress = address;
    }

    @Override
    public AMOResult execute(Command command) {
        if (!(command instanceof AMOCommand)) {
            throw new IllegalArgumentException();
        }

        AMOCommand amoCommand = (AMOCommand) command;

        // Your code here...
        if (alreadyExecuted(amoCommand)) {
            return new AMOResult(bookkeeping.get(amoCommand.clientID()).result, amoCommand.sequenceNum());
        } else {
            Result res = application.execute(amoCommand.command());
            bookkeeping.put(amoCommand.clientID(), new Tuple(amoCommand.sequenceNum(), res));
            return new AMOResult(res, amoCommand.sequenceNum());
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
