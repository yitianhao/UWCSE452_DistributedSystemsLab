package dslabs.primarybackup;

import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Message;
import lombok.Data;

/* -------------------------------------------------------------------------
    ViewServer Messages
   -----------------------------------------------------------------------*/
@Data
class Ping implements Message {
    private final int viewNum;
}

@Data
class GetView implements Message {
}

@Data
class ViewReply implements Message {
    private final View view;
}

/* -------------------------------------------------------------------------
    Primary-Backup Messages
   -----------------------------------------------------------------------*/
@Data
class Request implements Message {
    // Your code here...
    private final AMOCommand command;
}

@Data
class Reply implements Message {
    // Your code here...
    private final AMOResult result;
}

// Your code here...

@Data
class ForwardedRequest implements Message {
    private final AMOCommand command;
    private final Address client;
}

@Data
class BackupAck implements Message {
    private final AMOCommand command;
    private final Address client;
}

@Data
class TransferredState implements Message {
    private final Application application;
    private final View view;
}

@Data
class StateTransferAck implements Message {
}
