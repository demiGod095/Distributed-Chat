package activitystreamer.server;

import activitystreamer.util.Strings;

public class RejectedConnectionState extends ConnectionState {

    @Override
    public String toString() { 
        return Strings.REJECTED;
    } 
}
