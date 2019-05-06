package activitystreamer.server;

import activitystreamer.util.Strings;

public class BasicConnectionState extends ConnectionState{

    @Override
    public String toString() { 
        return Strings.BASIC;
    } 
}
