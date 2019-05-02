package activitystreamer.server;

import java.util.Comparator;

public class ServerLoadComparator implements Comparator<ServerAnnouncement> {

    @Override
    public int compare(ServerAnnouncement s1, ServerAnnouncement s2) {
        return Long.compare(s1.getLoad(), s2.getLoad());
    }
}
