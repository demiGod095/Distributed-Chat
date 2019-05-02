package activitystreamer.server;

import java.util.HashSet;

public class PendingClientRegistration {
    private Connection client;
    private int pendingServers;

    public PendingClientRegistration(Connection client, int knownServers) {
        this.client = client;
        this.pendingServers = knownServers;
    }

    public Connection getClient() {
        return client;
    }

    public void serverApprovedRegistration() {
        pendingServers--;
    }

    public boolean allServersApprovedRegistration() {
        return pendingServers <= 0;
    }
}
