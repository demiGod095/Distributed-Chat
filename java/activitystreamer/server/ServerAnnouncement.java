package activitystreamer.server;

public class ServerAnnouncement implements Comparable<ServerAnnouncement> {

    private String id;
    private String hostname;
    private long port;
    private long load;


    public ServerAnnouncement(String id, String hostname, long port, long load) {
        this.id = id;
        this.hostname = hostname;
        this.port = port;
        this.load = load;
    }

    public String getId() {
        return id;
    }

    public String getHostname() {
        return hostname;
    }

    public long getPort() {
        return port;
    }

    public long getLoad() {
        return load;
    }

    public void setLoad(long load) {
        this.load = load;
    }


    @Override
    public int compareTo(ServerAnnouncement s) {
        return (this.getId()).compareTo(s.getId());
    }

}
