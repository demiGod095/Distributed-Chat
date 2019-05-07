package activitystreamer.server;

import java.util.UUID;

/** Contains the data for a graph edge, representing a connection to another server */
public class ServerEdge {

	private Identifier ident;
	private UUID uuid;
	
	public ServerEdge() {
		uuid = UUID.randomUUID();
	}
}
