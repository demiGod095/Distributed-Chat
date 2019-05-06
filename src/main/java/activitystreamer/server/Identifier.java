package activitystreamer.server;

import java.util.UUID;

/**
 * Identifies an Edge in the network. Uses the port number and ip address of the
 * connection, to have a unique edge for every outgoing edge in the tree. Stores
 * the pairs in a consistent and deterministic order, so that there is agreement
 * on the ordering
 */
public class Identifier {

	UUID uuid1;
	UUID uuid2;

	public Identifier(UUID a, UUID b) {
		if (a.compareTo(b) > 0) {
			uuid1 = b;
			uuid2 = a;
		} else {
			uuid1 = a;
			uuid2 = b;
		}
	}

	public UUID getUUID1() {
		return uuid1;
	}

	public UUID getUUID2() {
		return uuid2;
	}

	@Override
	public String toString() {
		return "UUID 1 " + uuid1.toString() + "\nUUID2 " + uuid2.toString();
	}
}
