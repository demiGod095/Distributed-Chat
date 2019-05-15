package activitystreamer.server;

import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Contains the data for a graph edge, representing a connection to another
 * server. This contains a weight value, along with an Identifier for the
 * fragment, alongside a branch state.
 */
public class ServerEdge extends Edge implements Comparable<ServerEdge> {
	private static final Logger log = LogManager.getLogger();

	private Identifier ident;
	private int weight;
	private EdgeState edgeState;

	public ServerEdge(UUID uuid1, UUID uuid2, int lag) {
		ident = new Identifier(uuid1, uuid2);
		this.weight = lag;
		edgeState = EdgeState.Basic;
	}

	public Identifier getIdentifier() {
		return ident;
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}

	public int getWeight() {
		return weight;
	}

	public void setEdgeState(EdgeState state) {
		if (state == EdgeState.Branch) {
			log.info("A MST Branch between " + ident.getUUID1().toString() + " and  " + ident.getUUID2().toString()
					+ " has been made, or this node has become aware of it");
		}
		edgeState = state;
	}

	public EdgeState getEdgeState() {
		return edgeState;
	}

	@Override
	public int compareTo(ServerEdge arg0) {
		// Compares with the branches weight. If we have a collision, we compare the
		// branches identifier.
		int weightCmp = Integer.compare(weight, arg0.weight);
		if (weightCmp == 0) {
			return ident.compareTo(arg0.getIdentifier());
		}
		return weightCmp;
	}
}
