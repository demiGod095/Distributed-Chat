package activitystreamer.server;

import java.util.UUID;

/** Contains the data for a graph edge, representing a connection to another server */
public class ServerEdge extends Edge implements Comparable <ServerEdge>{

	private Identifier ident;
	int lag;
	private EdgeState edgeState;
	
	public ServerEdge(UUID uuid1, UUID uuid2, int lag) {
		ident = new Identifier(uuid1, uuid2);
		this.lag = lag;
		edgeState = EdgeState.Basic;
	}
	
	public Identifier getIdentifier() {
		return ident;
	}
	
	public void setLag(int lag) {
		this.lag = lag;
	}
	
	public int getLag() {
		return lag;
	}
	
	public void setEdgeState(EdgeState state) {
		edgeState = state;
	}
	
	public EdgeState getEdgeState() {
		return edgeState;
	}

	@Override
	public int compareTo(ServerEdge arg0) {
		// TODO Auto-generated method stub
		int lagCmp = Integer.compare(lag, arg0.lag);
		if (lagCmp == 0) {
			return ident.compareTo(arg0.getIdentifier());
		}
		return lagCmp;
	} 
}
