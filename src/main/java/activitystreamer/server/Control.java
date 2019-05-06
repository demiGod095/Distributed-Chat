package activitystreamer.server;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import activitystreamer.util.Settings;
import activitystreamer.util.Strings;

public class Control extends Thread {

	private static final Logger log = LogManager.getLogger();
	// For Undetermined Connections
	private static ArrayList<Connection> connections;

	// For Server Connections
	private static ArrayList<Connection> serverConnections;

	// For Client Connections
	private static ArrayList<Connection> clientConenctions;

	private Connection inBranch = null;
	private Connection bestEdge = null;
	private int bestWeight = -1;

	private int level;
	private NodeState state = null;
	private int foundCount = 0;

	private static boolean term = false;
	private static Listener listener;

	private UUID uuid;
	private Identifier fragmentIdentifier;

	protected static Control control = null;

	public static Control getInstance() {
		if (control == null) {
			control = new Control();
		}
		return control;
	}

	public Control() {
		connections = new ArrayList<Connection>();
		serverConnections = new ArrayList<Connection>();
		clientConenctions = new ArrayList<Connection>();

		uuid = UUID.randomUUID();

		// Initially the node is just a fragment on its own. To represent this, we
		// repeat the UUID twice.
		fragmentIdentifier = new Identifier(uuid, uuid);

		// start the listener
		try {
			listener = new Listener();
			initiateRemoteConnections();
		} catch (IOException e1) {
			log.fatal("failed to startup a listening thread: " + e1);
			System.exit(-1);
		}
	}

	/** Makes a Connection to another server, if a remote hostname is supplied */
	public void initiateRemoteConnections() {
		if (Settings.getRemoteHostname() != null) {
			try {
				Connection outgoing = outgoingConnection(
						new Socket(Settings.getRemoteHostname(), Settings.getRemotePort()));
				outgoing.setConnectionInformation(new ServerConnectionInformation());
				sendLagAgreement(outgoing);
				sendUUID(outgoing);

			} catch (IOException e) {
				log.error("failed to make connection to " + Settings.getRemoteHostname() + ":"
						+ Settings.getRemotePort() + " :" + e);
				System.exit(-1);
			}
		}
	}

	/**
	 * A new outgoing connection has been established, and a reference is returned
	 * to it
	 */
	private Connection outgoingConnection(Socket s) throws IOException {
		Connection c = new Connection(s);
		JSONObject jobj = new JSONObject();
		JSONObject level2 = new JSONObject();
		jobj.put(Strings.CONNECTION_TYPE, Strings.SERVER);
		c.writeMsg(jobj.toJSONString());
		serverConnections.add(c);
		return c;
	}

	/**
	 * Perform the wakeup, initializing the the state varibles, as per GHS
	 * algorithm.
	 * 
	 */
	private void wakeup() {
		level = 0;
		state = new Found();
		foundCount = 0;

		if (serverConnections.size() == 0) {
			log.error("No Connections to any outgoing server");
			return;
		}

		// Search for the edge with the lowest weight
		Connection current = serverConnections.get(0);
		for (Connection serverCon : serverConnections) {
			ServerConnectionInformation serverInfo = (ServerConnectionInformation) serverCon.getConnectionInformation();
			ServerConnectionInformation currentInfo = (ServerConnectionInformation) current.getConnectionInformation();
			if (serverInfo.getLag() < currentInfo.getLag()) {
				current = serverCon;
			}
		}
		// Send a Connect message to that branch
		sendConnect(current);
	}

	/**
	 * Send a Connect Message.
	 * 
	 * @param serverCon the connection to which to send the connect message to
	 * 
	 */
	private void sendConnect(Connection serverCon) {
		JSONObject jobj = new JSONObject();
		jobj.put(Strings.CONNECT, level);
		serverCon.writeMsg(jobj.toJSONString());
	}

	private void receiveConnect(Connection serverCon, JSONObject msgJSON) {
		int level = (int) (long) msgJSON.get(Strings.CONNECT);

		if (state == null) {
			wakeup();
		}

		ServerConnectionInformation serverConI = (ServerConnectionInformation) serverCon.getConnectionInformation();
		if (level < this.level) {

			log.debug("Absorb Nodes " + serverConI.getIdentifier().getUUID1() + " and "
					+ serverConI.getIdentifier().getUUID2());
			serverCon.setConnectionState(new BranchConnectionState());
			sendInitiate(serverCon, this.level, fragmentIdentifier, state);
			if (state instanceof Find) {
				foundCount++;
			}
			return;
		} else if (serverCon.getConnectionState() instanceof BasicConnectionState) {
			log.debug("Storing Received Connection");
			// NOTE: POTENTIAL FOR DEVIATIONS FROM GHS HERE
			// Store the result, but wait until another fragment forms
			serverCon.setReceivedConnect();
		} else {
			log.debug("Merge with nodes " + serverConI.getIdentifier().getUUID1() + " and "
					+ serverConI.getIdentifier().getUUID2());
			sendInitiate(serverCon, this.level + 1, serverConI.getIdentifier(), new Find());
		}

	}

	/**
	 * Sends an Initiate Message to the specified Server Connection
	 * 
	 * @param serverCon The Connection to send it to
	 * @param level     The Level to specify in the Initiate Request
	 * @param fragId    The Fragment Identifier
	 * @param state
	 */
	private void sendInitiate(Connection serverCon, int level, Identifier fragId, NodeState state) {
		JSONObject firstLevel = new JSONObject();
		JSONObject secondLevel = new JSONObject();

		secondLevel.put(Strings.UUID1, fragId.getUUID1().toString());
		secondLevel.put(Strings.UUID2, fragId.getUUID2().toString());
		secondLevel.put(Strings.NODE_STATE, state.toString());

		firstLevel.put(Strings.INITIATE, secondLevel);
		serverCon.writeMsg(firstLevel.toJSONString());
	}

	/**
	 * Process the receipt of an Initiate Message
	 * 
	 * @param level
	 * @param newLevel
	 * @param nodeState
	 * @param serverCon
	 */
	private void receiveInitiate(int level, Identifier fragmentID, NodeState nodeState, Connection serverCon) {
		// TODO process the receipt of a initiate message
		this.level = level;
		fragmentIdentifier = fragmentID;
		state = nodeState;
		inBranch = serverCon;
		bestEdge = null;
		bestWeight = -1;
		// Go through all the server connections,
		for (Connection con : serverConnections) {
			if (serverCon.equals(con)) {
				break;
			}
			if (serverCon.getConnectionState() instanceof BranchConnectionState) {
				sendInitiate(con, level, fragmentID, nodeState);
				if (nodeState instanceof Find) {
					foundCount++;
				}
			}
		}
		if (nodeState instanceof Find) {
			test();
		}
	}

	/** Execute the test procedure */
	private void test() {

	}

	/**
	 * Respond to the receipt of a test message
	 * 
	 * @param level
	 * @param nodeState
	 * @param serverCon
	 */
	private void respondTest(int level, NodeState nodeState, Connection serverCon) {
		// TODO process the receipt of a test message
	}

	/**
	 * Respond to the receipt of an accept message
	 * 
	 * @param serverCon
	 */
	private void respondAccept(Connection serverCon) {
		// TODO process the receipt of an accept message
	}

	/**
	 * Respond to the receipt of an Reject message
	 * 
	 * @param serverCon
	 */
	private void respondReject(Connection serverCon) {
		// TODO process the receipt of a rejection message
	}

	/**
	 * Respond to the receipt of an Report message
	 * 
	 * @param level
	 * @param serverCon
	 */
	private void respondReport(int level, Connection serverCon) {
		// TODO process the receipt of a rejection message
	}

	/**
	 * Respond to a change core message
	 * 
	 * @param serverCon
	 */
	private void respondChangeCore(Connection serverCon) {
		// TODO process the receipt of a Change Core Message
	}

	/**
	 * Converts the String contained into a JSONObject
	 */
	private JSONObject convertStringToJSON(String msg) throws ParseException {
		JSONParser jparse = new JSONParser();
		JSONObject json = (JSONObject) jparse.parse(msg);
		return json;
	}

	/**
	 * Used to get agreement on the lag value of the connection. with the other
	 * node. This message specifies the lag parameter of the current connection.
	 * 
	 * @param con
	 */
	private void sendLagAgreement(Connection con) {
		JSONObject jobj = new JSONObject();
		jobj.put(Strings.LAG_NEGOTIATE, Settings.LAG);
		con.writeMsg(jobj.toJSONString());
	}

	/**
	 * Sends a message containing the UUID of the node
	 * 
	 * @param con
	 */
	private void sendUUID(Connection con) {
		JSONObject jobj = new JSONObject();
		jobj.put(Strings.UUID, uuid.toString());
		con.writeMsg(jobj.toJSONString());
	}

	/**
	 * Used to get agreement on the lag value of the connection. with the other
	 * node. This message specifies the lag parameter of the current connection.
	 * 
	 * @param con The connection to set the lag on
	 */
	private void receiveLagAgreement(Connection con, JSONObject jobj) {
		int receivedLag = (int) (long) jobj.get(Strings.LAG_NEGOTIATE);
		if (receivedLag > Settings.LAG) {
			// The other node has a higher lag value. Therefore set the lag of our channel
			// the received value
			((ServerConnectionInformation) con.getConnectionInformation()).setLag(receivedLag);
		} else {
			// Increment Lag to avoid edges with the same values
			Settings.setLag(Settings.LAG++);
			((ServerConnectionInformation) con.getConnectionInformation()).setLag(Settings.getLag());
		}
		System.out.println("Connection lag " + ((ServerConnectionInformation) con.getConnectionInformation()).getLag());
	}

	/**
	 * Sets the connection type, depending on the connection type JSON received by
	 * the server
	 */
	private synchronized void setConnectionType(Connection con, JSONObject jobj) {
		String typeStr = jobj.get(Strings.CONNECTION_TYPE).toString();
		if (typeStr.equals(Strings.SERVER)) {
			// Create a new Server Connection
			connections.remove(con);
			con.setConnectionInformation(new ServerConnectionInformation());
			serverConnections.add(con);

			System.out.println("New Server Connection");

			// Perform Agreement on Lag for the connection
			// TODO Refactor these out.
			sendLagAgreement(con);
			sendUUID(con);
			System.out.println("Processed Server");
		}
		if (typeStr.equals(Strings.CLIENT)) {
			clientConenctions.add(con);
			connections.remove(con);
			System.out.println("New Client Connection");
		}
	}

	/**
	 * Processes any "message" JSON objects, by forwarding them to all the chat
	 * clients and all the servers on the network. Any simulated lag is also added
	 * when forwarding the message to other servers on the network.
	 * 
	 * @param con
	 * @param msgJSON
	 */
	private void processMessage(Connection con, JSONObject msgJSON) {
		// We have received a new message from a client.
		// Forward it to all the other connections on the server
		for (Connection cli : clientConenctions) {
			cli.writeMsg(msgJSON.toJSONString());
		}

		// Forward it to the other servers, after waiting our simulated
		// lag
		for (Connection server : serverConnections) {
			if (!(server.equals(con))) {
				LaggedMessage lagMsg = new LaggedMessage(msgJSON,
						((ServerConnectionInformation) server.getConnectionInformation()).getLag(), server);
				Thread msgThread = new Thread(lagMsg);
				msgThread.start();
			}
		}
	}

	/**
	 * Processes a message containing the UUID
	 * 
	 * @param con
	 * @param msgJson
	 */
	public void processUUIDmsg(Connection con, JSONObject msgJson) {
		UUID otherNodeUUID = UUID.fromString((String) msgJson.get(Strings.UUID));
		((ServerConnectionInformation) con.getConnectionInformation()).setIdentifier(uuid, otherNodeUUID);
	}

	/*
	 * Processing incoming messages from the connection. Return true if the
	 * connection should close.
	 */
	public synchronized boolean process(Connection con, String msg) {
		System.out.println("Message " + msg);
		try {
			JSONObject msgJSON = convertStringToJSON(msg);
			if (msgJSON.containsKey(Strings.CONNECTION_TYPE)) {
				// Set the connection type
				setConnectionType(con, msgJSON);
			}
			if (msgJSON.containsKey(Strings.MESSAGE)) {
				// A chat message from the client, process it
				processMessage(con, msgJSON);
			}
			if (msgJSON.containsKey(Strings.LAG_NEGOTIATE)) {
				// We have received the lag from the other node, so we perform negotiation
				if (serverConnections.contains(con)) {
					receiveLagAgreement(con, msgJSON);

					// Wakeup after receiving information on the lag.
					wakeup();
				}
			}
			if (msgJSON.containsKey(Strings.UUID)) {
				if (serverConnections.contains(con)) {
					processUUIDmsg(con, msgJSON);
				}
			}
			if (msgJSON.containsKey(Strings.CONNECT)) {
				System.out.println("RECEIVED CONNECT");
				if (serverConnections.contains(con)) {
					log.debug("SERVER CON");
					receiveConnect(con, msgJSON);
				}
			}
		} catch (ParseException e) {
			// We have an invalid message
			System.out.println("INVALID MESSAGE : CANNOT CONVERT TO JSON" + msg);
			e.printStackTrace();
		}
		return false;
	}

	/*
	 * The connection has been closed by the other party.
	 */
	public synchronized void connectionClosed(Connection con) {
		if (!term)
			connections.remove(con);
	}

	/*
	 * A new incoming connection has been established, and a reference is returned
	 * to it
	 */
	public synchronized Connection incomingConnection(Socket s) throws IOException {
		log.debug("incomming connection: " + Settings.socketAddress(s));
		Connection c = new Connection(s);
		connections.add(c);
		return c;
	}

	@Override
	public void run() {
		log.info("using activity interval of " + Settings.getActivityInterval() + " milliseconds");
		while (!term) {
			// do something with 5 second intervals in between
			try {
				Thread.sleep(Settings.getActivityInterval());
			} catch (InterruptedException e) {
				log.info("received an interrupt, system is shutting down");
				break;
			}
			if (!term) {
				term = doActivity();
			}

		}
		log.info("closing " + connections.size() + " connections");
		// clean up
		for (Connection connection : connections) {
			connection.closeCon();
		}
		listener.setTerm(true);
	}

	public boolean doActivity() {
		return false;
	}

	public final void setTerm(boolean t) {
		term = t;
	}

	public final ArrayList<Connection> getConnections() {
		return connections;
	}
}
