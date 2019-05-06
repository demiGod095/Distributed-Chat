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
	private static ArrayList<Connection> connections;

	private int level;
	private NodeState state = null;
	private int foundCount = 0;

	private ArrayList<ClientConnection> clientConnections = new ArrayList<ClientConnection>();
	private ArrayList<ServerConnection> serverConnections = new ArrayList<ServerConnection>();

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
		// initialize the connections array
		connections = new ArrayList<Connection>();
		uuid = UUID.randomUUID();
		fragmentIdentifier = new Identifier(uuid, uuid);

		// start a listener
		try {
			listener = new Listener();
			initiateConnection();
		} catch (IOException e1) {
			log.fatal("failed to startup a listening thread: " + e1);
			System.exit(-1);
		}
	}

	public void initiateConnection() {
		// make a connection to another server if remote hostname is supplied
		if (Settings.getRemoteHostname() != null) {
			try {
				Connection outgoing = outgoingConnection(
						new Socket(Settings.getRemoteHostname(), Settings.getRemotePort()));
				if (outgoing instanceof ServerConnection) {
					sendLagAgreement((ServerConnection) outgoing);
					sendUUID((ServerConnection) outgoing);
				}
			} catch (IOException e) {
				log.error("failed to make connection to " + Settings.getRemoteHostname() + ":"
						+ Settings.getRemotePort() + " :" + e);
				System.exit(-1);
			}
		}
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
		ServerConnection current = serverConnections.get(0);
		for (ServerConnection serverCon : serverConnections) {
			if (serverCon.getLag() < current.getLag()) {
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
	private void sendConnect(ServerConnection serverCon) {
		JSONObject jobj = new JSONObject();
		jobj.put(Strings.CONNECT, level);
		serverCon.writeMsg(jobj.toJSONString());
	}

	private void receiveConnect(ServerConnection serverCon, JSONObject msgJSON) {
		System.out.println("STATE " + state);
		int level = (int) (long) msgJSON.get(Strings.CONNECT);
		// TODO process the receipt of a connect message
		if (state == null) {
			wakeup();
		}
		if (level < this.level) {
			log.debug("Merge with nodes " + serverCon.getIdentifier().getUUID1() + " and " + serverCon.getIdentifier().getUUID2());
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
			sendInitiate(serverCon, this.level + 1, serverCon.getIdentifier(), new Find());
		}

	}

	private void sendInitiate(ServerConnection serverCon, int level, Identifier fragId, NodeState state) {
		JSONObject firstLevel = new JSONObject();
		JSONObject secondLevel = new JSONObject();

		secondLevel.put(Strings.UUID1, fragId.getUUID1().toString());
		secondLevel.put(Strings.UUID2, fragId.getUUID2().toString());
		secondLevel.put(Strings.NODE_STATE, state.toString());

		firstLevel.put(Strings.INITIATE, secondLevel);
		serverCon.writeMsg(firstLevel.toJSONString());
	}

	private void receiveInitiate(int level, int newLevel, NodeState nodeState, ServerConnection serverCon) {
		// TODO process the receipt of a initiate message
	}

	private void respondTest(int level, NodeState nodeState, ServerConnection serverCon) {
		// TODO process the receipt of a test message
	}

	private void respondAccept(ServerConnection serverCon) {
		// TODO process the receipt of an accept message
	}

	private void respondReject(ServerConnection serverCon) {
		// TODO process the receipt of a rejection message
	}

	private void respondReport(int level, ServerConnection serverCon) {
		// TODO process the receipt of a rejection message
	}

	private void respondChangeCore(ServerConnection serverCon) {
		// TODO process the receipt of a Change Core Message
	}

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
	private void sendLagAgreement(ServerConnection con) {
		JSONObject jobj = new JSONObject();
		jobj.put(Strings.LAG_NEGOTIATE, Settings.LAG);
		con.writeMsg(jobj.toJSONString());
	}

	private void sendUUID(ServerConnection con) {
		JSONObject jobj = new JSONObject();
		jobj.put(Strings.UUID, uuid.toString());
		con.writeMsg(jobj.toJSONString());
	}

	/**
	 * Used to get agreement on the lag value of the connection. with the other
	 * node. This message specifies the lag parameter of the current connection.
	 * 
	 * @param con
	 */
	private void receiveLagAgreement(ServerConnection con, JSONObject jobj) {
		int receivedLag = (int) (long) jobj.get(Strings.LAG_NEGOTIATE);
		if (receivedLag > Settings.LAG) {
			// The other node has a higher lag value. Therefore set the lag of our channel
			// the received value
			con.setLag(receivedLag);
		} else {
			// Increment Lag to avoid edges with the same values
			Settings.setLag(Settings.LAG++);
			con.setLag(Settings.getLag());
		}
		System.out.println("Connection lag " + con.getLag());
	}

	/**
	 * Sets the connection type, depending on the connection type JSON received by
	 * the server
	 */
	private void setConnectionType(Connection con, JSONObject jobj) {
		String typeStr = jobj.get(Settings.CONNECTION_TYPE).toString();
		if (typeStr.equals(Settings.SERVER)) {
			// Create a new Server Connection
			ServerConnection serverCon;
			try {
				serverCon = new ServerConnection(con.getSocket());
				serverConnections.add(serverCon);
				connections.remove(con);
				System.out.println("New Server Connection");

				// Perform Agreement on Lag for the connection
				// TODO Refactor these out.
				sendLagAgreement(serverCon);
				sendUUID(serverCon);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if (typeStr.equals(Settings.CLIENT)) {
			try {
				ClientConnection cliCon = new ClientConnection(con.getSocket());
				clientConnections.add(cliCon);
				connections.remove(con);
				System.out.println("New Client Connection");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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
		for (ClientConnection cli : clientConnections) {
			cli.writeMsg(msgJSON.toJSONString());
		}

		// Forward it to the other servers, after waiting our simulated
		// lag
		for (ServerConnection server : serverConnections) {
			if (!(server.equals(con))) {
				LaggedMessage lagMsg = new LaggedMessage(msgJSON, server.getLag(), server);
				Thread msgThread = new Thread(lagMsg);
				msgThread.start();
			}
		}
	}

	public void processUUIDmsg(ServerConnection con, JSONObject msgJson) {
		UUID otherNodeUUID = UUID.fromString((String) msgJson.get(Strings.UUID));
		con.setIdentifier(uuid, otherNodeUUID);
	}

	/*
	 * Processing incoming messages from the connection. Return true if the
	 * connection should close.
	 */
	public synchronized boolean process(Connection con, String msg) {
		System.out.println("Message " + msg);
		try {
			JSONObject msgJSON = convertStringToJSON(msg);
			if (msgJSON.containsKey(Settings.CONNECTION_TYPE)) {
				// Set the connection type
				setConnectionType(con, msgJSON);
			}
			if (msgJSON.containsKey(Settings.MESSAGE)) {
				// A chat message from the client, process it
				processMessage(con, msgJSON);
			}
			if (msgJSON.containsKey(Strings.LAG_NEGOTIATE)) {
				// We have received the lag from the other node, so we perform negotiation
				if (con instanceof ServerConnection) {
					receiveLagAgreement((ServerConnection) con, msgJSON);

					// Wakeup after receiving information on the lag.
					wakeup();
				}
			}
			if (msgJSON.containsKey(Strings.UUID)) {
				if (con instanceof ServerConnection) {
					processUUIDmsg((ServerConnection) con, msgJSON);
				}
			}
			if (msgJSON.containsKey(Strings.CONNECT)) {
				if (con instanceof ServerConnection) {
					receiveConnect((ServerConnection) con, msgJSON);
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

	/*
	 * A new outgoing connection has been established, and a reference is returned
	 * to it
	 */
	public synchronized Connection outgoingConnection(Socket s) throws IOException {
		log.debug("outgoing connection: " + Settings.socketAddress(s));
		ServerConnection c = new ServerConnection(s);
		JSONObject jobj = new JSONObject();
		JSONObject level2 = new JSONObject();

		jobj.put(Settings.CONNECTION_TYPE, Settings.SERVER);
		c.writeMsg(jobj.toJSONString());
		System.out.println(jobj.toJSONString());
		serverConnections.add(c);
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
