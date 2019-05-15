package activitystreamer.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import activitystreamer.Server;
import activitystreamer.util.Strings;

/**
 * Consumes the messages from the queue, and then processes them. The node
 * either uses the messages to form a MST network between connected servers, or
 * processes chat messages by forwarding them onto the clients and the server
 * edges which are a part of the MST.
 * 
 */
public class Node implements Runnable {
	private static final Logger log = LogManager.getLogger();

	/**
	 * Maps between a Connection Thread and a class representing the Edges of the
	 * network.
	 */
	private HashMap<Connection, ServerEdge> connectionEdgeMapper = new HashMap<Connection, ServerEdge>();
	private ArrayList<Connection> clients = new ArrayList<Connection>();
	private BlockingQueue<Message> queue;

	/** Variables used in the GHS algorithm */
	private NodeState nodeState;
	private int level = 0;
	private int findCount = 0;
	private Connection inBranch = null;
	private Connection bestConnection = null;
	private Connection testConnection = null;
	private Double bestWeight = Double.POSITIVE_INFINITY;
	private boolean completed = false;

	private Identifier fragmentIdentifier = new Identifier(Server.SERVER_UUID, Server.SERVER_UUID);

	public Node(BlockingQueue<Message> queue) {
		this.queue = queue;
		nodeState = NodeState.Sleeping;
	}

	/** Performs the wakeup routine, as per the GHS algorithm. */
	public void wakeup() {
		log.debug("Wakeup Executed");
		level = 0;
		findCount = 0;
		this.nodeState = NodeState.Found;
		if (connectionEdgeMapper.size() == 0) {
			log.error("No outgoing edges");
			return;
		}

		// Find the connection with the lowest weight
		Connection lowest = null;
		try {
			for (Connection con : connectionEdgeMapper.keySet()) {
				if (lowest == null) {
					lowest = con;
					continue;
				}
				ServerEdge se = connectionEdgeMapper.get(con);
				if (se.compareTo(connectionEdgeMapper.get(lowest)) < 0) {
					lowest = con;
				}
			}
			if (lowest != null) {
				connectionEdgeMapper.get(lowest).setEdgeState(EdgeState.Branch);
				sendConnect(lowest);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Processes a normal chat "message" JSON object. This JSON object is sent by
	 * the client, and contains the chat message, along with the users username.
	 * This is to be forwarded onto other servers in the MST, and sent to every
	 * client on the server.
	 * 
	 * @param msg
	 */
	private void receiveMessage(Message msg) {
		Connection con = msg.getConnection();
		for (Connection c : clients) {
			c.writeMsg(msg.getMessage().toJSONString());
		}
		for (Connection c : connectionEdgeMapper.keySet()) {
			if (c.equals(con)) {
				continue;
			}
			if (connectionEdgeMapper.get(c).getEdgeState() == EdgeState.Branch) {
				c.writeMsg(msg.getMessage().toJSONString());
			}
		}
	}

	/** Processes the receipt of an acknowledgement to the handshake */
	private void processAckHandshake(Message message) {
		Connection con = message.getConnection();
		JSONObject jobj = message.getMessage();
		JSONObject level2 = (JSONObject) jobj.get(Strings.HANDSHAKE_ACK);
		UUID otherUUID = UUID.fromString((String) level2.get(Strings.UUID));
		ServerEdge se = new ServerEdge(Server.SERVER_UUID, otherUUID, con.getLag());
		connectionEdgeMapper.put(con, se);
	}

	/** Responds to a connect message, as per the GHS algortihm */
	private void respondToConnect(Message message) {
		JSONObject msgJSON = message.getMessage();
		Connection con = message.getConnection();
		ServerEdge se = connectionEdgeMapper.get(con);

		int levelReceived = (int) (long) msgJSON.get(Strings.CONNECT);
		if (nodeState == NodeState.Sleeping) {
			wakeup();
		}

		if (levelReceived < level) {
			// The node sending the connecting message is absorbed.
			// As the level we received from it, is less than the level of this node.

			Identifier edgeIdentifier = se.getIdentifier();
			log.debug("Absorbing Nodes " + edgeIdentifier.getUUID1() + " and " + edgeIdentifier.getUUID2());
			se.setEdgeState(EdgeState.Branch);
			sendInitiate(con, this.level, this.fragmentIdentifier, this.nodeState);
			if (this.nodeState == NodeState.Find) {
				findCount++;
			}
			connectionEdgeMapper.put(con, se);
			return;
		} else if (se.getEdgeState() == EdgeState.Basic) {
			// Place the message at the back of the queue, as we have not identified that it
			// is our lowest outgoing edge. We wait until we merge or absorb another node,
			// then come back to absorb or merge with this node later.
			log.debug("Putting Connection request at back of message queue");
			try {
				queue.put(message);
			} catch (InterruptedException e) {
				log.error("Could not place message on queue");
				e.printStackTrace();
			}
		} else {
			// A GHS Merge
			log.debug("Merge with nodes " + se.getIdentifier().getUUID1() + " and " + se.getIdentifier().getUUID2());
			sendInitiate(con, this.level + 1, se.getIdentifier(), NodeState.Find);
		}

	}

	/**
	 * Performs the JSON processing, necessary for calling the initiate function,
	 * then calls it, so the receipt of a Initiate message can be processed.
	 */
	private void receiveInitiate(Message message) {
		JSONObject jobj = message.getMessage();
		JSONObject secondLvl = (JSONObject) jobj.get(Strings.INITIATE);
		log.debug(message.getMessage().toString());

		UUID uuid1 = (UUID) UUID.fromString((String) secondLvl.get(Strings.UUID1));
		UUID uuid2 = (UUID) UUID.fromString((String) secondLvl.get(Strings.UUID2));
		Identifier fid = new Identifier(uuid1, uuid2);

		String ndeStateStr = (String) secondLvl.get(Strings.NODE_STATE);
		NodeState nodeState = NodeState.valueOf(ndeStateStr);
		int level = (int) (long) secondLvl.get(Strings.LEVEL);
		initiate(level, fid, nodeState, message.getConnection());
	}

	/**
	 * Process the receipt of an Initiate Message, as per the GHS algorithm. This
	 * message indicates that the node which this node has sent a connect message
	 * to, wants it to absorb or merge with it.
	 * 
	 * @param level              The level to merge / absorb at
	 * @param fragmentIdentifier The identifier of the fragment being used.
	 * @param nodeState          State at which the node is in.
	 * @param serverCon          The Connection which this message was received
	 *                           from.
	 */
	private void initiate(int level, Identifier fragmentID, NodeState nodeState, Connection serverCon) {
		// TODO process the receipt of a initiate message
		this.level = level;
		fragmentIdentifier = fragmentID;
		this.nodeState = nodeState;
		inBranch = serverCon;
		bestConnection = null;
		bestWeight = Double.POSITIVE_INFINITY;

		// Go through all the server connections, sending a initiate message to all the
		// nodes already on the tree, to inform them of the new fragment

		for (Connection con : connectionEdgeMapper.keySet()) {
			if (serverCon.equals(con)) {
				continue;
			}
			ServerEdge se = connectionEdgeMapper.get(con);

			if (se.getEdgeState() == EdgeState.Branch) {
				sendInitiate(con, level, fragmentID, nodeState);
				if (nodeState == NodeState.Find) {
					findCount++;
				}
			}
		}
		if (nodeState == NodeState.Find) {
			test();
		}
	}

	/**
	 * Execute the test procedure, as per the GHS algorithm. This tests the edge
	 * with the least weight, that we have not decided is in the tree yet.
	 */
	private void test() {
		// Look for edges with the basic state
		Connection min = null;
		for (Connection con : connectionEdgeMapper.keySet()) {
			ServerEdge se = connectionEdgeMapper.get(con);
			if (se.getEdgeState() == EdgeState.Basic) {
				if (min == null) {
					min = con;
				} else if (se.compareTo(connectionEdgeMapper.get(min)) < 0) {
					min = con;
				}
			}
		}
		if (min == null) {
			testConnection = null;
			report();
		} else {
			testConnection = min;
			sendTest(min, this.level, this.fragmentIdentifier);
		}
	}

	/**
	 * Receives a test message, as per the GHS algorithm. This checks to see if the
	 * node is in the same or different fragment, and then responds with an accept
	 * or reject message, depending on if they are in a different fragment or not.
	 * 
	 * @param msg
	 */
	private void receiveTest(Message msg) {
		JSONObject jobj = msg.getMessage();
		JSONObject info = (JSONObject) jobj.get(Strings.TEST);
		UUID uuid1 = UUID.fromString((String) info.get(Strings.UUID1));
		UUID uuid2 = UUID.fromString((String) info.get(Strings.UUID2));
		int testLevel = (int) (long) info.get(Strings.LEVEL);
		Identifier fid = new Identifier(uuid1, uuid2);

		ServerEdge se = connectionEdgeMapper.get(msg.getConnection());
		if (nodeState == NodeState.Sleeping) {
			log.debug("Waking Up");
			wakeup();
		}
		if (testLevel > this.level) {
			queue.add(msg);
			return;
		} else if (!(fid.equals(fragmentIdentifier))) {
			log.debug("Sending Accept between the connection " + se.getIdentifier().getUUID1()
					+ se.getIdentifier().getUUID2());
			sendAccept(msg.getConnection());
		} else {
			if (se.getEdgeState() == EdgeState.Basic) {
				se.setEdgeState(EdgeState.Reject);
			}
			if (testConnection == null) {
				// Handle Case where the connection being tested is set to null.
				// A deviation from the Algorithm, needed to implement it in java.
				log.debug("Sending Reject");
				sendReject(msg.getConnection());
			} else if ((!testConnection.equals(msg.getConnection()))) {
				log.debug("Sending Reject");
				sendReject(msg.getConnection());
			} else {
				test();
			}
		}

	}

	/**
	 * Handles the receipt of an Accept Message. A message indicating that the
	 * branch should be in the MST.
	 * 
	 * @param message
	 */
	private void receiveAccept(Message message) {
		log.debug("Receive Accept");
		Connection con = message.getConnection();
		testConnection = null;
		if (connectionEdgeMapper.get(con).getWeight() < bestWeight) {
			bestConnection = con;
			bestWeight = (double) (connectionEdgeMapper.get(con).getWeight());
		}
		report();
	}

	/**
	 * Processes the receipt of a receipt message, as per the GHS algorithm
	 * 
	 * @param message
	 */
	private void receiveReject(Message message) {
		log.debug("Receive Reject");
		ServerEdge se = connectionEdgeMapper.get(message.getConnection());
		if (se.getEdgeState() == EdgeState.Basic) {
			se.setEdgeState(EdgeState.Reject);
		}
		test();
	}
	
	/**
	 * Process the receipt of a report message, as per the GHS algorithm. This makes
	 * the node aware of what connection should be the next one to include in the
	 * MST.
	 */
	private void receiveReport(Message message) {
		JSONObject jobj = message.getMessage();
		double weight;

		if (jobj.get(Strings.REPORT) == null) {
			weight = Double.POSITIVE_INFINITY;
		} else {
			weight = (double) jobj.get(Strings.REPORT);
		}
		Connection con = message.getConnection();

		if (!(con.equals(inBranch))) {
			findCount--;
			if (weight < bestWeight) {
				bestWeight = (double) weight;
				bestConnection = con;
			}
			report();
		} else if (this.nodeState == NodeState.Find) {
			queue.add(message);
		} else if (weight > bestWeight) {
			changeCore();
		} else if (weight == bestWeight && bestWeight == Double.POSITIVE_INFINITY) {
			log.debug("Complete");
			completed = true;
		}
	}

	/**
	 * The report function, as per the GHS algorithm, reporting on the results of
	 * searching through every possible server connection in the node.
	 */
	private void report() {
		if (findCount == 0 && testConnection == null) {
			log.debug("Reporting");
			nodeState = NodeState.Found;
			sendReport(inBranch, bestWeight);
		}
	}

	/**
	 * Performs the Change core procedure, as per the GHS algorithm
	 * 
	 */
	private void changeCore() {
		if (connectionEdgeMapper.get(bestConnection).getEdgeState() == EdgeState.Branch) {
			log.debug("Change Core: Sending Change core to best connection");
			sendChangeCore(bestConnection);
		} else {
			log.debug("Sending Connect to best edge, as a part of the Core Change");
			sendConnect(bestConnection);
			connectionEdgeMapper.get(bestConnection).setEdgeState(EdgeState.Branch);
		}
	}

	/**
	 * Performs the Change Core Procedure, on the receipt of a change core message,
	 * as per GHS
	 */
	private void receiveChangeCore() {
		changeCore();
	}
	
	/** Sends a formatted Change Core JSON message */
	private void sendChangeCore(Connection con) {
		JSONObject jobj = new JSONObject();
		jobj.put(Strings.CHANGE_CORE, "");
		con.writeMsg(jobj.toJSONString());
	}

	/** Sends a report message */
	private void sendReport(Connection con, Double bestWeight2) {
		log.debug("Sending Report");
		JSONObject jobj = new JSONObject();
		jobj.put(Strings.REPORT, bestWeight2);
		con.writeMsg(jobj.toJSONString());
	}

	/** Sends a test message */
	private void sendTest(Connection connection, int level, Identifier fragmentIdentifier) {
		JSONObject firstLevel = new JSONObject();
		JSONObject secondLevel = new JSONObject();

		secondLevel.put(Strings.UUID1, fragmentIdentifier.getUUID1().toString());
		secondLevel.put(Strings.UUID2, fragmentIdentifier.getUUID2().toString());
		secondLevel.put(Strings.LEVEL, level);

		firstLevel.put(Strings.TEST, secondLevel);
		connection.writeMsg(firstLevel.toJSONString());
	}

	/** Sends an accept message */
	private void sendAccept(Connection con) {
		JSONObject jobj = new JSONObject();
		jobj.put(Strings.ACCEPT, Server.SERVER_UUID.toString());
		con.writeMsg(jobj.toJSONString());
	}

	/** Sends an reject message */
	private void sendReject(Connection con) {
		JSONObject jobj = new JSONObject();
		jobj.put(Strings.REJECT, Server.SERVER_UUID.toString());
		con.writeMsg(jobj.toJSONString());
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
		secondLevel.put(Strings.LEVEL, level);
		firstLevel.put(Strings.INITIATE, secondLevel);
		serverCon.writeMsg(firstLevel.toJSONString());
	}

	/**
	 * Send a Connect Message.
	 * 
	 * @param serverCon the connection to which to send the connect message to
	 * 
	 */
	private void sendConnect(Connection serverCon) {
		log.debug("SendingConnect");
		JSONObject jobj = new JSONObject();
		jobj.put(Strings.CONNECT, level);
		serverCon.writeMsg(jobj.toJSONString());
	}

	/**
	 * Sends an acknowledgement to a server handshake, sent by a remote process
	 * making a connection to this server.
	 */
	public void sendAckHandshake(Connection con, int lag) {
		JSONObject jobj = new JSONObject();
		JSONObject lvl2 = new JSONObject();
		lvl2.put(Strings.LAG, lag);
		lvl2.put(Strings.UUID, Server.SERVER_UUID.toString());
		jobj.put(Strings.HANDSHAKE_ACK, lvl2);
		con.writeMsg(jobj.toString());
	}

	/**
	 * Processes the message retrieved from the queue, and decides on which function
	 * is to be called from the JSON object.
	 */
	private void processMessage(Message message) {
		JSONObject jobj = message.getMessage();
		if (jobj.containsKey(Strings.SERVER_HANDSHAKE)) {
			processServerHandshake(message.getConnection(), jobj);
		} else if (jobj.containsKey(Strings.CONNECTION_TYPE)) {
			processClientConnection(message.getConnection(), jobj);
		} else if (jobj.containsKey(Strings.WAKE_UP)) {
			if (nodeState == NodeState.Sleeping) {
				log.debug("Node State" + nodeState);
				log.debug("Wakeing Up, due to Client Request");
				wakeup();
			}
		} else if (jobj.containsKey(Strings.CONNECT)) {
			respondToConnect(message);
		} else if (jobj.containsKey(Strings.INITIATE)) {
			receiveInitiate(message);
		} else if (jobj.containsKey(Strings.TEST)) {
			receiveTest(message);
		} else if (jobj.containsKey(Strings.ACCEPT)) {
			receiveAccept(message);
		} else if (jobj.containsKey(Strings.REJECT)) {
			receiveReject(message);
		} else if (jobj.containsKey(Strings.REPORT)) {
			receiveReport(message);
		} else if (jobj.containsKey(Strings.CHANGE_CORE)) {
			receiveChangeCore();
		} else if (jobj.containsKey(Strings.MESSAGE)) {
			receiveMessage(message);
		} else if (jobj.containsKey(Strings.HANDSHAKE_ACK)) {
			processAckHandshake(message);
		}
	}

	/**
	 * Processes a server handshake message. This gets the UUID of the server making
	 * the new connection, and adds the connection to the list of server
	 * connections. This function also then makes an acknowledgement to this
	 * message.
	 * 
	 * @param connection
	 * @param jobj
	 */
	private void processServerHandshake(Connection connection, JSONObject jobj) {
		JSONObject level2 = (JSONObject) jobj.get(Strings.SERVER_HANDSHAKE);
		int lag = (int) (long) level2.get(Strings.LAG);
		connection.setLag(lag);
		UUID objUUID = UUID.fromString((String) level2.get(Strings.UUID));
		ServerEdge se = new ServerEdge(Server.SERVER_UUID, objUUID, lag);
		connectionEdgeMapper.put(connection, se);
		sendAckHandshake(connection, lag);
		log.debug("Added new edge with lag " + connection.getLag());
	}

	/**
	 * If a client connects, it sends a message specifying that it is a client. We
	 * keep track of this, so we know what connections are made by clients.
	 * 
	 * @param connection
	 * @param jobj
	 */
	private void processClientConnection(Connection connection, JSONObject jobj) {
		if (jobj.get(Strings.CONNECTION_TYPE).equals(Strings.CLIENT)) {
			log.debug("Adding New Client\n");
			clients.add(connection);
		}
	}

	@Override
	public void run() {
		log.debug("Node Started");
		try {
			while (true) {
				// Take the Message from the queue
				Message msg = queue.take();
				processMessage(msg);
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
