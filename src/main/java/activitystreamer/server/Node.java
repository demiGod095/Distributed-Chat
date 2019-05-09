package activitystreamer.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;

import activitystreamer.Server;
import activitystreamer.util.Settings;
import activitystreamer.util.Strings;

/** Consumes the messages, and then processes them, to form a GHS network */
public class Node implements Runnable {
	private static final Logger log = LogManager.getLogger();

	/**
	 * Maps between a Connection and a class representing the Edges of the network.
	 */
	private HashMap<Connection, ServerEdge> connectionEdgeMapper = new HashMap<Connection, ServerEdge>();

	boolean once = false;
	// Mapping to Every Client
	private ArrayList<Connection> clients = new ArrayList<Connection>();

	private BlockingQueue<Message> queue;
	private NodeState nodeState;
	private int level = 0;
	private int findCount = 0;

	private boolean completed = false;

	private Connection inBranch = null;
	private Connection bestConnection = null;

	private Connection testConnection = null;

	private Double bestWeight = Double.POSITIVE_INFINITY;

	private Identifier fragmentIdentifier = new Identifier(Server.SERVER_UUID, Server.SERVER_UUID);

	public Node(BlockingQueue<Message> queue) {
		this.queue = queue;
		nodeState = NodeState.Sleeping;
	}

	/** Performs the wakeup routine */
	public void wakeup() {
		log.debug("Wakeup Executed");
		if (connectionEdgeMapper.size() == 0) {
			log.error("No outgoing edges");
			return;
		}
		level = 0;
		nodeState = NodeState.Found;

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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/** Respond to a connect message */
	private void respondToConnect(Message message) {
		JSONObject msgJSON = message.getMessage();
		Connection con = message.getConnection();
		ServerEdge se = connectionEdgeMapper.get(con);

		int levelReceived = (int) (long) msgJSON.get(Strings.CONNECT);
		if (nodeState == NodeState.Sleeping) {
			wakeup();
		}

		if (levelReceived < level) {
			Identifier edgeIdentifier = se.getIdentifier();
			log.debug("Absorbing Nodes " + edgeIdentifier.getUUID1() + " and " + edgeIdentifier.getUUID2());
			se.setEdgeState(EdgeState.Branch);
			sendInitiate(con, this.level, this.fragmentIdentifier, this.nodeState);
			if (nodeState == NodeState.Find) {
				findCount++;
			}
			connectionEdgeMapper.put(con, se);
			return;
		} else if (se.getEdgeState() == EdgeState.Basic) {
			log.debug("Putting Connection request at back of message queue");
			try {
				queue.put(message);
			} catch (InterruptedException e) {
				log.error("Could not place message on queue");
				e.printStackTrace();
			}
		} else {
			log.debug("Merge with nodes " + se.getIdentifier().getUUID1() + " and " + se.getIdentifier().getUUID2());
			sendInitiate(con, this.level + 1, se.getIdentifier(), NodeState.Find);
		}

	}

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
	 * Process the receipt of an Initiate Message
	 * 
	 * @param level
	 * @param newLevel
	 * @param nodeState
	 * @param serverCon
	 */
	private void initiate(int level, Identifier fragmentID, NodeState nodeState, Connection serverCon) {
		// TODO process the receipt of a initiate message
		this.level = level;
		fragmentIdentifier = fragmentID;
		this.nodeState = nodeState;
		inBranch = serverCon;

		bestConnection = null;
		bestWeight = Double.POSITIVE_INFINITY;
		// Go through all the server connections,

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

	/** Execute the test procedure */
	private void test() {
		// Look for edges with the basic state
		log.debug("TEST CALLED");
		Connection min = null;
		for (Connection con : connectionEdgeMapper.keySet()) {
			ServerEdge se = connectionEdgeMapper.get(con);
			if (se.getEdgeState() == EdgeState.Basic) {
				log.debug("Basic Connection Found");
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

	private void receiveTest(Message msg) {
		log.debug("Received Test Message " + msg.getMessage().toString());
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
			log.debug("Test Received. Putting back in queue");
			queue.add(msg);
			return;
		} else if (!(fid.equals(fragmentIdentifier))) {
			log.debug("Sending Accept");
			sendAccept(msg.getConnection());
		} else {
			if (se.getEdgeState() == EdgeState.Basic) {
				se.setEdgeState(EdgeState.Reject);
			}
			if (testConnection == null) {
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

	private void receiveAccept(Message message) {
		Connection con = message.getConnection();
		testConnection = null;
		if (connectionEdgeMapper.get(con).getLag() < bestWeight) {
			bestConnection = testConnection;
			bestWeight = (double) (connectionEdgeMapper.get(con).getLag());
		}
		report();
	}

	private void receiveReject(Message message) {
		ServerEdge se = connectionEdgeMapper.get(message.getConnection());
		if (se.getEdgeState() == EdgeState.Basic) {
			se.setEdgeState(EdgeState.Reject);
		}
		test();
	}

	private void sendAccept(Connection con) {
		JSONObject jobj = new JSONObject();
		jobj.put(Strings.ACCEPT, Server.SERVER_UUID.toString());
		con.writeMsg(jobj.toJSONString());
	}

	private void sendReject(Connection con) {
		JSONObject jobj = new JSONObject();
		jobj.put(Strings.REJECT, Server.SERVER_UUID.toString());
		con.writeMsg(jobj.toJSONString());
	}

	private void report() {
		log.debug("Find Count" + findCount);
		if (testConnection != null) {
			log.debug("TEST CONNECTION " + testConnection.getSocket().toString());
		}
		if (findCount == 0 && testConnection == null) {
			nodeState = NodeState.Found;
			sendReport(inBranch, bestWeight);
		}
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
		JSONObject jobj = new JSONObject();
		jobj.put(Strings.CONNECT, level);
		serverCon.writeMsg(jobj.toJSONString());
	}

	/** Process the message retrieved from the queue */
	private void processMessage(Message message) {
		log.debug("JSON STR " + message.getMessage().toJSONString());
		JSONObject jobj = message.getMessage();
		if (jobj.containsKey(Strings.SERVER_HANDSHAKE)) {
			processServerHandshake(message.getConnection(), jobj);
		} else if (jobj.containsKey(Strings.CONNECTION_TYPE)) {
			processClientConnection(message.getConnection(), jobj);
		} else if (jobj.containsKey(Strings.WAKE_UP)) {
			if (nodeState == NodeState.Sleeping) {
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
		}
	}

	private void processServerHandshake(Connection connection, JSONObject jobj) {
		JSONObject level2 = (JSONObject) jobj.get(Strings.SERVER_HANDSHAKE);
		int lag = (int) (long) level2.get(Strings.LAG);
		UUID objUUID = UUID.fromString((String) level2.get(Strings.UUID));

		if (lag < Settings.LAG) {
			lag = Settings.LAG;
		}
		ServerEdge se = new ServerEdge(Server.SERVER_UUID, objUUID, lag);
		connectionEdgeMapper.put(connection, se);

	}

	private void processClientConnection(Connection connection, JSONObject jobj) {
		if (jobj.get(Strings.CONNECTION_TYPE).equals(Strings.CLIENT)) {
			log.debug("Adding New Client\n");
			clients.add(connection);
		}
	}

	private void receiveReport(Message message) {
		JSONObject jobj = message.getMessage();
		log.debug("Report Received from " + message.getConnection().getSocket().toString());
		double weight;
		if (jobj.get(Strings.REPORT) == null) {
			weight = Double.POSITIVE_INFINITY;
		} else {
			weight = (double) (long) jobj.get(Strings.REPORT);
		}
		Connection con = message.getConnection();

		if (!(con.equals(inBranch))) {
			log.debug("Con not in In branch");
			log.debug("In Branch is " + inBranch.getSocket().toString());
			findCount--;
			log.debug("New Find Count " + findCount);
			if (weight < bestWeight) {
				bestWeight = (double) weight;
				bestConnection = con;
			}
			report();
		} else if (this.nodeState == NodeState.Find) {
			log.debug("Queueing Message");
			queue.add(message);
		} else if (weight > bestWeight) {
			changeCore();
		} else if (weight == bestWeight && bestWeight == Double.POSITIVE_INFINITY) {
			log.debug("Complete");
			completed = true;
		}
	}

	private void changeCore() {
		if (connectionEdgeMapper.get(bestConnection).getEdgeState() == EdgeState.Branch) {
			sendChangeCore(bestConnection);
		} else {
			sendConnect(bestConnection);
			connectionEdgeMapper.get(bestConnection).setEdgeState(EdgeState.Branch);
		}
	}

	private void sendChangeCore(Connection con) {
		JSONObject jobj = new JSONObject();
		jobj.put(Strings.CHANGE_CORE, "");
		con.writeMsg(jobj.toJSONString());
	}

	private void receiveChangeCore() {
		changeCore();
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
