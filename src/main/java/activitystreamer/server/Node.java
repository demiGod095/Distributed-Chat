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
	private static final int negInf = -1;
	/**
	 * Maps between a Connection and a class representing the Edges of the network.
	 */
	private HashMap<Connection, ServerEdge> connectionEdgeMapper = new HashMap<Connection, ServerEdge>();

	// Mapping to Every Client
	private ArrayList<Connection> clients = new ArrayList<Connection>();

	private BlockingQueue<Message> queue;
	private NodeState nodeState;
	private int level = 0;
	private int findCount = 0;

	private Connection inBranch = null;
	private Connection bestConnection = null;
	private Connection testConnection = null;
	private Integer bestWeight = negInf;

	private Identifier fragmentIdentifier = new Identifier(Server.SERVER_UUID, Server.SERVER_UUID);

	public Node(BlockingQueue<Message> queue) {
		this.queue = queue;
		nodeState = NodeState.Sleeping;
	}

	/** Performs the wakeup routine */
	public void wakeup() {
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
			connectionEdgeMapper.get(lowest).setEdgeState(EdgeState.Branch);
			sendConnect(lowest);
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
			log.debug("Absorbing Nodes " + se.getIdentifier().getUUID1() + " and " + se.getIdentifier().getUUID2());
			se.setEdgeState(EdgeState.Branch);
			sendInitiate(con, level, fragmentIdentifier, nodeState);
			if (nodeState == NodeState.Find) {
				findCount++;
			}
			return;
		} else if (se.getEdgeState() == EdgeState.Basic) {
			log.debug("Putting Connection request at backof message queue");
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
		bestWeight = negInf;
		// Go through all the server connections,

		for (Connection con : connectionEdgeMapper.keySet()) {
			if (serverCon.equals(con)) {
				break;
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

	private void receiveTest(Message msg) {
		JSONObject jobj = msg.getMessage();
		JSONObject info = (JSONObject) jobj.get(Strings.TEST);
		UUID uuid1 = UUID.fromString((String) jobj.get(Strings.UUID1));
		UUID uuid2 = UUID.fromString((String) jobj.get(Strings.UUID2));
		int testLevel = (int) (long) info.get(Strings.LEVEL);
		Identifier fid = new Identifier(uuid1, uuid2);
		
		ServerEdge se = connectionEdgeMapper.get(msg.getConnection());
		if (nodeState == NodeState.Sleeping) {
			wakeup();
		}
		if (testLevel > this.level) {
			queue.add(msg);
			return;
		} else if (!(fid.equals(fragmentIdentifier))) {
			sendAccept(msg.getConnection());
		} else {
			if (se.getEdgeState() == EdgeState.Basic) {
				se.setEdgeState(EdgeState.Reject);
			}
			if (!(testConnection.equals(msg.getConnection()))) {
				sendReject(msg.getConnection());
			} else {
				test();
			}

		}
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
		if (findCount == 0 && testConnection == null) {
			nodeState = NodeState.Found;
			sendReport(inBranch, bestWeight);
		}
	}

	/** Sends a report message */
	private void sendReport(Connection con, int bestWeight) {
		JSONObject jobj = new JSONObject();
		jobj.put(Strings.REPORT, bestWeight);
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
		}
		else if (jobj.containsKey(Strings.CONNECTION_TYPE)) {
			processClientConnection(message.getConnection(), jobj);
		}
		else if (jobj.containsKey(Strings.WAKE_UP)) {
			if (nodeState == NodeState.Sleeping) {
				log.debug("Wakeing Up, due to Client Request");
				wakeup();
			}
		}
		else if (jobj.containsKey(Strings.CONNECT)) {
			respondToConnect(message);
		}
		else if (jobj.containsKey(Strings.INITIATE)) {
			receiveInitiate(message);
		}
		else if (jobj.containsKey(Strings.TEST)) {
			receiveTest(message);
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
