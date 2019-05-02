package activitystreamer.server;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import activitystreamer.util.Settings;

public class Control extends Thread {
	private static final Logger log = LogManager.getLogger();
	private static ArrayList<Connection> connections;

	private ArrayList<ClientConnection> clientConnections = new ArrayList<ClientConnection>();
	private ArrayList<ServerConnection> serverConnections = new ArrayList<ServerConnection>();

	private static boolean term = false;
	private static Listener listener;

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
				outgoingConnection(new Socket(Settings.getRemoteHostname(), Settings.getRemotePort()));
			} catch (IOException e) {
				log.error("failed to make connection to " + Settings.getRemoteHostname() + ":"
						+ Settings.getRemotePort() + " :" + e);
				System.exit(-1);
			}
		}
	}

	private JSONObject convertStringToJSON(String msg) throws ParseException {
		JSONParser jparse = new JSONParser();
		JSONObject json = (JSONObject) jparse.parse(msg);
		return json;
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
		try {
			Thread.sleep(Settings.getLag());
			for (ServerConnection server : serverConnections) {
				if (!(server.equals(con))) {
					server.writeMsg(msgJSON.toJSONString());
				}
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
		jobj.put(Settings.CONNECTION_TYPE, Settings.SERVER);
		c.writeMsg(jobj.toJSONString());
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
				// log.debug("doing activity");
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
