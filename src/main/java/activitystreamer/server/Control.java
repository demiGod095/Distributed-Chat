package activitystreamer.server;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import activitystreamer.Server;
import activitystreamer.util.IpPortLagTriplet;
import activitystreamer.util.Settings;
import activitystreamer.util.Strings;

/**
 * The Controller for the server. This thread receives any new connections,
 * initiates any remote connections to be made at startup. It also places
 * messages in the Blocking Queue. The Node thread then takes the messages from
 * this FIFO queue, and processes the messages.
 * 
 * Uses elements of Arron Harwood's code from last year.
 * 
 * @author Patrick
 *
 */
public class Control extends Thread {
	private static final Logger log = LogManager.getLogger();
	private static ArrayList<Connection> connections;

	private BlockingQueue<Message> queue;

	private static boolean term = false;
	private static Listener listener;

	protected static Control control = null;

	/** Create the initial instance, with a defined Blocking Queue */
	public synchronized static Control initInstance(BlockingQueue<Message> queue) {
		log.debug("INITIAL CONTROL");
		control = new Control(queue);
		return control;
	}

	/**
	 * Get an instance of the Control Class. If an instance has not been
	 * Initialized, an exception is thrown.
	 */
	public synchronized static Control getInstance() throws Exception {
		if (control == null) {
			throw new Exception("Instance was forgotten to be initialised");
		}
		return control;
	}

	public Control(BlockingQueue<Message> queue) {
		this.queue = queue;
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

	/**
	 * If a remote Server arguement has been supplied, then we make a connection to
	 * every server specified in the remote server list.
	 */
	public void initiateConnection() {
		// make a connection to another server if remote hostname is supplied
		if (Settings.getIpPorts().size() != 0) {
			for (IpPortLagTriplet ipp : Settings.getIpPorts()) {
				try {
					log.debug("Connecting to " + ipp.getIpAddress().toString() + " Port " + ipp.getPort());
					Connection outgoing = outgoingConnection(new Socket(ipp.getIpAddress(), ipp.getPort()));
					outgoing.setLag(ipp.getLag());
					sendHandshake(outgoing, ipp.getLag(), Server.SERVER_UUID);
				} catch (IOException e) {
					log.error("Error connecting to remote servers");
					e.printStackTrace();
					System.exit(-1);
				} catch (Exception e) {
					log.error("Error connecting to remote servers");
					System.exit(-1);
				}
			}
		}
	}

	/**
	 * A new outgoing connection has been established, and a reference is returned
	 * to it
	 */
	public synchronized Connection outgoingConnection(Socket s) throws IOException {
		log.debug("outgoing connection: " + Settings.socketAddress(s));
		Connection c = new Connection(s);
		JSONObject jobj = new JSONObject();
		c.writeMsg(jobj.toJSONString());
		connections.add(c);
		return c;
	}

	/**
	 * The connection has been closed by the other party.
	 */
	public synchronized void connectionClosed(Connection con) {
		if (!term)
			connections.remove(con);
	}

	/**
	 * ( A new incoming connection has been established, and a reference is returned
	 * to it
	 */
	public synchronized Connection incomingConnection(Socket s) throws IOException {
		log.debug("incomming connection: " + Settings.socketAddress(s));
		Connection c = new Connection(s);
		connections.add(c);
		return c;
	}

	/**
	 * Send a message specifying a server handshake, to any connections made to
	 * other servers. This allows the other servers to learn that this connection,
	 * is a server connection. Not a client.
	 */
	public void sendHandshake(Connection connection, int lag, UUID uuid) {
		JSONObject level1 = new JSONObject();
		JSONObject level2 = new JSONObject();
		level2.put(Strings.UUID, uuid.toString());
		level2.put(Strings.LAG, lag);
		level1.put(Strings.SERVER_HANDSHAKE, level2);
		connection.writeMsg(level1.toJSONString());
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
	 * Parses the String message into JSON, and then places it on the queue.
	 * 
	 * @param con
	 * @param msg
	 * @return True - Connection should be closed. False - The connection should
	 *         remain open.
	 */
	public synchronized boolean process(Connection con, String msg) {
		JSONObject jobj;
		try {
			log.debug("Msg " + msg);
			jobj = convertStringToJSON(msg);
			Message message = new Message(con, jobj);
			queue.add(message);
		} catch (ParseException e) {
			log.error("Invalid JSON");
			e.printStackTrace();
			return true;
		}
		return false;
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
