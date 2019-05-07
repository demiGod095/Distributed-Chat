package activitystreamer.server;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import activitystreamer.util.Settings;

public class Control extends Thread {
	private static final Logger log = LogManager.getLogger();
	private static ArrayList<Connection> connections;

	private BlockingQueue<Message> queue;

	private static boolean term = false;
	private static Listener listener;

	protected static Control control = null;

	public static Control initInstance(BlockingQueue<Message> queue) {
		control = new Control(queue);
		return control;
	}

	public static Control getInstance() throws Exception {
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

	public void initiateConnection() {
		// make a connection to another server if remote hostname is supplied
		if (Settings.getRemoteHostname() != null) {
			try {
				Connection outgoing = outgoingConnection(
						new Socket(Settings.getRemoteHostname(), Settings.getRemotePort()));
				connections.add(outgoing);
			} catch (IOException e) {
				log.error("failed to make connection to " + Settings.getRemoteHostname() + ":"
						+ Settings.getRemotePort() + " :" + e);
				System.exit(-1);
			}
		}
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
		Connection c = new Connection(s);
		JSONObject jobj = new JSONObject();
		// jobj.put(Settings.CONNECTION_TYPE, Settings.SERVER);
		c.writeMsg(jobj.toJSONString());
		connections.add(c);
		return c;
	}

	/**
	 * Converts the String contained into a JSONObject
	 */
	private JSONObject convertStringToJSON(String msg) throws ParseException {
		JSONParser jparse = new JSONParser();
		JSONObject json = (JSONObject) jparse.parse(msg);
		return json;
	}

	/** Parses the String message into JSON, and then places it on the queue.
	 * @param con
	 * @param msg
	 * @return
	 */
	public synchronized boolean process(Connection con, String msg) {
		System.out.println("Message " + msg);
		JSONObject jobj;
		try {
			jobj = convertStringToJSON(msg);
			Message message = new Message(con, jobj);
			queue.add(message);
		} catch (ParseException e) {
			log.error("Invalid JSON");
			e.printStackTrace();
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
