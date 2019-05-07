package activitystreamer.server;

import org.json.simple.JSONObject;

/**
 * A Class to represent a passed Message. It contains the JSON object itself,
 * alongside a refrence to the connection which has sent the message.
 * 
 * @author Patrick
 *
 */
public class Message {

	private Connection connection;
	private JSONObject jobj;

	public Message(Connection con, JSONObject msg) {
		connection = con;
		jobj = msg;
	}

	public Connection getConnection() {
		return connection;
	}

	public JSONObject getMessage() {
		return jobj;
	}
}
