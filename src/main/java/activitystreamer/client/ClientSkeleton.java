package activitystreamer.client;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import activitystreamer.util.Settings;
import activitystreamer.util.Strings;

import static java.lang.StrictMath.toIntExact;

public class ClientSkeleton extends Thread {
	public static String CONNECTION_TYPE = "connection_type";
	public static String CLIENT = "client";
	private static final Logger log = LogManager.getLogger();
	private static ClientSkeleton clientSolution;
	private OutputStream output;
	private BufferedReader input;
	private PrintWriter pwrite;

	private Boolean connected = false;

	private String messages;

	private TextFrame textFrame;

	private Socket clientSocket;

	public static ClientSkeleton getInstance() {
		if (clientSolution == null) {
			clientSolution = new ClientSkeleton();
		}
		return clientSolution;
	}

	public ClientSkeleton() {
		messages = "";
		textFrame = new TextFrame();
		start();
		// Initalise the socket
		try {
			clientSocket = new Socket(Settings.getRemoteHostname(), Settings.getRemotePort());
			output = clientSocket.getOutputStream();
			pwrite = new PrintWriter(output, true);
			input = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
			sendClientConnectionRequest();
			connected = true;
		} catch (ConnectException e) {
			log.error("Connection Refused");
			e.printStackTrace();
			System.exit(-1);
		} catch (UnknownHostException e) {
			log.error("Unknown Host Error " + Settings.getRemoteHostname());
			e.printStackTrace();
			System.exit(-1);
		} catch (IOException e) {
			log.error("IO Exception");
			e.printStackTrace();
			System.exit(-1);
		}
	}

	@SuppressWarnings("unchecked")
	public void sendJsonOnSocket(JSONObject activityObj) {
		pwrite.println(activityObj.toJSONString());
		pwrite.flush();
	}

	/** Sends information specifing the Connection type to the server */
	private void sendClientConnectionRequest() {
		JSONObject obj = new JSONObject();
		obj.put(Strings.CONNECTION_TYPE, CLIENT);
		sendJsonOnSocket(obj);
	}

	/**
	 * Updates the window with the incoming text, with the user name appended to the
	 * left hand side
	 */
	public boolean updateIncomingData() {
		if (!connected) {
			return false;
		}
		String receivedMessage;
		try {
			if ((receivedMessage = input.readLine()) != null) {
				JSONParser parse = new JSONParser();
				JSONObject obj;
				try {
					obj = (JSONObject) parse.parse(receivedMessage);
					if (obj.containsKey(Strings.MESSAGE)) {
						JSONObject jobj2 = (JSONObject) obj.get(Strings.MESSAGE);
						String message = (String) jobj2.get(Strings.MESSAGE);
						String username = (String) jobj2.get(Strings.USERNAME);
						messages = messages + username + ": " + message + "\n";
						textFrame.setOutputText(messages);
					}
				} catch (ParseException e) {
					e.printStackTrace();
					return true;
				}

			}
		} catch (IOException e) {
			e.printStackTrace();
			return true;
		}
		return false;
	}

	/**
	 * Sends a message, contained in the message string. This class also appends the
	 * username into the MESSAGE json object sent as well.
	 * 
	 * @param message
	 */
	public void sendMessage(String message) {
		message = message.trim().replaceAll("\r", "").replaceAll("\n", "").replaceAll("\t", "");
		JSONObject msg = new JSONObject();
		JSONObject level2 = new JSONObject();
		level2.put(Strings.MESSAGE, message);
		level2.put(Strings.USERNAME, Settings.getUsername());
		msg.put(Strings.MESSAGE, level2);
		sendJsonOnSocket(msg);
	}

	/** Sends a wakeup message, which starts the GHS algorithm */
	public void sendWakeup() {
		JSONObject jobj = new JSONObject();
		jobj.put(Strings.WAKE_UP, Strings.WAKE_UP);
		sendJsonOnSocket(jobj);
	}

	public void disconnect() {
		try {
			connected = false;
			input.close();
			output.close();
			clientSocket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
