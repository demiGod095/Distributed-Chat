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

	private TextFrame textFrame;

	private Socket clientSocket;

	public static ClientSkeleton getInstance() {
		if (clientSolution == null) {
			clientSolution = new ClientSkeleton();
		}
		return clientSolution;
	}

	private void processReply(String reply) {
		JSONParser parse = new JSONParser();
		try {
			Object obj = parse.parse(reply);
			JSONObject jobj = (JSONObject) obj;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public ClientSkeleton() {
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
					System.out.println("RECEIVED MESSAGE " + receivedMessage);
					obj = (JSONObject) parse.parse(receivedMessage);
					System.out.println("Received Object " + obj);
					if (obj.containsKey(Strings.MESSAGE)) {
						textFrame.setOutputText(obj.get(Strings.MESSAGE).toString());
					}

					if (obj.containsKey("command")) {
						String command = (String) obj.get("command");
						if (command.equals("REDIRECT")) {
							log.debug("recieved redirect");
							String newHost = (String) obj.get("hostname");
							int newPort = toIntExact((Long) obj.get("port"));
							try {
								clientSocket.close();
								clientSocket = new Socket(newHost, newPort);
								output = clientSocket.getOutputStream();
								pwrite = new PrintWriter(output, true);
								input = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
							} catch (UnknownHostException e) {
								log.error("Unknown Host Error " + Settings.getRemoteHostname());
								e.printStackTrace();
								return true;
							} catch (IOException e) {
								log.error("IO Exception");
								e.printStackTrace();
								return true;
							}
						}

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
