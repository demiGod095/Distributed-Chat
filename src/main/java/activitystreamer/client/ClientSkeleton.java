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

import static java.lang.StrictMath.toIntExact;

public class ClientSkeleton extends Thread {
	public static final String USERNAME = "username";
	public static final String SECRET = "secret";
	public static final String COMMAND = "command";
	public static final String LOGIN = "LOGIN";
	public static final String ANONYMOUS = "anonymous";
	public static final String REGISTER = "REGISTER";
	public static final String LOGOUT = "LOGOUT";
	public static final String REGISTER_FAIL = "REGISTER_FAILED";
	public static final String REGISTER_SUCCESS = "REGISTER_SUCCESS";
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

	/*
	 * Performs an anonmyous login
	 */
	private void performLogin() {
		log.debug("Performing Anomyous Login");
		JSONObject jobj = new JSONObject();
		jobj.put(COMMAND, LOGIN);
		jobj.put(USERNAME, ANONYMOUS);
		sendActivityObject(jobj);
	}

	private void performLogin(String username, String secret) {
		JSONObject jobj = new JSONObject();
		jobj.put(COMMAND, LOGIN);
		jobj.put(USERNAME, username);
		jobj.put(SECRET, secret);
		sendActivityObject(jobj);
	}

	private void sendRegistration(String username, String secret) {
		JSONObject jobj = new JSONObject();
		jobj.put(COMMAND, REGISTER);
		jobj.put(USERNAME, username);
		jobj.put(SECRET, secret);
		sendActivityObject(jobj);
	}

	private void processReply(String reply) {
		JSONParser parse = new JSONParser();
		try {
			Object obj = parse.parse(reply);
			JSONObject jobj = (JSONObject) obj;
			if (jobj.containsKey(COMMAND)) {
				if (jobj.get(COMMAND).equals(REGISTER_FAIL)) {
					// Client Registration fail. Abort
					log.error(
							"Client is attempting to register a username that is already registered.\n Please pick a new username and try again");
					textFrame.setOutputText(jobj);					
				} else if (jobj.get(COMMAND).equals(REGISTER_SUCCESS)) {
					// Perform Login with new secret
					performLogin(Settings.getUsername(), Settings.getSecret());
					log.info("Client registered with secret " + Settings.getSecret());
				}
			} else {
				// ABORT, server doesn't reply to logins
				log.error("Server sending invalid reply to Registration attempt.");
				System.exit(1);
			}
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
			log.debug("Username " + Settings.getUsername());
			if (Settings.getUsername().equals(ANONYMOUS)) {
				// Log in as anonymous
				log.debug("Performing Anonmynous Login");
				performLogin();
			} else if (Settings.getSecret() == null) {
				// Register the username. If it is sucessful then login
				String secret = Settings.nextSecret();
				Settings.setSecret(secret);
				sendRegistration(Settings.getUsername(), secret);

				// Proces reply
				String reply = input.readLine();
				log.debug("REPLY " + reply);
				// Process the reply
				processReply(reply);

			} else {
				performLogin(Settings.getUsername(), Settings.getSecret());
			}
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
	public void sendActivityObject(JSONObject activityObj) {
		// TODO Write code to submit ac)tivity object to server
		pwrite.println(activityObj.toJSONString());
		pwrite.flush();
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
					obj = (JSONObject) parse.parse(receivedMessage);
					textFrame.setOutputText(obj);

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

	private void sendLogout() {
		JSONObject jobj = new JSONObject();
		jobj.put(COMMAND, LOGOUT);
		sendActivityObject(jobj);
	}

	public void disconnect() {
		try {
			connected = false;
			sendLogout();
			input.close();
			output.close();
			clientSocket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
