package activitystreamer.server;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.Socket;
import java.util.*;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import activitystreamer.Server;
import activitystreamer.util.Settings;

public class Control extends Thread {
	private static final String JSON_COMMAND = "command";
	private static final Logger log = LogManager.getLogger();
	private static ArrayList<Connection> connections;

	private static ArrayList<ClientRegistration> registrations;
	private static HashMap<ClientRegistration, PendingClientRegistration> pendingRegistrations;

	private static Map<String, ServerAnnouncement> serverAnnounceMapper;
	private static List<ServerAnnouncement> serverLoadList;

	private static boolean term = false;
	private static Listener listener;

	private static final int SERVER_LOAD_DIFF = 2;

	public static final String USERNAME = "username";
	public static final String SECRET = "secret";
	public static final String ACTIVITY = "activity";
	public static final String INFO = "info";
	public static final String COMMAND = "command";
	public static final String ID = "id";
	public static final String LOAD = "load";
	public static final String HOSTNAME = "hostname";
	public static final String PORT = "port";

	public static final String AUTHENTICATE = "AUTHENTICATE";
	public static final String INVALID_MESSAGE = "INVALID_MESSAGE";
	public static final String AUTHENTICATION_FAIL = "AUTHENTICATION_FAIL";
	public static final String LOGIN = "LOGIN";
	public static final String LOGIN_SUCCESS = "LOGIN_SUCCESS";
	public static final String REDIRECT = "REDIRECT";
	public static final String LOGIN_FAILED = "LOGIN_FAILED";
	public static final String LOGOUT = "LOGOUT";
	public static final String ACTIVITY_MESSAGE = "ACTIVITY_MESSAGE";
	public static final String SERVER_ANNOUNCE = "SERVER_ANNOUNCE";
	public static final String ACTIVITY_BROADCAST = "ACTIVITY_BROADCAST";
	public static final String REGISTER = "REGISTER";
	public static final String REGISTER_FAILED = "REGISTER_FAILED";
	public static final String REGISTER_SUCCESS = "REGISTER_SUCCESS";
	public static final String LOCK_REQUEST = "LOCK_REQUEST";
	public static final String LOCK_DENIED = "LOCK_DENIED";
	public static final String LOCK_ALLOWED = "LOCK_ALLOWED";

	public static final String ANONYMOUS_USER = "anonymous";

    public static final String AUTHENTICATED_USER = "authenticated_user";

	protected static Control control = null;

	public static Control getInstance() {
		if (control == null) {
			control = new Control();
		}
		return control;
	}

	public Control() {
		// Initialise an array to hold client registrations
		registrations = new ArrayList<>();
		// initialize the connections array
		connections = new ArrayList<>();
		pendingRegistrations = new HashMap<>();

		serverAnnounceMapper = new HashMap<>();
		serverLoadList = new ArrayList<>();

		initiateConnection();
		// start a listener
		try {
			listener = new Listener();
		} catch (IOException e1) {
			log.fatal("failed to startup a listening thread: " + e1);
			System.exit(-1);
		}
	}

	private int getLoad() {
		int clients = 0;
		for (Connection con : connections) {
			if (con.getConnectionInformation() instanceof ClientConnectionInformation ||
					con.getConnectionInformation() instanceof AnonymousConnectionInformation) {
				clients++;
			}
		}
		return clients;
	}

	private void doServerAnnoucements() {
		int load = getLoad();
		for (Connection con : connections) {
			if (con.getConnectionInformation() instanceof ServerConnectionInformation) {
				makeServerAnnounce(load, con);
			}
		}
	}

	private void makeServerAnnounce(int load, Connection con) {
		JSONObject jobj = new JSONObject();
		jobj.put(COMMAND, SERVER_ANNOUNCE);
		jobj.put(ID, Server.ID);
		jobj.put(LOAD, load);
		jobj.put(HOSTNAME, Settings.getLocalHostname());
		jobj.put(PORT, Settings.getLocalPort());
		writeMessageOnSocket(con, jobj);
	}

	private ArrayList<Connection> getServers() {
		ArrayList<Connection> serverList = new ArrayList<>();
		for (Connection con : connections) {
			if (con.getConnectionInformation() instanceof ServerConnectionInformation) {
				serverList.add(con);
			}
		}
		return serverList;
	}

	private ArrayList<Connection> getKnownConnections() {
		ArrayList<Connection> knownConnections = new ArrayList<>();
		for (Connection con : connections) {
			if (con.getConnectionInformation() != null) {
				knownConnections.add(con);
			}
		}
		return knownConnections;
	}

	public void initiateConnection() {
		log.debug("Initiating Connection");
		// make a connection to another server if remote hostname is supplied
		if (Settings.getRemoteHostname() != null) {
			try {
				Connection outgoingCon = outgoingConnection(
						new Socket(Settings.getRemoteHostname(), Settings.getRemotePort()));
				sendAuth(outgoingCon, Settings.getSecret());
				outgoingCon.setConnectionInformation(new ServerConnectionInformation(Settings.getSecret()));
			} catch (IOException e) {
				log.error("failed to make connection to " + Settings.getRemoteHostname() + ":"
						+ Settings.getRemotePort() + " :" + e);
				System.exit(-1);
			}
		}
	}

	private JSONObject convert2JSON(String message) throws ParseException {
		JSONParser parser = new JSONParser();
		JSONObject json = (JSONObject) parser.parse(message);
		return json;
	}

	private void writeMessageOnSocket(Connection con, JSONObject json) {
		Socket sock = con.getSocket();
		OutputStream ostream;
		try {
			ostream = sock.getOutputStream();
			PrintWriter pw = new PrintWriter(ostream, true);
			pw.println(json.toString());
			pw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private void sendInvalidMessage(Connection con, String infoMessage) {
		JSONObject jobj = new JSONObject();
		jobj.put(COMMAND, INVALID_MESSAGE);
		jobj.put(INFO, infoMessage);
		writeMessageOnSocket(con, jobj);
	}

	private void sendCommandWInfo(Connection con, String command, String message) {
		JSONObject jobj = new JSONObject();
		jobj.put(COMMAND, command);
		jobj.put(INFO, message);
		writeMessageOnSocket(con, jobj);
	}

	private void sendLoginSuccess(Connection con) {
		JSONObject jobj = new JSONObject();
		jobj.put(COMMAND, LOGIN_SUCCESS);
		jobj.put(INFO, "Logged in as user anonymous");
		writeMessageOnSocket(con, jobj);
	}

	private void sendLoginSuccess(Connection con, ClientRegistration rego) {
		JSONObject jobj = new JSONObject();
		jobj.put(COMMAND, LOGIN_SUCCESS);
		jobj.put(INFO, "Logged in as user " + rego.getUsername());
		writeMessageOnSocket(con, jobj);
	}

	private void sendLoginFailed(Connection con) {
		JSONObject jobj = new JSONObject();
		jobj.put(COMMAND, LOGIN_FAILED);
		jobj.put(INFO, "attempt to login with an unknown username/secret combination");
		writeMessageOnSocket(con, jobj);
	}

	private void sendRedirect(Connection con) {
		JSONObject jobj = new JSONObject();
		ServerAnnouncement altServer = serverLoadList.get(0);
		jobj.put(COMMAND, REDIRECT);
		jobj.put(HOSTNAME, altServer.getHostname());
		jobj.put(PORT, altServer.getPort());
		writeMessageOnSocket(con, jobj);
		altServer.setLoad(altServer.getLoad() + 1);
		serverLoadList.sort(new ServerLoadComparator());
	}

	/***
	 * Checks if a new client can be inserted into the collection of clients
	 *
	 * @param newClient
	 *            The client to insert
	 * @return whether a registration is valid
	 */
	private boolean validRegistration(ClientRegistration newClient) {
		for (ClientRegistration client : registrations) {
			if (client.getUsername().equals(newClient.getUsername())) {
				return false;
			}
		}
		return true;
	}

	private void sendAuth(Connection con, String secret) {
		JSONObject jobj = new JSONObject();
		jobj.put(COMMAND, AUTHENTICATE);
		jobj.put(SECRET, secret);
		writeMessageOnSocket(con, jobj);
	}

	private void sendRegisterFailed(Connection con, String message) {
		JSONObject jobj = new JSONObject();
		jobj.put(COMMAND, REGISTER_FAILED);
		jobj.put(INFO, message);
		writeMessageOnSocket(con, jobj);
	}

	private void sendRegisterSuccessful(Connection con, String message) {
		JSONObject jobj = new JSONObject();
		jobj.put(COMMAND, REGISTER_SUCCESS);
		jobj.put(INFO, message);
		writeMessageOnSocket(con, jobj);
	}

	private void sendLockRequest(ClientRegistration cliReg, Connection server) {
		JSONObject jobj = new JSONObject();
		jobj.put(COMMAND, LOCK_REQUEST);
		jobj.put(USERNAME, cliReg.getUsername());
		jobj.put(SECRET, cliReg.getSecret());
		writeMessageOnSocket(server, jobj);
	}

	private void sendLockAllowed(ClientRegistration cliReg, Connection server) {
		JSONObject jobj = new JSONObject();
		jobj.put(COMMAND, LOCK_ALLOWED);
		jobj.put(USERNAME, cliReg.getUsername());
		jobj.put(SECRET, cliReg.getSecret());
		writeMessageOnSocket(server, jobj);
	}

	private void sendLockDenied(ClientRegistration cliReg, Connection server) {
		log.debug("Sending Lock Denied for " + cliReg.getUsername());
		JSONObject jobj = new JSONObject();
		jobj.put(COMMAND, LOCK_DENIED);
		jobj.put(USERNAME, cliReg.getUsername());
		jobj.put(SECRET, cliReg.getSecret());
		writeMessageOnSocket(server, jobj);
	}

	private void sendActivity(JSONObject activity, Connection receivedConnection) {
		log.debug("Sending Activity	Broadcast");
		JSONObject jobj = new JSONObject();
		jobj.put(COMMAND, ACTIVITY_BROADCAST);
		jobj.put(ACTIVITY, activity);

		for (Connection connection : getKnownConnections()) {
		    if (connection != receivedConnection) {
				writeMessageOnSocket(connection, jobj);
			}
		}
	}

	private boolean processLockRequest(String username, String secret, Connection con) {
		log.debug("Received Lock Request for " + username);

		ClientRegistration client = new ClientRegistration(username, secret);

	    // Propagate LOCK_REQUEST
		for (Connection server : getServers()) {
			if (!(server.equals(con))) {
				sendLockRequest(client, server);
			}
		}

		// Handle the registration
		if (validRegistration(client)) {
			registrations.add(client);

			for (Connection server : getServers()) {
			    sendLockAllowed(client, server);
			}
		} else {
			for (Connection connection : getServers()) {
				sendLockDenied(client, connection);
			}
		}
		return false;
	}

	private boolean processLockAllowed(String username, String secret, Connection con) {
		log.debug("Received Lock Allowed for " + username);

		ClientRegistration client = new ClientRegistration(username, secret);

		// Propagate LOCK_ALLOWED
		for (Connection server : getServers()) {
			if (!(server.equals(con))) {
				sendLockAllowed(client, server);
			}
		}

		// Track registration approvals from other servers so long as we are the "seed" server
		if (pendingRegistrations.containsKey(client)) {
			PendingClientRegistration pendingClient = pendingRegistrations.get(client);
			pendingClient.serverApprovedRegistration();

			// All known servers approved the registration, so inform the client
			if (pendingClient.allServersApprovedRegistration()) {
				sendRegisterSuccessful(pendingClient.getClient(), "register success for " + username);
				pendingRegistrations.remove(client);
			}
		}

		return false;
	}

	private boolean processLockDenied(String username, String secret, Connection con) {
		log.debug("Received Lock Denied for " + username);

		ClientRegistration client = new ClientRegistration(username, secret);

		// Propagate LOCK_DENIED
		for (Connection server : getServers()) {
			if (!(server.equals(con))) {
				sendLockDenied(client, server);
			}
		}

		// Delete client registration
        registrations.removeIf(registration -> registration.getUsername().equals(username));

		// Inform the client if we are the "seed" server of the rejection
        if (pendingRegistrations.containsKey(client)) {
            Connection clientConnection = pendingRegistrations.get(client).getClient();
			sendRegisterFailed(clientConnection,
					client.getUsername() + " is already registered with the system");

			// TODO - check that closing doesn't prevent message from being sent
			connections.remove(clientConnection);
			clientConnection.setTerm(true);
		}

		return false;
	}

	private boolean processRegister(String username, String secret, Connection con) {
		if (username.equals("anonymous")) {
			sendInvalidMessage(con, "Cannot Register anonymous");
			return true;
		}

		ClientRegistration client = new ClientRegistration(username, secret);

		// Check the local client list
		if (validRegistration(client)) {
			// Register the client
            registrations.add(client);

			// If we're the only server, we can immediately inform the client of approval
			if (getServers().size() == 0) {
				sendRegisterSuccessful(con, "register success for " + client.getUsername());
				return false;
			}

			// Otherwise, need to contact all the other servers for approval
			pendingRegistrations.put(client, new PendingClientRegistration(con, serverLoadList.size()));

            // Request the registration from all servers
            for (Connection server : getServers()) {
            	if (!(server.equals(con))) {
            		sendLockRequest(client, server);
				}
			}
		} else {
			sendRegisterFailed(con, client.getUsername() + " is already registered with the system");
			return true;
		}
		return false;
	}

	private boolean processLogin(String username, String secret, Connection con) {
		for (ClientRegistration cliReg : registrations) {
			if (cliReg.getUsername().equals(username) && cliReg.getSecret().equals(secret)) {
				// Successful Login
				sendLoginSuccess(con, cliReg);
				con.setConnectionInformation(new ClientConnectionInformation(secret, username));
				return false;
			}
		}
		sendLoginFailed(con);
		return true;
	}

	/*
	 * Processing incoming messages from the connection. Return true if the
	 * connection should close.
	 */
	public synchronized boolean process(Connection con, String msg) {
		// Write a reply back
		Socket sock = con.getSocket();
		try {
			JSONObject jobj = convert2JSON(msg);
			// Look for a command in the message
			if (jobj.get(JSON_COMMAND) instanceof String) {
				// Now check it for a valid command
				String command = (String) jobj.get(JSON_COMMAND);
				switch (command) {
				case AUTHENTICATE:
					if (jobj.get(SECRET) instanceof String) {
						log.debug("Authenticating");
						if (authenticate(jobj, con)) {
						    connections.remove(con);
						    return true;
						} else {
							return false;
						}
					} else {
						sendInvalidMessage(con, "Malformed Authentication");
						connections.remove(con);
						return true;
					}
				case AUTHENTICATION_FAIL:
					log.info("Received Authentication Fail from remote server. Check secret");
					connections.remove(con);
					return true;
				case LOGIN:
					if (con.getConnectionInformation() instanceof ServerConnectionInformation) {
						sendInvalidMessage(con, "Servers cannot Login");
						connections.remove(con);
						return true;
					}

					if (jobj.get(USERNAME) instanceof String) {
						String username = (String) jobj.get(USERNAME);
						if (username.equals(ANONYMOUS_USER)) {
							con.setConnectionInformation(new AnonymousConnectionInformation());
							sendLoginSuccess(con);
						} else {
							// Username with secret
							if (jobj.get(SECRET) instanceof String) {
								String secret = (String) jobj.get(SECRET);
								Boolean didLoginNotSucceed = processLogin(username, secret, con);
								if (didLoginNotSucceed) {
									connections.remove(con);
									return true;
								}
							} else {
								sendInvalidMessage(con, "Secret Missing");
								connections.remove(con);
								return true;
							}
						}

						// Check the server load to see if a redirect needs to occur
						if (checkServerLoads()) {
							sendRedirect(con);
							connections.remove(con);
							return true;
						} else {
							return false;
						}
					} else {
						sendInvalidMessage(con, "Username Missing");
						connections.remove(con);
						return true;
					}
				case REDIRECT:
					log.debug("Perform Redirect");
					sendInvalidMessage(con, "Server cannot redirect");
					connections.remove(con);
					return true;
				case LOGOUT:
					if (con.getConnectionInformation() instanceof ServerConnectionInformation) {
						sendInvalidMessage(con, "Servers Cannot Logout");
						connections.remove(con);
						return true;
					}
					log.debug("Client logging out");
					logout(con);
					connections.remove(con);
					return true;
				case ACTIVITY_MESSAGE:
					if (!(con.getConnectionInformation() instanceof ClientConnectionInformation ||
							con.getConnectionInformation() instanceof AnonymousConnectionInformation)) {
						sendInvalidMessage(con, "Only logged in clients can broadcast ACTIVITY_MESSAGE");
						connections.remove(con);
						return true;
					}

					if (!(jobj.get(USERNAME) instanceof String) ||
							!(jobj.get(SECRET) instanceof String || jobj.get(SECRET) == null) ||
							!(jobj.get(ACTIVITY) instanceof JSONObject)) {
						sendInvalidMessage(con, "Invalid ACTIVITY_BROADCAST message received");
						connections.remove(con);
						return true;
					}

					String username = (String) jobj.get(USERNAME);
					String secret = (String) jobj.get(SECRET);

					JSONObject activity = (JSONObject) jobj.get(ACTIVITY);
					activity.put(AUTHENTICATED_USER, username);

					if (con.getConnectionInformation() instanceof AnonymousConnectionInformation &&
							!username.equals(ANONYMOUS_USER)) {
						sendCommandWInfo(con, AUTHENTICATION_FAIL,
								"The username and secret combination do not match your credentials");
						connections.remove(con);
						return true;
					} else if (con.getConnectionInformation() instanceof ClientConnectionInformation) {
					    ClientConnectionInformation connectionInfo = (ClientConnectionInformation) con.getConnectionInformation();
					    if (!connectionInfo.getUsername().equals(username) || !connectionInfo.getSecret().equals(secret)) {
							sendCommandWInfo(con, AUTHENTICATION_FAIL,
									"The username and secret combination do not match your credentials");
							connections.remove(con);
							return true;
						}
					}

					sendActivity(activity, con);
					return false;
				case SERVER_ANNOUNCE:
					if (!(con.getConnectionInformation() instanceof ServerConnectionInformation)) {
						sendInvalidMessage(con, "Not a server connection");
						connections.remove(con);
						return true;
					}
					if (!((ServerConnectionInformation) con.getConnectionInformation()).getAuthenticated()) {
						sendInvalidMessage(con, "Unauthenticated server connection");
						connections.remove(con);
						return true;
					}
					try {
						String id = (String) jobj.get(ID);
						Long load = (Long) jobj.get(LOAD);
						String hostname = (String) jobj.get(HOSTNAME);
						Long port = (Long) jobj.get(PORT);

						if (id == null || load == null || hostname == null || port == null) {
							sendInvalidMessage(con, "Required field missing for Server Announce");
							connections.remove(con);
							return true;
						}

						if (serverAnnounceMapper.containsKey(id)) { // Assumes ip and port don't change
							ServerAnnouncement sa = serverAnnounceMapper.get(id);
							if (sa.getLoad() != load){
								sa.setLoad(load);
								serverLoadList.sort(new ServerLoadComparator());
							}
						} else {
							ServerAnnouncement sa = new ServerAnnouncement(id, hostname, port, load);
							serverAnnounceMapper.put(id, sa);
							serverLoadList.add(sa);
							serverLoadList.sort(new ServerLoadComparator());
						}
						// Propagate server announcement
						for (Connection connection : getServers()) {
							if (!(connection.equals(con))) {
								writeMessageOnSocket(connection, jobj);
							}
						}
						return false;

					} catch (Exception e) {
						e.printStackTrace();
						log.debug("Invalid Server Announce");
						sendInvalidMessage(con, "Incorrect server announce message");
						connections.remove(con);
						return true;
					}
				case ACTIVITY_BROADCAST:
					if (!(con.getConnectionInformation() instanceof ServerConnectionInformation)) {
						sendInvalidMessage(con, "Only Authenticated Servers can broadcast ACTIVITY_BROADCAST");
						connections.remove(con);
						return true;
					}
					if (jobj.get(ACTIVITY) instanceof JSONObject) {
						JSONObject message_activity = (JSONObject) jobj.get(ACTIVITY);
						sendActivity(message_activity, con);
						return false;
					} else {
						sendInvalidMessage(con, "Invalid ACTIVITY_BROADCAST message received");
						connections.remove(con);
						return true;
					}
				case LOCK_DENIED:
					if (!(con.getConnectionInformation() instanceof ServerConnectionInformation)) {
						sendInvalidMessage(con, "Only Authenticated Servers can broadcast LOCK_DENIED");
						connections.remove(con);
						return true;
					}
					if (jobj.get(USERNAME) instanceof String && jobj.get(SECRET) instanceof String) {
						String message_username = (String) jobj.get(USERNAME);
						String message_secret = (String) jobj.get(SECRET);
						if (processLockDenied(message_username, message_secret, con)) {
							connections.remove(con);
							return true;
						} else {
							return false;
						}
					} else {
						sendInvalidMessage(con, "Invalid LOCK_DENIED request received");
						connections.remove(con);
						return true;
					}
				case LOCK_ALLOWED:
					log.debug("Received Lock Allowed");
					if (!(con.getConnectionInformation() instanceof ServerConnectionInformation)) {
						sendInvalidMessage(con, "Only Authenticated Servers can broadcast LOCK_ALLOWED");
						connections.remove(con);
						return true;
					}
					if (jobj.get(USERNAME) instanceof String && jobj.get(SECRET) instanceof String) {
						String message_username = (String) jobj.get(USERNAME);
						String message_secret = (String) jobj.get(SECRET);
						if (processLockAllowed(message_username, message_secret, con)) {
							connections.remove(con);
							return true;
						} else {
							return false;
						}
					} else {
						sendInvalidMessage(con, "Invalid Lock request received");
						connections.remove(con);
						return true;
					}
				case LOCK_REQUEST:
					// Make sure it is an authenticated server
					log.debug("Lock Request Received");
					if (con.getConnectionInformation() == null) {
						sendInvalidMessage(con, "Not Authenticated");
						connections.remove(con);
						return true;
					}
					if (!(con.getConnectionInformation() instanceof ServerConnectionInformation)) {
						sendInvalidMessage(con, "Clients cannot perform lock requests");
						connections.remove(con);
						return true;
					}

					// Check Local list
					if (jobj.get(USERNAME) instanceof String && jobj.get(SECRET) instanceof String) {
						String message_username = (String) jobj.get(USERNAME);
						String message_secret = (String) jobj.get(SECRET);
						if (processLockRequest(message_username, message_secret, con)) {
							connections.remove(con);
							return true;
						} else {
							return false;
						}
					} else {
						sendInvalidMessage(con, "Error parsing Username or Password");
						connections.remove(con);
						return true;
					}
				case INVALID_MESSAGE:
					connections.remove(con);
					return true;
				case REGISTER:
					log.debug("Received Registration Request");
					if (con.getConnectionInformation() instanceof ClientConnectionInformation ||
							con.getConnectionInformation() instanceof AnonymousConnectionInformation) {
						sendInvalidMessage(con,
								"Already Registered. Please logout if you want to register a new account");
						connections.remove(con);
						return true;
					}
					if (con.getConnectionInformation() instanceof ServerConnectionInformation) {
						sendInvalidMessage(con, "Only Clients can register");
						connections.remove(con);
						return true;
					}
					if (jobj.get(USERNAME) instanceof String && jobj.get(SECRET) instanceof String) {
						String message_username = (String) jobj.get(USERNAME);
						String message_secret = (String) jobj.get(SECRET);
						if (processRegister(message_username, message_secret, con)) {
							connections.remove(con);
							return true;
						} else {
							return false;
						}
					} else {
						sendInvalidMessage(con, "The Registration Request is Invalid");
						connections.remove(con);
						return true;
					}
				default:
					sendInvalidMessage(con, "Invalid Command");
					connections.remove(con);
					return true;
				}
			} else {
				sendInvalidMessage(con, "the received message did not contain a command");
				connections.remove(con);
				return true;
			}
		} catch (ParseException e) {
			e.printStackTrace();
			sendInvalidMessage(con, "JSON Parse Error while parsing message");
			connections.remove(con);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			sendInvalidMessage(con, "Internal Server Error encountered.");
			connections.remove(con);
			return true;
		}
	}

	private boolean logout(Connection con) {
		pendingRegistrations.values().remove(con);
		// Close the socket
		return true;
	}

	private boolean authenticate(JSONObject jobj, Connection con) {
		// Write code for authentication
		String secret = null;
		if (con.getConnectionInformation() instanceof ServerConnectionInformation) {
			// The server is already Authenticated
			sendInvalidMessage(con, "Already Authenticated");
			return true;
		}
		if (con.getConnectionInformation() instanceof ClientConnectionInformation ||
				con.getConnectionInformation() instanceof AnonymousConnectionInformation) {
			sendInvalidMessage(con, "You are a Client");
			return true;
		}
		if (jobj.get(SECRET) instanceof String) {
			secret = (String) jobj.get(SECRET);
			if (secret != null && secret.equals(Settings.getSecret())) {
				// Perform Authentication
				con.setConnectionInformation(new ServerConnectionInformation(secret));
				return false;
			}
			sendCommandWInfo(con, AUTHENTICATION_FAIL, "The supplied secret is incorrect: " + secret);
			return true;
		}
		sendCommandWInfo(con, AUTHENTICATION_FAIL, "No Secret Supplied");
		return true;
	}

	/**
	 * @return true if valid server to redirect to, false otherwise
	 */
	private boolean checkServerLoads() {
		return ((serverLoadList.size() > 0)
				&& ((this.getLoad() - serverLoadList.get(0).getLoad()) > SERVER_LOAD_DIFF));
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
	public synchronized Connection outgoingConnection(Socket s) throws IOException, ConnectException {
		log.debug("outgoing connection: " + Settings.socketAddress(s));
		Connection c = new Connection(s);
		connections.add(c);
		return c;
	}

	@Override
	public void run() {
		log.info("Starting Server Thread");
		// If started alone, print the secret
		if (Settings.getRemoteHostname() == null) {
			// Generate a secret and print the secret to screen
			Settings.setSecret(Settings.nextSecret());
			log.info("Secret " + Settings.getSecret());
		} else {
			// Connect to remote server
			log.debug("Connecting to remote server " + Settings.getRemoteHostname());
			try {
				Socket serverSocket = new Socket(Settings.getRemoteHostname(), Settings.getRemotePort());
				Connection serverConn = new Connection(serverSocket);
			} catch (IOException e) {
				log.debug("Error connecting to remote server");
				e.printStackTrace();
			}
		}

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
		doServerAnnoucements();
		return false;
	}

	public final void setTerm(boolean t) {
		term = t;
	}

	public final ArrayList<Connection> getConnections() {
		return connections;
	}
}
