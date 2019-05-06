package activitystreamer.server;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import activitystreamer.util.Settings;

public class Connection extends Thread {
	private static final Logger log = LogManager.getLogger();
	private DataInputStream in;
	private DataOutputStream out;
	private BufferedReader inreader;
	private PrintWriter outwriter;
	private boolean open = false;
	private Socket socket;
	private boolean term = false;

	private boolean hasReceivedConnect;

	private ConnectionState connectionState;
	private ConnectionInformation connectionInformation;

	Connection(Socket socket) throws IOException {
		in = new DataInputStream(socket.getInputStream());
		out = new DataOutputStream(socket.getOutputStream());
		inreader = new BufferedReader(new InputStreamReader(in));
		outwriter = new PrintWriter(out, true);
		this.socket = socket;
		hasReceivedConnect = false;
		open = true;
		start();
	}
	
	public ConnectionInformation getConnectionInformation() {
		return connectionInformation;
	}
	
	public void setConnectionInformation(ConnectionInformation ci) {
		this.connectionInformation = ci;
	}

	public void setConnectionState(ConnectionState conState) {
		connectionState = conState;
	}

	public void resetReceivedConnect() {
		hasReceivedConnect = false;
	}

	public void setReceivedConnect() {
		hasReceivedConnect = true;
	}
	
	public boolean getReceivedConnect() {
		return hasReceivedConnect;
	}

	public ConnectionState getConnectionState() {
		return connectionState;
	}

	/*
	 * returns true if the message was written, otherwise false
	 */
	public boolean writeMsg(String msg) {
		if (open) {
			outwriter.println(msg);
			outwriter.flush();
			return true;
		}
		return false;
	}

	public void closeCon() {
		if (open) {
			log.info("closing connection " + Settings.socketAddress(socket));
			try {
				term = true;
				inreader.close();
				out.close();
			} catch (IOException e) {
				// already closed?
				log.error("received exception closing the connection " + Settings.socketAddress(socket) + ": " + e);
			}
		}
	}

	public void run() {
		try {
			String data;
			while (!term && (data = inreader.readLine()) != null) {
				term = Control.getInstance().process(this, data);
			}
			log.debug("connection closed to " + Settings.socketAddress(socket));
			Control.getInstance().connectionClosed(this);
			in.close();
		} catch (IOException e) {
			log.error("connection " + Settings.socketAddress(socket) + " closed with exception: " + e);
			Control.getInstance().connectionClosed(this);
		}
		open = false;
	}

	public Socket getSocket() {
		return socket;
	}

	public boolean isOpen() {
		return open;
	}

	public void setTerm(boolean term) {
		this.term = term;
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}

		if (!(o instanceof Connection)) {
			return false;
		}

		Connection con = (Connection) o;

		if (con.getSocket().equals(this.socket)) {
			return true;
		}
		return false;
	}
}
