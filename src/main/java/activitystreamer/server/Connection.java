package activitystreamer.server;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import activitystreamer.util.Settings;

/**
 * A Thread, which is created for every connection. This thread then contains
 * the socket, and then methods for writing messages on the socket. It also
 * provides a mechanism for simulating lag, by delaying any messages sent out,
 * by the amount defined for the connection.*
 */
public class Connection extends Thread {
	private static final Logger log = LogManager.getLogger();
	private DataInputStream in;
	private DataOutputStream out;
	private BufferedReader inreader;
	private PrintWriter outwriter;
	private boolean open = false;
	private Socket socket;
	private boolean term = false;
	private int lag = 0;

	Connection(Socket socket) throws IOException {
		in = new DataInputStream(socket.getInputStream());
		out = new DataOutputStream(socket.getOutputStream());
		inreader = new BufferedReader(new InputStreamReader(in));
		outwriter = new PrintWriter(out, true);
		this.socket = socket;
		open = true;
		start();
	}

	public synchronized int getLag() {
		return lag;
	}

	public synchronized void setLag(int lag) {
		this.lag = lag;
	}

	/**
	 * Sends the message, on the socket contained in this connection. This adds any
	 * delay, as defined in the lag parameter, to simulate a laggy connection
	 * 
	 * @param msg The Message, as a string, to send on the socket.
	 * @return True if starting the thread to send the message was sucessful. False
	 *         otherwise
	 */
	public boolean writeMsg(String msg) {
		if (open) {
			Thread delayedMsg = new Thread(new DelayedMessage(outwriter, msg, lag));
			delayedMsg.start();
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
			try {
				Control.getInstance().connectionClosed(this);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		} catch (Exception e) {
			e.printStackTrace();
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
