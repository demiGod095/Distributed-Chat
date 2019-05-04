package activitystreamer.server;

import java.io.IOException;
import java.net.Socket;

import activitystreamer.util.Settings;

public class ServerConnection extends Connection {

	private int lag;

	ServerConnection(Socket socket) throws IOException {
		super(socket);

		// Make the default lag the same as the lag defined in the settings.
		lag = Settings.LAG;
	}

	public int getLag() {
		return lag;
	}

	public void setLag(int newLag) {
		lag = newLag;
	}

}
