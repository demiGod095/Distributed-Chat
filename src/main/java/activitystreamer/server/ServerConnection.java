package activitystreamer.server;

import java.io.IOException;
import java.net.Socket;

public class ServerConnection extends Connection{

	ServerConnection(Socket socket) throws IOException {
		super(socket);
	}
	
}
