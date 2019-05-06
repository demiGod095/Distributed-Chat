package activitystreamer.server;

import java.io.IOException;
import java.net.Socket;
import java.util.UUID;

import activitystreamer.util.Settings;

public class ServerConnectionInformation extends ConnectionInformation{
	private int lag;
	private Identifier ident;

	ServerConnectionInformation() {
		// Make the default lag the same as the lag defined in the settings.
		lag = Settings.LAG;		
	}
	
	public void setIdentifier(UUID nodeUUID, UUID otherUUID) {
		ident = new Identifier(nodeUUID, otherUUID);
	}

	public int getLag() {
		return lag;
	}

	public void setLag(int newLag) {
		lag = newLag;
	}
	
	public Identifier getIdentifier() {
		return ident;
	}
}
