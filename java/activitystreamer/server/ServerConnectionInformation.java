package activitystreamer.server;

public class ServerConnectionInformation extends ConnectionInformation{
	private String secret;
	public ServerConnectionInformation(String secret) {
		this.authenticated = true;
		this.secret = secret;
	}
	
	private boolean authenticated = false;
	public boolean getAuthenticated() { return authenticated; }
	public void setAuthenticated() { this.authenticated = true; }
}
