package activitystreamer.server;

public class ClientConnectionInformation extends ConnectionInformation{
	private String secret;
	private String username;

	public ClientConnectionInformation(String secret, String username) {
		this.secret = secret;
		this.username = username;
	}
	
	public String getUsername( ) { return username; }
	public void setUsername(String username) { this.username = username; }
	public String getSecret( ) { return secret; }
}
