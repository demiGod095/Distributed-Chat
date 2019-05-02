package activitystreamer.server;

import java.util.Objects;

public class ClientRegistration {
	private String secret;
	private String username;
	
	public ClientRegistration(String username, String secret) {
		this.secret = secret;
		this.username = username;
	}
	
	public String getSecret() {
		return secret;
	}
	public String getUsername() {
		return username;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		ClientRegistration that = (ClientRegistration) o;
		return Objects.equals(getSecret(), that.getSecret()) &&
				Objects.equals(getUsername(), that.getUsername());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getSecret(), getUsername());
	}
}
