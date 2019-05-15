package activitystreamer.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A class representing an IP port and lag triplet, representing the information
 * needed to make a remote server connection.
 * 
 * @author Patrick
 *
 */
public class IpPortLagTriplet {
	private InetAddress ipAddress;
	int port;
	int lag;
	private static final Logger log = LogManager.getLogger();

	
	public IpPortLagTriplet(String ipAddress, int port, int lag) {
		try {
			this.ipAddress = InetAddress.getByName(ipAddress);
			this.port = port;
			this.lag = lag;
		} catch (UnknownHostException e) {
			log.error("Unknown Host");
			e.printStackTrace();
		}
	}
	
	public InetAddress getIpAddress() {
		return ipAddress;
	}
	
	public int getPort() {
		return port;
	}
	
	public int getLag() {
		return lag;
	}
			
}
