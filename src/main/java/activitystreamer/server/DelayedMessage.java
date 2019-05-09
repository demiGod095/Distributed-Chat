package activitystreamer.server;

import org.json.simple.JSONObject;

public class DelayedMessage implements Runnable{
	private JSONObject jsonMessage;
	int lag;
	private Connection connection;
	
	DelayedMessage(JSONObject message, int lag, Connection connection) {
		jsonMessage = message;
		this.lag = lag;
		this.connection = connection;
	}
	
	public void run () {
		try {
			Thread.sleep(lag);
			connection.writeMsg(jsonMessage.toJSONString());
		} catch (InterruptedException e) {
			System.out.println("Interrupted Exception");
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
