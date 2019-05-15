package activitystreamer.server;

import java.io.PrintWriter;

/**
 * A class used by the control, representing a delayed message.
 * @author Patrick
 *
 */
public class DelayedMessage implements Runnable {

	private PrintWriter pw;
	private String msg;
	private int lag;

	public DelayedMessage(PrintWriter printWrite, String message, int lag) {
		pw = printWrite;
		msg = message;
		this.lag = lag;
	}

	@Override
	public void run() {
		try {
			Thread.sleep(lag);
			pw.println(msg);
			pw.flush();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return;
	}
}
