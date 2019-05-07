package activitystreamer.server;

import java.util.HashMap;
import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Consumes the messages, and then processes them, to form a GHS network */
public class Node implements Runnable {
	private static final Logger log = LogManager.getLogger();

	/** Maps between a Connection and a class reprenting the Edges of the network.
	 */
	private HashMap<Connection, ServerEdge> connectionEdgeMapper = new HashMap<Connection, ServerEdge>();
	private BlockingQueue<Message> queue;

	public Node(BlockingQueue<Message> queue) {
		this.queue = queue;
	}
	
	/** Process the message retrieved from the queue */
	private void processMessage(Message message) {
		System.out.println("MESSAGE TESTING");
		System.out.print(message.getMessage().toJSONString());
	}

	@Override
	public void run() {
		log.debug("Node Started");
		try {
			while (true) {
				// Take the Message from the queue
				Message msg = queue.take();
				processMessage(msg);
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
