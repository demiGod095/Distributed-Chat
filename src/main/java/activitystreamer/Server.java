package activitystreamer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import activitystreamer.server.Control;
import activitystreamer.server.Message;
import activitystreamer.server.Node;
import activitystreamer.util.IpPortLagTriplet;
import activitystreamer.util.Settings;

/**
 * A Class representing the entry point for our server. This code was adapted
 * from Aaron Harwoods code For the activity Streamer for COMP90015 last year.
 */
public class Server {
	private static final Logger log = LogManager.getLogger();

	public static final String ID = Settings.nextSecret();

	private static void help(Options options) {
		String header = "An Chat Server, that implements the GHS Algorithm\n\n";
		String footer = "\ncontact phudgell@student.unimelb.edu.au for issues.";
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("ActivityStreamer.Server", header, options, footer, true);
		System.exit(-1);
	}

	// Create the servers UUID
	public static UUID SERVER_UUID = UUID.randomUUID();

	private static BlockingQueue<Message> queue = new LinkedBlockingQueue<Message>();

	public static void main(String[] args) {
		log.debug("UUID " + SERVER_UUID);

		log.info("reading command line options");

		Options options = new Options();

		options.addOption("lp", true, "local port number");
		options.addOption("lh", true, "local hostname");
		options.addOption("a", true, "activity interval in milliseconds");
		options.addOption("lg", true, "Lag to Simulate");
		options.addOption("t", true, "Test");
		Option remoteServers = new Option("rs", true,
				"remote server, specified in ip:port:lag format, separated by a colon");
		remoteServers.setArgs(Option.UNLIMITED_VALUES);
		options.addOption(remoteServers);

		// build the parser
		CommandLineParser parser = new DefaultParser();

		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e1) {
			help(options);
		}

		if (cmd.hasOption("rs")) {

			try {
				for (String remoteServer : cmd.getOptionValues("rs")) {
					String[] splitStr = remoteServer.split(":");
					if (splitStr.length == 3) {
						int serverPort;
						int serverLag;
						String serverHost;
						serverHost = splitStr[0];
						serverPort = Integer.parseInt(splitStr[1]);
						serverLag = Integer.parseInt(splitStr[2]);
						Settings.ipPorts.add(new IpPortLagTriplet(serverHost, serverPort, serverLag));
						log.debug("Remote Server " + serverHost + " port " + serverPort + " with lag " + serverLag);

					} else {
						throw new Exception("Incorrect IP Port Lag pair specified");
					}
				}
			} catch (NumberFormatException e) {
				log.debug("Error Parsing Port Number");
			} catch (Exception e) {
				log.error(e.getMessage());
			}
		}

		if (cmd.hasOption("lp")) {
			try {
				int port = Integer.parseInt(cmd.getOptionValue("lp"));
				Settings.setLocalPort(port);
			} catch (NumberFormatException e) {
				log.info("-lp requires a port number, parsed: " + cmd.getOptionValue("lp"));
				help(options);
			}
		}

		if (cmd.hasOption("a")) {
			try {
				int a = Integer.parseInt(cmd.getOptionValue("a"));
				Settings.setActivityInterval(a);
			} catch (NumberFormatException e) {
				log.error("-a requires a number in milliseconds, parsed: " + cmd.getOptionValue("a"));
				help(options);
			}
		}

		try {
			Settings.setLocalHostname(InetAddress.getLocalHost().getHostAddress());
		} catch (UnknownHostException e) {
			log.warn("failed to get localhost IP address");
		}

		if (cmd.hasOption("lh")) {
			Settings.setLocalHostname(cmd.getOptionValue("lh"));
		}
		if (cmd.hasOption("lg")) {
			try {
				int lag = Integer.parseInt(cmd.getOptionValue("lg"));
				Settings.setLag(lag);
			} catch (NumberFormatException e) {
				log.error("-lg requires a lag number in miliseconds, parsed: " + cmd.getOptionValue("lg"));
				help(options);
			}
		}

		log.info("starting server");

		// Make the Consumer Node
		Thread node = new Thread(new Node(queue));
		node.start();

		final Control c = Control.initInstance(queue);
		c.run();

		// the following shutdown hook doesn't really work, it doesn't give us enough
		// time to
		// cleanup all of our connections before the jvm is terminated.
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				c.setTerm(true);
				c.interrupt();
			}
		});

	}

}
