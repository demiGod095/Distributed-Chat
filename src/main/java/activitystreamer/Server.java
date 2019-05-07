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
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import activitystreamer.server.Control;
import activitystreamer.server.Message;
import activitystreamer.server.Node;
import activitystreamer.util.Settings;

public class Server {
	private static final Logger log = LogManager.getLogger();

	public static final String ID = Settings.nextSecret();

	private static void help(Options options) {
		String header = "An ActivityStream Server for Unimelb COMP90015\n\n";
		String footer = "\ncontact aharwood@unimelb.edu.au for issues.";
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
		options.addOption("rp", true, "remote port number");
		options.addOption("rh", true, "remote hostname");
		options.addOption("lh", true, "local hostname");
		options.addOption("a", true, "activity interval in milliseconds");
		options.addOption("s", true, "secret for the server to use");
		options.addOption("lg", true, "Lag to Simulate");

		// build the parser
		CommandLineParser parser = new DefaultParser();

		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e1) {
			help(options);
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

		if (cmd.hasOption("rh")) {
			Settings.setRemoteHostname(cmd.getOptionValue("rh"));
		}

		if (cmd.hasOption("rp")) {
			try {
				int port = Integer.parseInt(cmd.getOptionValue("rp"));
				Settings.setRemotePort(port);
			} catch (NumberFormatException e) {
				log.error("-rp requires a port number, parsed: " + cmd.getOptionValue("rp"));
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

		if (cmd.hasOption("s")) {
			Settings.setSecret(cmd.getOptionValue("s"));
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
