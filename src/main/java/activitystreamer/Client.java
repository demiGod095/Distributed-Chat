package activitystreamer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import activitystreamer.client.ClientSkeleton;
import activitystreamer.util.Settings;

/** A Class representing the entry point for our client.
 *  This code was adapted from Aaron Harwoods code
 *  For the activity Streamer for COMP90015 last year.
 *
 */
public class Client {

	private static final Logger log = LogManager.getLogger();

	private static void help(Options options) {
		String header = "An Chat Client for Demonstrating GHS algorithm\n\n";
		String footer = "\ncontact phudgell@student.unimelb.edu.au for issues.";
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("ActivityStreamer.Client", header, options, footer, true);
		System.exit(-1);
	}

	public static void main(String[] args) {

		log.info("reading command line options");

		Options options = new Options();
		options.addOption("u", true, "username");
		options.addOption("rp", true, "remote port number");
		options.addOption("rh", true, "remote hostname");

		// build the parser
		CommandLineParser parser = new DefaultParser();

		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e1) {
			help(options);
		}

		if (cmd.hasOption("rh")) {
			Settings.setRemoteHostname(cmd.getOptionValue("rh"));
		}
		if (cmd.hasOption("u")) {
			Settings.setUsername(cmd.getOptionValue("u"));
		} else {
			log.error("No Username Supplied. Exiting");
			System.exit(-1);
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
		if (cmd.hasOption("u")) {
			Settings.setUsername(cmd.getOptionValue("u"));
		}

		log.info("starting client");

		ClientSkeleton c = ClientSkeleton.getInstance();

		boolean term = false;
		
		while (!term) {
			try {
				Thread.sleep(1000);
				term = c.updateIncomingData();
			} catch (InterruptedException ex) {
				break;
			}
		}

		Thread.currentThread().interrupt();
		System.exit(-1);
	}

}
