package com.ef;

import com.ef.parser.ArgumentUtil;
import com.ef.parser.DefaultProcessor;
import com.ef.parser.DurationArg;
import com.ef.parser.Processor;
import com.ef.parser.ProgramOption;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class Parser {

	private static final String CHECK_OPTION_MSG = "The %s option is required.";
	private Options options;

	public Parser() {
		options = init();
	}

	/**
	 * Load program options
	 * 
	 * @return
	 */
	protected final Options init() {
		Option help = Option.builder("h").longOpt(ProgramOption.HELP.getOption()).required(false)
				.desc(ProgramOption.HELP.getDescription()).build();

		Option accessLog = Option.builder().longOpt(ProgramOption.ACCESS_LOG.getOption()).hasArg().required(false)
				.desc(ProgramOption.ACCESS_LOG.getDescription()).build();

		Option startDate = Option.builder().longOpt(ProgramOption.START_DATE.getOption()).hasArg().required(false)
				.desc(ProgramOption.START_DATE.getDescription()).build();

		Option duration = Option.builder().longOpt(ProgramOption.DURATION.getOption()).hasArg().required(false)
				.desc(ProgramOption.DURATION.getDescription()).build();

		Option threshold = Option.builder().longOpt(ProgramOption.THRESHOLD.getOption()).hasArg().required(false)
				.desc(ProgramOption.THRESHOLD.getDescription()).build();

		options = new Options();
		options.addOption(help);
		options.addOption(accessLog);
		options.addOption(startDate);
		options.addOption(duration);
		options.addOption(threshold);

		return options;
	}

	protected Options getOptions() {
		return this.options;
	}

	public void start(String[] args) throws ParseException {
		CommandLineParser parser = new DefaultParser();
		CommandLine commandLine = parser.parse(options, args);

		if (commandLine.hasOption(ProgramOption.HELP.getOption()) || commandLine.getOptions().length == 0) {
			help();
			return;
		}

		try {
			String accessLogArg = checkAccessFileOption(commandLine);
			String startDateArg = checkStartDateOption(commandLine);
			String durationArg = checkDurationOption(commandLine);
			String thresholdArg = checkThresholdOption(commandLine);

			File file;
			if (accessLogArg == null) {

				file = null;
			} else {
				file = ArgumentUtil.parseAccessFile(accessLogArg);
			}
			LocalDateTime startDate = ArgumentUtil.parseStartDate(startDateArg);
			DurationArg duration = ArgumentUtil.parseDuration(durationArg);
			LocalDateTime finalDate = ArgumentUtil.getFinalDate(startDate, duration);
			int threshold = ArgumentUtil.parseThreshold(thresholdArg);

			try {
				Processor processor = new DefaultProcessor();
				Map<String, Integer> ips = processor.findIps(file, startDate, finalDate, threshold);

				print(ips, threshold, startDate, finalDate);
			} catch (IOException e) {
				System.out.println(e.getMessage());
			}
		} catch (IllegalArgumentException e) {

		}
	}

	public void help() {
		HelpFormatter helpFormatter = new HelpFormatter();
		helpFormatter.printHelp("sb server", options);
	}

	public static String checkAccessFileOption(CommandLine commandLine) {
		if (commandLine.hasOption(ProgramOption.ACCESS_LOG.getOption())) {
			return commandLine.getOptionValue(ProgramOption.ACCESS_LOG.getOption());
		} else {
			return null;
			// throw new IllegalArgumentException(String.format(CHECK_OPTION_MSG,
			// ProgramOption.ACCESS_LOG.getOption()));
		}
	}

	public static String checkStartDateOption(CommandLine commandLine) {
		if (commandLine.hasOption(ProgramOption.START_DATE.getOption())) {
			return commandLine.getOptionValue(ProgramOption.START_DATE.getOption());
		} else {
			throw new IllegalArgumentException(String.format(CHECK_OPTION_MSG, ProgramOption.START_DATE.getOption()));
		}
	}

	public static String checkDurationOption(CommandLine commandLine) {
		if (commandLine.hasOption(ProgramOption.DURATION.getOption())) {
			return commandLine.getOptionValue(ProgramOption.DURATION.getOption());
		} else {
			throw new IllegalArgumentException(String.format(CHECK_OPTION_MSG, ProgramOption.DURATION.getOption()));
		}
	}

	public static String checkThresholdOption(CommandLine commandLine) {
		if (commandLine.hasOption(ProgramOption.THRESHOLD.getOption())) {
			return commandLine.getOptionValue(ProgramOption.THRESHOLD.getOption());
		} else {
			throw new IllegalArgumentException(String.format(CHECK_OPTION_MSG, ProgramOption.THRESHOLD.getOption()));
		}
	}

	/**
	 * Print IPs on console
	 * 
	 * @param ips
	 * @param threshold
	 * @param startDate
	 * @param finalDate
	 */
	public static void print(Map<String, Integer> ips, int threshold, LocalDateTime startDate,
			LocalDateTime finalDate) {
		String headerMsg = String.format("IPs that made more than %d requests from %s to %s", threshold, startDate,
				finalDate);
		System.out.println(headerMsg);
		for (String key : ips.keySet()) {
			System.out.println("IP: " + key + "\tTotal Requests: " + ips.get(key));
			
			// Store IP into another table in Database
			// DB Connection
			try {
				Class.forName("com.mysql.jdbc.Driver");
				Connection con = DriverManager.getConnection(
						"jdbc:mysql://localhost:3306/logdb?autoReconnect=true&useSSL=false", "root", "password");
				// The mysql insert statement
				String query = " insert into iptable (ip,comment)" + " values (?, ?)";

				// Create the mysql insert preparedstatement
				PreparedStatement preparedStmt = con.prepareStatement(query);
				preparedStmt.setString(1, key);
				preparedStmt.setString(2, "Comment Total Request=" + ips.get(key));
				// Execute the preparedstatement
				preparedStmt.execute();

			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block

			} catch (SQLException e) {
				// TODO Auto-generated catch block

			}

		}

	}

	/**
	 * 
	 *
	 * @param args
	 * @throws org.apache.commons.cli.ParseException
	 */
	public static void main(String[] args) throws ParseException {
		Parser parser = new Parser();
		parser.start(args);
	}
}
