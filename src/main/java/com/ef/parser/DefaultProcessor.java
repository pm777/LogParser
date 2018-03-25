package com.ef.parser;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

public class DefaultProcessor implements Processor {

	private Map<String, Integer> ips;
	private static final String DATE_FORMAT_FILE = "yyyy-MM-dd HH:mm:ss.SSS";

	@Override
	public Map<String, Integer> findIps(File file, LocalDateTime startDate, LocalDateTime finalDate, int threshold)
			throws IOException {
		ips = new HashMap<>();

		if (file == null) {

			// Fetch data from the database
			// Handle if no data is found in the table
			// DB Connection
			try {
				Class.forName("com.mysql.jdbc.Driver");

				Connection con = DriverManager.getConnection(
						"jdbc:mysql://localhost:3306/logdb?autoReconnect=true&useSSL=false", "root", "password");

				Statement stmt = con.createStatement();
				ResultSet rs = stmt.executeQuery("select * from logdata");
				while (rs.next()) {

					String dateTxt = rs.getString(2);
					String ip = rs.getString(3);
					String request = rs.getString(4);
					String statusCode = rs.getInt(5) + "";
					String userAgent = rs.getString(6);

					LocalDateTime dateOfTheLine = LocalDateTime.parse(dateTxt,
							DateTimeFormatter.ofPattern(DATE_FORMAT_FILE));

					if (dateOfTheLine.isBefore(startDate)) {
						continue;
					}

					if (dateOfTheLine.isAfter(finalDate)) {
						break;
					}

					if (ips.get(ip) != null) {
						Integer count = ips.get(ip);
						ips.put(ip, ++count);
					} else {
						ips.put(ip, 1);
					}

				}
				con.close();

			} catch (Exception e1) {
				// TODO Auto-generated catch block

			}
		} else {
			LineIterator lineIterator = FileUtils.lineIterator(file);

			while (lineIterator.hasNext()) {
				String[] line = lineIterator.nextLine().split("\\|"); // The delimiter of the log file is pipe (|)

				String dateTxt = line[0];
				String ip = line[1];
				String request = line[2];
				String statusCode = line[3];
				String userAgent = line[4];

				LocalDateTime dateOfTheLine = LocalDateTime.parse(dateTxt,
						DateTimeFormatter.ofPattern(DATE_FORMAT_FILE));

				// Store above data into database
				try {
					Class.forName("com.mysql.jdbc.Driver");
					Connection con = DriverManager.getConnection(
							"jdbc:mysql://localhost:3306/logdb?autoReconnect=true&useSSL=false", "root", "password");

					// The mysql insert statement
					String query = " insert into logdata (timestamp,ip,request,status,user_agent)"
							+ " values (?, ?, ?, ?, ?)";

					// Create the mysql insert preparedstatement
					PreparedStatement preparedStmt = con.prepareStatement(query);
					preparedStmt.setString(1, dateTxt);
					preparedStmt.setString(2, ip);
					preparedStmt.setString(3, request);
					preparedStmt.setInt(4, Integer.parseInt(statusCode));
					preparedStmt.setString(5, userAgent);

					// Execute the preparedstatement
					preparedStmt.execute();
					con.close();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block

				} catch (SQLException e) {
					// TODO Auto-generated catch block

				}

				if (dateOfTheLine.isBefore(startDate)) {
					continue;
				}

				if (dateOfTheLine.isAfter(finalDate)) {
					break;
				}

				if (ips.get(ip) != null) {
					Integer count = ips.get(ip);
					ips.put(ip, ++count);
				} else {
					ips.put(ip, 1);
				}
			}
		}

		return ips.entrySet().stream().filter(map -> map.getValue() >= threshold)
				.collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
	}

}
