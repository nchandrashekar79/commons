package com.example.commons;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class GenricCsv2DbConverter {

	private static Map<String, String> columnNames2datatype = new LinkedHashMap<>();

	private static String tblName = null;

	private static String path = "C:/resources/";
	
	private static String csvFile = "matches";

	public static void main(String[] args) throws ClassNotFoundException {
		int batchSize = 20;

		Connection connection = getConnection();
		String csvFilePath = path + csvFile + ".csv";

		try {

			createDateIntoTable(connection, csvFilePath);
			insertDateIntoStateTable(connection, csvFilePath, batchSize);

		} catch (SQLException ex) {
			ex.printStackTrace();

			try {
				connection.rollback();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

	}

	private static Connection getConnection() throws ClassNotFoundException {
		String jdbcURL = "jdbc:sqlserver://localhost:1433;databaseName=demo;selectMethod=cursor";
		String driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		String username = "sa";
		String password = "sa";

		Connection connection = null;

		try {

			Class.forName(driverClass);
			connection = DriverManager.getConnection(jdbcURL, username, password);
			connection.setAutoCommit(false);

		} catch (SQLException ex) {
			ex.printStackTrace();

			try {
				connection.rollback();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		System.out.println("Connection: " + connection);
		return connection;

	}

	private static void insertDateIntoStateTable(Connection connection, String csvFilePath, int batchSize)
			throws SQLException {

		int id = 1;

		try {
			String sql = "INSERT INTO " + tblName + " (" + getKeys() + ") VALUES (" + getRepeatString() + ")";

			System.out.println("Keys : " + getKeys());
			System.out.println("Insert sql: " + sql);

			System.out.println("Parameters size: " + columnNames2datatype.size());

			PreparedStatement statement = connection.prepareStatement(sql);

			BufferedReader lineReader = new BufferedReader(new FileReader(csvFilePath));
			String lineText = null;

			int count = 0;

			lineReader.readLine(); // skip header line

			while ((lineText = lineReader.readLine()) != null) {
				String[] data = lineText.split(",");

				int sizeOfData = data.length;

				/*
				 * if(sizeOfData != owidCovidDataTablecolumns.size()){
				 * System.out.println("Invalid Date in csv"); break; }
				 */

				for (int i = 0; i < sizeOfData; i++) {

					if (i == sizeOfData) {
						break;
					}
					String value = data[i];
					if (value.length() != 0 && value.length() >= 50) {
						value = value.substring(0, 49);
					}

					try {
						// System.out.println("setString " + i + ", data:" +
						// value);
						statement.setString(i + 1, value);
					} catch (Exception e) {
						System.out.println("Exception while set data id: " + i + ", data:" + value);
					}

				}

				statement.addBatch();

				id++;

				// System.out.println("statement : " + id + " = " +
				// statement.toString());

				if (count % batchSize == 0) {
					statement.executeBatch();
				}
			}

			lineReader.close();

			// execute the remaining queries
			statement.executeBatch();

			connection.commit();
			connection.close();

		} catch (IOException ex) {
			System.err.println(ex);
		}

		System.out.println(id + " Rows inserted table ");

	}

	private static String getRepeatString() {
		return String.join(",", Collections.nCopies(columnNames2datatype.size(), "?"));
	}

	private static String getKeys() {

		return columnNames2datatype.keySet().stream().map(Object::toString).collect(Collectors.joining(","));
	}

	private static void createDateIntoTable(Connection connection, String csvFilePath) throws SQLException {

		try {

			BufferedReader lineReader = new BufferedReader(new FileReader(csvFilePath));
			String lineText = null;

			String fileName = new File(csvFilePath).getName();
			String tableName = fileName.substring(0, fileName.indexOf("."));
			tableName = tableName.replaceAll("[^a-zA-Z0-9]", "");

			tblName = tableName;

			while ((lineText = lineReader.readLine()) != null) {
				String[] data = lineText.split(",");

				for (int i = 0; i < data.length; i++) {

					String coloumName = data[i];

					coloumName = coloumName.replaceAll("\\s", "");

					columnNames2datatype.put(coloumName, "VARCHAR(50)");
				}

				break;

			}

			String sql = createQuery();
			Statement stmt = connection.createStatement();
			stmt.execute(sql);
			connection.commit();

			System.out.println(tableName + " Table created  ");

		} catch (IOException ex) {
			System.err.println(ex);
		}

	}

	private static String createQuery() {

		String dropTable = "DROP TABLE  IF EXISTS " + tblName;
		String query = dropTable + " CREATE TABLE " + tblName + "(";

		for (String key : columnNames2datatype.keySet()) {
			query = query + key + " " + columnNames2datatype.get(key) + ",";
		}

		query = query.substring(0, query.length() - 1);
		query = query + ");";

		System.out.println(query);

		return query;

	}
}
