package com.hhstechgroup.dyp.fileupload.custome;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class CoreApplication  {

	private static String JDBC_CONNECTION_URL = 
			"jdbc:postgresql://localhost:5432/VYP";
	
	
	public static void main(String[] args) {
		try {
			System.out.println("Batach start ");
			CSVLoader loader = new CSVLoader(getCon());
			
			loader.loadCSV("\npidata_100000.csv", "npps_report", true);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public  static Connection getCon() {
		Connection connection = null;
		try {
			Class.forName("org.postgresql.Driver");
			connection = DriverManager.getConnection(JDBC_CONNECTION_URL,"postgres", "postgres");

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return connection;
	}
}
