package com.hhstechgroup.dyp.fileupload.custome;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;

import org.apache.commons.lang.StringUtils;

import com.opencsv.CSVReader;

public class CSVLoader {

	private static final 
		String SQL_INSERT = "INSERT INTO ${table}(${keys}) VALUES(${values})";
	private static final String TABLE_REGEX = "\\$\\{table\\}";
	private static final String KEYS_REGEX = "\\$\\{keys\\}";
	private static final String VALUES_REGEX = "\\$\\{values\\}";

	private Connection connection;
	private char seprator;

	/**
	 * Public constructor to build CSVLoader object with
	 * Connection details. The connection is closed on success
	 * or failure.
	 * @param connection
	 */
	public CSVLoader(Connection connection) {
		this.connection = connection;
		//Set default separator
		this.seprator = ',';
	}
	
	/**
	 * Parse CSV file using OpenCSV library and load in 
	 * given database table. 
	 * @param csvFile Input CSV file
	 * @param tableName Database table name to import data
	 * @param truncateBeforeLoad Truncate the table before inserting 
	 * 			new records.
	 * @throws Exception
	 */
	public void loadCSV(String csvFile, String tableName,
			boolean truncateBeforeLoad) throws Exception {

		CSVReader[] csvReaders;
		//CSVReader csvReader;
		if(null == this.connection) {
			throw new Exception("Not a valid connection.");

		}
		
		File folder = new File("//home/cuelogic.local/ganesh.jadhav/eclipse-workspace/SpringBatch/SpringBatch/SpringBatch-CSV-Database-Example/SpringBatchExample/src/main/resources/cvs/error/");
		File[] listOfFiles = folder.listFiles();
		
		try {
			csvReaders = new CSVReader[listOfFiles.length];
			for(int i=0 ;i<listOfFiles.length;i++) {
				csvReaders[i] = new CSVReader(new FileReader(listOfFiles[i]), this.seprator);
			}	
			//CSVReader csvReader = new CSVReader(new FileReader("//home/cuelogic.local/ganesh.jadhav/eclipse-workspace/VYP-WorkSpace/SpringBatch/SpringBatch-CSV-Database-Example/SpringBatchExample/src/main/resources/cvs/single/npidata_pfile_20050523-20200308.csv"), this.seprator);
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("Error occured while executing file. "
					+ e.getMessage());
		}
		for(int i=0 ;i<listOfFiles.length;i++) 
		{
			System.out.println("Batach Part  "+i +" Started, Part Name "+listOfFiles[i].getName());
			
			CSVReader csvReader = new CSVReader(new FileReader(listOfFiles[i]), this.seprator);
			String[] headerRow = csvReader.readNext();
	
			if (null == headerRow) {
				throw new FileNotFoundException("No columns defined in given CSV file.Please check the CSV file format.");
			}
	
			String questionmarks = StringUtils.repeat("?,", 330);
			questionmarks = (String) questionmarks.subSequence(0, questionmarks
					.length() - 1);
	
			String query = SQL_INSERT.replaceFirst(TABLE_REGEX, tableName);
			query = query
					.replaceFirst(KEYS_REGEX, Constant.columnname);
			query = query.replaceFirst(VALUES_REGEX, questionmarks);
	
			//System.out.println("Query: " + query);
	
			String[] nextLine;
			Connection con = null;
			PreparedStatement ps = null;
			try {
				con = CoreApplication.getCon();
				con.setAutoCommit(false);
				ps = con.prepareStatement(query);
	
				if(truncateBeforeLoad) {
					//delete data from table before loading csv
					//con.createStatement().execute("DELETE FROM " + tableName);
				}
	
				final int batchSize = 1000;
				long count = 0;
				while ((nextLine = csvReader.readNext()) != null) {
					if(count==99055) {
						System.out.println("Hellow");
					}
					 if (null != nextLine) {
						int index = 1;
						if(nextLine.length==330 || nextLine.length<330) {
							for (String string :nextLine) {
								ps.setString(index++, string);
							}
							for(int k=nextLine.length;k<330;k++) {
								ps.setString(index++, "");
							}
						}
						else if(nextLine.length>330) {
							for (int k=0;k<330;k++ ) {
								ps.setString(index++, nextLine[k]);
							}
						}
						ps.addBatch();
					}
					if (++count % batchSize == 0) {
						ps.executeBatch();
						System.out.println("batch "+count);
						con.commit();
					}
				}
				ps.executeBatch(); // insert remaining records
				System.out.println("batch "+count);
				con.commit();
				
			} catch (Exception e) {
				System.out.println("File Name -> "+listOfFiles[i].getName());
				System.out.println("Batach Part  "+i +" Failed, Part Name "+listOfFiles[i].getName());
				//con.rollback();
				e.printStackTrace();
				//throw new Exception("Error occured while loading data from file to database."+ e.getMessage());
				
				
			} finally {
				if (null != ps)
					ps.close();
				if (null != con)
					con.close();
	
				csvReader.close();
			}
			System.out.println("Batach Part  "+i +" Completed, Part Name "+listOfFiles[i].getName());
		}
	}

	public char getSeprator() {
		return seprator;
	}

	public void setSeprator(char seprator) {
		this.seprator = seprator;
	}

}