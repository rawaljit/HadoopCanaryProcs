package com.hadoop.canary;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

/**
 * Simple JDBC check whether hiveServer2 is running or not.
 * @author Teradata India Pvt Ltd..
 *
 */

public class HiveServer2Check {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private static String host = "localhost";
	private static String port = "10000";
	private static String db = "default";
	/**
	 * @param args
	 * @throws SQLException
	 */
	public static void main(String[] args) {
		try {
			if(args.length == 3) {
				host = args[0];
				port = args[1];
				db = args[2];
			}
			Class.forName(driverName);
			Connection con = DriverManager.getConnection(
					"jdbc:hive2://"+ host +":"+ port +"/"+db, "hive", "");
			Statement stmt = con.createStatement();
			// regular hive query
			String sql = "show tables";
			System.out.println("Running: " + sql);
			ResultSet res = stmt.executeQuery(sql);
			while (res.next()) {
				System.out.println(res.getString(1));
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		} catch(Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		System.exit(0);
	}
}