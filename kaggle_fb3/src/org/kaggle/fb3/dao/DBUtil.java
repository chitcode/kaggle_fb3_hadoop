package org.kaggle.fb3.dao;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class DBUtil {

	private static final String USER_NAME = "kaggle_fb3";
	private static final String PASSWORD = "kaggle_fb3";

	public static String getPredictedTags(int lineId, String title)
			throws Exception {

		Connection connect = null;
		CallableStatement cs = null;

		try {
			// This will load the MySQL driver, each DB has its own driver
			Class.forName("com.mysql.jdbc.Driver");
			// Setup the connection with the DB
			connect = DriverManager
					.getConnection("jdbc:mysql://172.25.32.3/kaggle_fb3?"
							+ "user=" + USER_NAME + "&password=" + PASSWORD);

			// PreparedStatements can use variables and are more efficient
			cs = connect.prepareCall("{? = call func_tag_score(?)}");
			cs.registerOutParameter(1, Types.VARCHAR);
			cs.setString(2, title);

			cs.execute();
			String tags = cs.getString(1);
			updateDataBase(connect, lineId, title, tags);
			return tags;

		} catch (Exception e) {
			throw e;
		} finally {
			try {
				if (connect != null) {
					connect.close();
				}
			} catch (Exception e) {

			}
		}
	}

	private static void updateDataBase(Connection connect, int lineId,
			String title, String tags) throws SQLException {

		String insertSQL = "INSERT INTO PREDICTION_TAGS VALUES (?,?,?)";
		PreparedStatement stmt = connect.prepareStatement(insertSQL);

		stmt.setInt(1, lineId);
		stmt.setString(2, title);
		stmt.setString(3, tags);

		stmt.executeUpdate();

	}

	public static void main(String[] args) {

		String titleString = "loop nsdictionary obtained from json";
		try {
			System.out.println(DBUtil.getPredictedTags(100, titleString));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void logInDB(int lineId, String tags) throws Exception {

		Connection connect = null;		
		PreparedStatement stmt = null;
		try {
			// This will load the MySQL driver, each DB has its own driver
			Class.forName("com.mysql.jdbc.Driver");
			// Setup the connection with the DB
			connect = DriverManager
					.getConnection("jdbc:mysql://172.25.32.3/kaggle_fb3?"
							+ "user=" + USER_NAME + "&password=" + PASSWORD);

			String insertSQL = "INSERT INTO PREDICTION_TAGS_MR VALUES (?,?)";
			stmt = connect.prepareStatement(insertSQL);

			stmt.setInt(1, lineId);
			stmt.setString(2, tags);

			stmt.executeUpdate();
		} catch (Exception e) {
			throw e;
		} finally {
			try {
				if (stmt != null) {
					stmt.close();
				}
				if (connect != null) {
					connect.close();
				}
			} catch (Exception e) {

			}
		}

	}
}