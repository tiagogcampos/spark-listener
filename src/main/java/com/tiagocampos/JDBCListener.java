package com.tiagocampos;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.spark.launcher.SparkAppHandle;

abstract class JDBCListener implements SparkAppHandle.Listener {
  protected final Connection connection;

  protected JDBCListener(Connection connection) {
    this.connection = connection;
  }

  protected JDBCListener(String url, Properties props) throws SQLException {
    this.connection = DriverManager.getConnection(url, props);
  }

  protected void initializeDatabase(String... queries) throws SQLException {
    try {
      connection.setAutoCommit(false);
      for (String query : queries) {
        PreparedStatement statement = this.connection.prepareStatement(query);
        this.executeQuery(statement);
        this.connection.commit();
      }
    } catch (SQLException e) {
      System.err.println(e.getLocalizedMessage());
      connection.rollback();
      throw e;
    }
  }

  protected void executeQuery(PreparedStatement statement) throws SQLException {
    statement.execute();
  }

  public abstract void initializeDatabase() throws SQLException;
}
