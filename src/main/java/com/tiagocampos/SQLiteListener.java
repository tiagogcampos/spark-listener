package com.tiagocampos;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.spark.launcher.SparkAppHandle;

public class SQLiteListener extends JDBCListener {
  public SQLiteListener(Connection conn) {
    super(conn);
  }

  public SQLiteListener(String url, Properties props) throws SQLException {
    super(url, props);
  }

  @Override
  public void stateChanged(SparkAppHandle handle) {
    synchronized (handle) {
      System.out.println("State changed to: " + handle.getState());
      if (handle.getState().isFinal()) {
        try {
          PreparedStatement statement =
              this.connection.prepareStatement(
                  "REPLACE INTO applications(id, state) values (?, ?)");

          statement.setString(1, handle.getAppId());
          statement.setString(2, handle.getState().toString());
          this.executeQuery(statement);
          this.connection.commit();
        } catch (SQLException e) {
          System.err.println(e.getLocalizedMessage());
        }
        handle.notifyAll();
      }
    }
  }

  @Override
  public void infoChanged(SparkAppHandle handle) {
    System.out.println("Something changed...");
  }

  @Override
  public void executeQuery(PreparedStatement statement) throws SQLException {
    statement.execute();
  }

  @Override
  public void initializeDatabase() throws SQLException {
    String createSQL = "CREATE TABLE IF NOT EXISTS applications(id TEXT PRIMARY KEY, state TEXT)";
    String indexSQL = "CREATE INDEX IF NOT EXISTS id_state_idx ON applications(id,state)";
    this.initializeDatabase(createSQL, indexSQL);
  }
}
