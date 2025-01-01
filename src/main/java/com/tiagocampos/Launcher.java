package com.tiagocampos;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

public class Launcher {
  public static synchronized void main(String[] args)
      throws IOException, InterruptedException, SQLException {
    System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%6$s%n");

    SparkLauncher launcher =
        new SparkLauncher()
            .setMaster("local[4]")
            .setAppName("ListenerStatusTest")
            .setConf("spark.eventLog.enabled", "true")
            .setConf(
                "spark.eventLog.dir", "/Users/tiagocampos/Software/spark-3.3.2-bin-hadoop3/jobs/")
            .setAppResource(args[0]);

    Connection conn = DriverManager.getConnection("jdbc:sqlite:db.sqlite");

    SQLiteListener listener = new SQLiteListener(conn);
    listener.initializeDatabase();

    SparkAppHandle handle = launcher.startApplication(listener);

    synchronized (handle) {
      // if the job fails, it goes into FINISHED state first and then FAILED.
      // To make sure we wait for the failing condition, we are waiting twice.
      // If it does not fail, the second .wait() call will timeout after
      // 1 second
      handle.wait();
      handle.wait(1000);
    }

    if (handle.getState().isFinal()) {
      System.out.println(handle.getState());
    }

    if (handle.getError().isPresent()) {
      System.err.println(handle.getError().get().getLocalizedMessage());
    }
  }
}
