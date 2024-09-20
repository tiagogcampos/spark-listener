package com.tiagocampos;

import java.io.IOException;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

public class Launcher {
  public static synchronized void main(String[] args) throws IOException, InterruptedException {
    System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%6$s%n");
    SparkLauncher launcher =
        new SparkLauncher()
            .setMaster("local[4]")
            .setAppName("ListenerStatusTest")
            .setConf("spark.eventLog.enabled", "true")
            .setConf("spark.eventLog.dir", "~/Software/spark-3.3.2-bin-hadoop3/jobs/")
            .setAppResource(args[0]);

    SparkAppHandle handle =
        launcher.startApplication(
            new SparkAppHandle.Listener() {
              @Override
              public void stateChanged(SparkAppHandle handle) {
                synchronized (handle) {
                  if (handle.getState().isFinal()) {
                    handle.notifyAll();
                  }
                }
              }

              @Override
              public void infoChanged(SparkAppHandle handle) {
                System.out.println(handle);
              }
            });

    synchronized (handle) {
      // if the job fails, it goes into FINISHED state first and then FAILED
      // to make sure we wait for the failing condition, we are waiting twice
      // if it does not fail, the second .wait() call will timeout after
      // 1 second
      handle.wait();
      handle.wait(1000);
    }

    if (handle.getError().isPresent()) {
      System.err.println(handle.getError().get().getLocalizedMessage());
    }
  }
}
