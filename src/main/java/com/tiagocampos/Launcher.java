package com.tiagocampos;

import java.io.IOException;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

/** Hello world! */
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
                  System.out.println(
                      "Thread " + Thread.currentThread().getName() + " acquired lock...");
                  System.out.println(
                      "State changed to "
                          + handle.getState()
                          + " on thread "
                          + Thread.currentThread().getName());
                  if (handle.getState().isFinal()) {
                    handle.notifyAll();
                  }
                  System.out.println(
                      "Thread " + Thread.currentThread().getName() + " released lock...");
                }
              }

              @Override
              public void infoChanged(SparkAppHandle handle) {
                System.out.println(handle);
              }
            });

    synchronized (handle) {
      System.out.println("Thread: " + Thread.currentThread().getName() + " is waiting...");
      handle.wait();
      System.out.println("Thread: " + Thread.currentThread().getName() + " resumed.");
      System.out.println("Thread: " + Thread.currentThread().getName() + " is waiting again.");
      handle.wait(1000);
      System.out.println("Thread: " + Thread.currentThread().getName() + " resumed.");
    }

    if (handle.getError().isPresent()) {
      System.out.println(handle.getError().get().getLocalizedMessage());
    }
  }
}
