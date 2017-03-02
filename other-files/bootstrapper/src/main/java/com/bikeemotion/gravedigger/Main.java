package com.bikeemotion.gravedigger;

import com.bikeemotion.gravedigger.impl.PodComm;
import com.bikeemotion.gravedigger.impl.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {

  private static final Logger log = LoggerFactory.getLogger(Main.class);
  private static final String MONITORED_POD_LABEL_KEY = getEnvOrDefault("MONITORED_POD_LABEL_KEY", null);
  private static final String MONITORED_POD_LABEL_VALUE = getEnvOrDefault("MONITORED_POD_LABEL_VALUE", null);
  private static final String POD_HEALTH_CHECK_PATH = getEnvOrDefault("POD_HEALTH_CHECK_PATH", null);
  private static final String POD_HEALTH_UPDATE_PATH = getEnvOrDefault("POD_HEALTH_UPDATE_PATH", null);
  private static final String POD_HEALTH_PORT = getEnvOrDefault("POD_HEALTH_PORT", null);
  private static final String TIMEOUT_SECONDS = getEnvOrDefault("TIMEOUT_SECONDS", null);
  private static final String PERIOD_SECONDS = getEnvOrDefault("PERIOD_SECONDS", null);

  public static void main(String[] args) {

    assert MONITORED_POD_LABEL_KEY != null;
    assert MONITORED_POD_LABEL_VALUE != null;
    assert POD_HEALTH_CHECK_PATH != null;
    assert POD_HEALTH_UPDATE_PATH != null;
    assert POD_HEALTH_PORT != null;
    assert TIMEOUT_SECONDS != null;
    assert PERIOD_SECONDS != null;

    int port = 0;
    int timeoutMillis = 0;
    int periodMillis = 0;
    ScheduledExecutorService scheduledPool;
    try {

      log.info("Starting Kubernetes GraveDigger...");
      log.info("MONITORED_POD_LABEL_KEY [{}]", MONITORED_POD_LABEL_KEY);
      log.info("MONITORED_POD_LABEL_VALUE [{}]", MONITORED_POD_LABEL_VALUE);
      log.info("POD_HEALTH_CHECK_PATH [{}]", POD_HEALTH_CHECK_PATH);
      log.info("POD_HEALTH_UPDATE_PATH [{}]", POD_HEALTH_UPDATE_PATH);
      log.info("POD_HEALTH_PORT [{}]", POD_HEALTH_PORT);
      log.info("TIMEOUT_SECONDS [{}]", TIMEOUT_SECONDS);
      log.info("PERIOD_SECONDS [{}]", PERIOD_SECONDS);

      port = Integer.parseInt(POD_HEALTH_PORT);
      timeoutMillis = (int) TimeUnit.SECONDS.toMillis(Integer.parseInt(TIMEOUT_SECONDS));
      periodMillis = (int) TimeUnit.SECONDS.toMillis(Integer.parseInt(PERIOD_SECONDS));

    } catch (Exception e) {

      log.error("Unable to start Kubernetes GraveDigger", e);
    }

    scheduledPool = Executors.newScheduledThreadPool(1);
    scheduledPool.scheduleWithFixedDelay(
        new Worker(
            MONITORED_POD_LABEL_KEY,
            MONITORED_POD_LABEL_VALUE,
            new PodComm(
                POD_HEALTH_CHECK_PATH,
                POD_HEALTH_UPDATE_PATH,
                port,
                timeoutMillis)),
        1,
        periodMillis,
        TimeUnit.MILLISECONDS);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> scheduledPool.shutdown()));
  }

  public static String getEnvOrDefault(final String variableName, final String defaultValue) {

    final String value = System.getenv(variableName);
    return (value == null || value.isEmpty())
        ? defaultValue
        : value;
  }
}
