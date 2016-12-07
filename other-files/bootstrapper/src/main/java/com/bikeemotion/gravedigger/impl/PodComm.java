package com.bikeemotion.gravedigger.impl;

import com.bikeemotion.kubernetes.Pod;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.bikeemotion.gravedigger.impl.PodState.Status.HEALTHY;
import static org.joda.time.DateTime.now;
import static org.joda.time.DateTimeZone.UTC;

public class PodComm {

  // members
  private static final Logger log = LoggerFactory.getLogger(PodComm.class);
  private CloseableHttpClient httpClient;
  private HttpContext httpContext;
  private String healthCheckAddress;
  private String healthUpdateAddress;
  private Integer port;

  // public API
  public PodComm(
      final String healthCheckAddress,
      final String healthUpdateAddress,
      final Integer port,
      final Integer timeoutMillis) {

    this.httpClient = HttpClientBuilder
        .create()
        .setDefaultRequestConfig(RequestConfig
            .custom()
            .setConnectTimeout(timeoutMillis)
            .setConnectionRequestTimeout(timeoutMillis)
            .setSocketTimeout(timeoutMillis)
            .build())
        .build();
    this.httpContext = HttpClientContext.create();
    this.healthCheckAddress = healthCheckAddress;
    this.healthUpdateAddress = healthUpdateAddress;
    this.port = port;
  }

  PodState query(Pod currentPod) {

    HttpGet request = new HttpGet(String.format(
        "http://%s:%s/%s",
        currentPod.getIP(),
        port,
        healthCheckAddress));
    request.addHeader("accept", "application/json");

    boolean healthy = false;
    log.debug("Querying pod [{}]...", currentPod.getIP());
    try (CloseableHttpResponse response = httpClient.execute(request, httpContext)) {

      log.debug(
          "Pod [{}] answered with [{}]...",
          currentPod.getIP(),
          response);
      healthy = response.getStatusLine().getStatusCode() == 204;

    } catch (Exception e) {

      log.warn(String.format(
          "Unable to query pod [%s] or parse its response  - may not be fully started yet or just sick...",
          currentPod.getIP()));
    }

    return new PodState()
        .setTimeStamp(now(UTC).getMillis())
        .setHealth(healthy ? HEALTHY : PodState.Status.SICK)
        .setHost(currentPod.getIP())
        .setContainerID(currentPod.getContainerID());
  }

  boolean poison(PodState victim) {

    return update(victim, false);
  }

  boolean save(PodState refurbished) {

    return update(refurbished, true);
  }

  // internal API
  private boolean update(PodState pod, boolean isHealthy) {

    boolean result = false;
    CloseableHttpResponse response = null;
    try {

      HttpPost request = new HttpPost(String.format(
          "http://%s:%s/%s",
          pod.getHost(),
          port,
          healthUpdateAddress));
      request.addHeader("accept", "application/json");
      request.setEntity(new StringEntity(Boolean.toString(isHealthy)));

      log.debug("Updating pod [{}]...", pod.getHost());
      response = httpClient.execute(request, httpContext);
      log.debug(
          "Updated Pod [{}] answered with [{}]...",
          pod.getHost(),
          response);

      result = response.getStatusLine().getStatusCode() == 204;
    } catch (Exception e) {

      log.error(String.format(
          "Unable to update pod [%s] or parse its response - may not be fully started yet or just sick",
          pod.getHost()));
    } finally {

      if (response != null) {

        try {
          response.close();
        } catch (IOException e) {
          log.error("Unable to close http connection", e);
        }
      }
    }

    return result;

  }
}
