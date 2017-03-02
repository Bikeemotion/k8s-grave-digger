package com.bikeemotion.gravedigger.impl;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Worker implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(Worker.class);
  private final KubernetesClient client = new DefaultKubernetesClient();
  private String podLabelKey;
  private String podLabelValue;
  private PodComm comm;
  private PodStateSet last;

  public Worker(
      final String podLabelKey,
      final String podLabelValue,
      final PodComm comm) {

    this.podLabelKey = podLabelKey;
    this.podLabelValue = podLabelValue;
    this.comm = comm;
    this.last = new PodStateSet();
  }

  @Override
  public void run() {

    try {

      log.debug("Searching for targets...");

      if (last != null && !last.isEmpty()) {
        log.debug("Last Pod States: {}", last.toString());
      }

      //    Set<Pod> currentPods = KubernetesDao.queryPods("name", podLabelValue);
      // ### get all RC pods/endpoints IPs
      List<Pod> currentPods = client
          .pods()
          .withLabel(podLabelKey, podLabelValue)
          .list()
          .getItems();
      currentPods = currentPods == null ? new ArrayList<>() : currentPods;
      log.debug("Current known Pods: {}", currentPods.toString());

      // ### query each one of those pods and compute its current health status
      Set<PodState> fresh = new HashSet<>();
      currentPods
          .stream()
          .filter(p -> p.getStatus().getPodIP() != null && !p.getStatus().getPodIP().isEmpty())
          .forEach(pod -> {

            PodState f = comm.query(
                pod
                    .getStatus()
                    .getPodIP(),
                pod
                    .getStatus()
                    .getContainerStatuses()
                    .stream()
                    .sorted(Comparator.comparing(c -> c.getName()))
                    .findFirst()
                    .get()
                    .getContainerID());

            if (f != null) {
              fresh.add(f);
            }
          });
      if (fresh != null && !fresh.isEmpty()) {
        log.debug("Current Pod States: {}", fresh.toString());
      }

      // ### discard missing pods
      Set<PodState> missing = last.cleanAbsent(fresh);
      if (missing != null && !missing.isEmpty()) {
        log.info("Missing Pods: {}", missing.toString());
      }

      // ### save healthy pods
      Set<PodState> healthy = last.getHealthy(fresh);
      Set<PodState> saved = new HashSet<>();
      if (healthy != null) {

        log.info("Healthy found: {}", healthy.toString());
        healthy.forEach(r -> {
          if (comm.save(r)) {
            saved.add(r);
            log.info("Pod [{}] saved with success", r.getHost());
          } else {
            log.warn("Unable to save Pod [{}]!", r.getHost());
          }
        });
      }

      // ### kill sick pods
      PodState victim = last.getVictim(fresh);
      PodState killed = null;
      if (victim != null) {

        log.info("Victim found: {}", victim.toString());
        if (comm.poison(victim)) {
          killed = victim;
          log.info("Pod [{}] poisoned with success", victim.getHost());
        } else {
          log.warn("Pod [{}] poisoning did NOT went as planned!", victim.getHost());
        }
      }

      // ### update
      /* 
      TODO: 28-11-2016  if a pod continues to report it is still unhealthy after a certain period, something 
      should be done about it...
      */
      Set<PodState> upserted = last.upsert(saved, killed);
      if (upserted != null && !upserted.isEmpty()) {
        log.info("Upserted Pods States: {}", upserted.toString());
      }

    } catch (Throwable e) {
      log.error("Unexpected error occurred", e);
    }
  }
}
