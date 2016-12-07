package com.bikeemotion.gravedigger.impl;

import com.bikeemotion.kubernetes.KubernetesDao;
import com.bikeemotion.kubernetes.Pod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class Worker implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(Worker.class);
  private PodComm comm;
  private String podName;
  private PodStateSet last;

  public Worker(
      final String podName,
      final PodComm comm) {

    this.podName = podName;
    this.comm = comm;
    this.last = new PodStateSet();
  }

  @Override
  public void run() {

    log.info("\r\n");
    log.info("Searching for targets...");

    if (last != null && !last.isEmpty()) {
      log.info("Last Pod States: {}", last.toString());
    }

    // ### get all RC pods/endpoints IPs
    Set<Pod> currentPods = KubernetesDao.queryPods("name", podName);
    log.debug("Current known Pods: {}", currentPods.toString());

    // ### query each one of those pods and compute its current health status
    Set<PodState> fresh = new HashSet<>();
    currentPods
        .stream()
        .filter(p -> p.getIP() != null && !p.getIP().isEmpty())
        .forEach(p -> {

          PodState f = comm.query(p);
          if (f != null) {
            fresh.add(f);
          }
        });
    if (fresh != null && !fresh.isEmpty()) {
      log.info("Current Pod States: {}", fresh.toString());
    }

    // ### discard missing pods
    Set<PodState> missing = last.cleanAbsent(fresh);
    if (missing != null && !missing.isEmpty()) {
      log.debug("Missing Pods: {}", missing.toString());
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
    // TODO: 28-11-2016  if a pod continues to report it is still unhealthy after a certain periodMillis of time should do something about it...
    last.upsert(saved, killed);
    if (last != null && !last.isEmpty()) {
      log.debug("Upserted Pods States: {}", last.toString());
    }

  }
}
