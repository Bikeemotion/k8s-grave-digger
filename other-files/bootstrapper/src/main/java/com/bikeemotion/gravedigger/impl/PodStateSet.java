package com.bikeemotion.gravedigger.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class PodStateSet extends HashSet<PodState> {

  // public API
  public PodStateSet() {}

  public PodStateSet(Collection<? extends PodState> c) {
    super(c);
  }

  /**
   * Pod state entries should be cleansed for pods that are not present anymore<br/>
   *
   * @return pods that were removed from the current known set
   */
  public Set<PodState> cleanAbsent(Set<PodState> freshPodStates) {

    Set<PodState> missingPods = new HashSet<>();

    forEach(l -> {

      if (!freshPodStates.stream().anyMatch(f -> f.equals(l))) {
        missingPods.add(l);
      }
    });

    removeAll(missingPods);

    return missingPods;
  }

  /**
   * The pod to kill should be the first reported as NOK and not yet poisoned<br/>
   */
  public PodState getVictim(Set<PodState> freshPodStates) {

    Set<PodState> victims = new HashSet<>();

    // need pods that are NOK and weren't before
    victims.addAll(
        freshPodStates
            .stream()
            .filter(n -> !n.isOk())
            .filter(n -> !stream().anyMatch(l -> l.equals(n) && !l.isOk()))
            .collect(Collectors.toSet())
        );

    // only interested in the oldest one of ALL the potential victims
    PodState candidate = victims
        .stream()
        .sorted((e1, e2) -> e1.getTimeStamp().compareTo(e2.getTimeStamp()))
        .findFirst()
        .orElse(null);

    // don't want to kill the last pod alive  
    boolean isLastManStanding = candidate != null
        && stream()
            .filter(l -> l.isOk())
            .count() == 1
        && stream().anyMatch(l -> l.equals(candidate));

    return isLastManStanding
        ? null
        : candidate;
  }

  /**
   * All the pods that just become OK should be retrieved
   */
  public Set<PodState> getHealthy(Set<PodState> freshPodStates) {

    Set<PodState> refurbished = new HashSet<>();

    // need the pods that just turned OK
    refurbished.addAll(
        freshPodStates
            .stream()
            .filter(n -> n.isOk())
            .filter(n -> stream().anyMatch(l -> l.equals(n) && !l.isOk()))
            .collect(Collectors.toSet())
        );

    // also need all pods that just came up and were not know yet
    refurbished.addAll(
        freshPodStates
            .stream()
            .filter(n -> n.isOk())
            .filter(n -> !stream().anyMatch(l -> l.equals(n)))
            .collect(Collectors.toSet())
        );

    return refurbished.isEmpty() ? null : refurbished;
  }

  /**
   * Updates current known pods with OK and the poisoned vitim<br/>
   * A NOK pod or victim will not be upserted if the poisoning was successful<br/>
   */
  public Set<PodState> upsert(Set<PodState> saved, PodState killed) {

    Set<PodState> upserted = new HashSet<>();

    // healthy pod states should all be updated
    if (saved != null) {

      Set<PodState> healthyPodStates = saved
          .stream()
          .filter(n -> n.isOk())
          .collect(Collectors.toSet());

      // need to remove old entries because a Set does not update existing elements
      removeAll(healthyPodStates);
      addAll(healthyPodStates);

      upserted.addAll(healthyPodStates);
    }

    // killed pod should also be updated
    if (killed != null) {

      removeIf(l -> l.equals(killed));
      add(killed);
      upserted.add(killed);
    }

    return upserted;
  }

  @Override
  public String toString() {

    String result = "[%s]";
    String value = "";

    if (!this.isEmpty()) {

      value = this
          .stream()
          .sorted((e1, e2) -> e1.getTimeStamp().compareTo(e2.getTimeStamp()))
          .map(e -> e.toString())
          .collect(Collectors.joining(","));
    }

    return String.format(result, value);
  }
}
