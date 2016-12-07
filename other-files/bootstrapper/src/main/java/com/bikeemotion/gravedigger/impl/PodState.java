package com.bikeemotion.gravedigger.impl;

public class PodState extends Object {

  enum Status {
    HEALTHY,
    SICK
  }

  // members
  private Long timeStamp;
  private String containerID;
  private String host;
  private Status health;
  private boolean available;

  // public API
  public boolean isOk() {
    if (health != null) {

      return health.equals(Status.HEALTHY);
    } else {

      return false;
    }
  }

  @Override
  public String toString() {
    return "PodState{" +
        "host='" + host + '\'' +
        //", containerID=" + containerID +
        ", health=" + health +
        '}';
  }

  @Override
  public int hashCode() {

    int prime = 31;
    int result = 1;
    int hostHash = getHost() != null
        ? getHost().hashCode()
        : 0;
    int containerIdHash = getContainerID() != null
        ? getContainerID().hashCode()
        : 0;

    result = prime * result + hostHash;
    result = prime * result + containerIdHash;

    return result;
  }

  @Override
  public boolean equals(Object obj) {

    if (obj == null || !(obj instanceof PodState)) {
      return false;
    }

    if (this == obj) {
      return true;
    }

    return this.hashCode() == obj.hashCode();
  }

  // getters & setters
  public Long getTimeStamp() {
    return timeStamp;
  }

  public PodState setTimeStamp(Long timeStamp) {
    this.timeStamp = timeStamp;
    return this;
  }

  public String getContainerID() {
    return containerID;
  }

  public PodState setContainerID(String containerID) {

    // member must be immutable to adhere to hashing contract
    if (this.containerID == null) {
      this.containerID = containerID;
    }
    return this;
  }

  public String getHost() {
    return host;
  }

  public PodState setHost(String host) {

    // member must be immutable to adhere to hashing contract
    if (this.host == null) {
      this.host = host;
    }
    return this;
  }

  public Status getHealth() {
    return health;
  }

  public PodState setHealth(Status health) {
    this.health = health;
    return this;
  }

  public boolean isAvailable() {
    return available;
  }

  public PodState setAvailable(boolean available) {
    this.available = available;
    return this;
  }
}
