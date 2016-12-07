package com.bikeemotion.gravedigger.impl;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class PodStateTest {

  @Test
  public void test_equals() {

    PodState p1 = new PodState()
        .setHost("0.0.0.0");

    PodState p2 = new PodState()
        .setHost("0.0.0.0");

    assertEquals(p1, p2);
  }

  @Test
  public void test_not_equals() {

    PodState p1 = new PodState()
        .setHost("0.0.0.0");

    PodState p2 = new PodState()
        .setHost("0.0.0.1");

    assertNotEquals(p1, p2);
  }
}
