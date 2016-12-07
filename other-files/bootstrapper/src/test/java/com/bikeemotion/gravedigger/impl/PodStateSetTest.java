package com.bikeemotion.gravedigger.impl;

import org.joda.time.DateTime;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class PodStateSetTest {

  private static final String test_get_victims_provider = "test_get_victims_provider";
  private static final String test_upsert_provider = "test_upsert_provider";
  private static final String test_clean_absent_provider = "test_clean_absent_provider";
  private static final String test_get_healthy_provider = "test_get_healthy_provider";

  @Test(dataProvider = test_get_healthy_provider)
  public void test_get_healthy(
      PodStateSet lastStates,
      Set<PodState> freshStates,
      Set<PodState> expectedHealthy) {

    Set<PodState> healthy = lastStates.getHealthy(freshStates);
    if (expectedHealthy != null) {

      assertNotNull(healthy);

      Iterator<PodState> expectedIterator = expectedHealthy.iterator();
      Iterator<PodState> currentIterator = healthy.iterator();
      while (expectedIterator.hasNext()) {

        PodState expected = expectedIterator.next();
        PodState actual = currentIterator.next();

        assertEquals(actual, expected);
        assertEquals(actual.getTimeStamp(), expected.getTimeStamp());
        assertTrue(actual.isOk());
      }

    } else {

      assertNull(healthy);
    }
  }

  @DataProvider(name = test_get_healthy_provider)
  public Object[][] test_get_healthy_provider() {
    return new Object[][] {
        new Object[]
        {
            // new refurbished entry
            new PodStateSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))
            ),

            new HashSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 36).getMillis()))
            ),

            new HashSet(
                asList(
                new PodState()
                    .setHealth(PodState.Status.HEALTHY)
                    .setContainerID("0.0.0.0")
                    .setHost("0.0.0.0")
                    .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis()))
            )
        },
        {
            // two new refurbished entries
            new PodStateSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))
            ),

            new HashSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 36).getMillis()))
            ),

            new HashSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 36).getMillis()))
            )
        },
        {
            // repeated ok entries
            new PodStateSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))
            ),

            new HashSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 36).getMillis()))
            ),

            // not expecting to refurbish anything - it was all good to start with
            null
        },
        {
            // new NOK entry
            new PodStateSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))
            ),

            new HashSet(
                Collections.singleton(
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis()))
            ),

            // not expecting to refurbish anything - it was all good to start with
            null
        },
        {
            // new unkown OK entry
            new PodStateSet(),

            new HashSet(
                Collections.singleton(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis()))
            ),

            // expecting the new pod
            new HashSet(
                Collections.singleton(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis()))
            ),
        },

        {
            // new pod with same ip but different ID
            new PodStateSet(
                Collections.singleton(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("c1")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()))
            ),

            new HashSet(
                Collections.singleton(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("c2")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()))
            ),

            // new pod has it has a new ID
            new HashSet(
                Collections.singleton(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("c2")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()))
            )
        }
    };
  }

  @Test(dataProvider = test_get_victims_provider)
  public void test_get_victims(
      PodStateSet lastStates,
      Set<PodState> freshStates,
      PodState expectedVictim) {

    PodState victim = lastStates.getVictim(freshStates);
    if (expectedVictim != null) {

      assertNotNull(victim);
      assertEquals(victim.isOk(), false);
      assertEquals(victim, expectedVictim);
      assertEquals(victim.getTimeStamp(), expectedVictim.getTimeStamp());
    } else {

      assertNull(victim);
    }
  }

  @DataProvider(name = test_get_victims_provider)
  public Object[][] test_get_victims_provider() {
    return new Object[][] {
        new Object[]
        {
            // new sick entry
            new PodStateSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))
            ),

            new HashSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 36).getMillis()))
            ),

            new PodState()
                .setHealth(PodState.Status.SICK)
                .setContainerID("0.0.0.1")
                .setHost("0.0.0.1")
                .setTimeStamp(new DateTime(1999, 1, 1, 15, 36).getMillis())
        },
        {
            // update a sick entry
            new PodStateSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))
            ),

            new HashSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 36).getMillis()))
            ),

            // already sick pod before the last update
            null
        },
        {
            // new sick entry with an already existing one
            new PodStateSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))
            ),

            new HashSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 36).getMillis()))
            ),

            // can kill more until at least one pod is OK
            null
        },
        {
            // 2 new sick entries
            new PodStateSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))
            ),

            new HashSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 36).getMillis()))
            ),

            new PodState()
                .setHealth(PodState.Status.SICK)
                .setContainerID("0.0.0.0")
                .setHost("0.0.0.0")
                .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis())
        },
        {
            // 2 sick entries recover to healthy
            new PodStateSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))
            ),

            new HashSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 36).getMillis()))
            ),

            // pods recovered, no one is sick now
            null
        },
        {
            // 1 sick entry that already exists
            new PodStateSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))
            ),

            new HashSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 36).getMillis()))
            ),

            // already sick pod before the last update
            null
        },
        {
            // new unkown NOK entry
            new PodStateSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))
            ),

            new HashSet(
                Collections.singleton(
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.2")
                        .setHost("0.0.0.2")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis()))
            ),

            // must be reported as NOK if no previous state was known
            new PodState()
                .setHealth(PodState.Status.SICK)
                .setContainerID("0.0.0.2")
                .setHost("0.0.0.2")
                .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis())
        },
        {
            // new NOK entry - same IP, different container
            new PodStateSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("container-Id-1")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("container-Id-2")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))
            ),

            new HashSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("container-Id-3")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("container-Id-2")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))
            ),

            // must be reported as NOK because its a different container
            new PodState()
                .setHealth(PodState.Status.SICK)
                .setContainerID("container-Id-3")
                .setHost("0.0.0.0")
                .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis())
        }
    };
  }

  @Test(dataProvider = test_upsert_provider)
  public void test_upsert(
      PodStateSet lastStates,
      Set<PodState> saved,
      PodState victim,
      PodStateSet expectedUpsertedStates) {

    PodStateSet upsertedPodStates = lastStates.upsert(saved, victim);
    assertEquals(upsertedPodStates.toString(), expectedUpsertedStates.toString());
  }

  @DataProvider(name = test_upsert_provider)
  public Object[][] test_upsert_provider() {
    return new Object[][] {
        new Object[]
        {
            // updating two OK elements to OK and NOK
            new PodStateSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))
            ),

            new HashSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 36).getMillis()))
            ),

            new PodState()
                .setHealth(PodState.Status.SICK)
                .setContainerID("0.0.0.1")
                .setHost("0.0.0.1")
                .setTimeStamp(new DateTime(1999, 1, 1, 15, 36).getMillis()),

            new PodStateSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 36).getMillis()))
            )
        },
        // updating OK elements to OK
        {
            new PodStateSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))
            ),

            new HashSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 36).getMillis()))
            ),

            null,

            new PodStateSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 36).getMillis()))
            )
        },

        // updating OK elements to NOK
        {
            new PodStateSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))
            ),

            new HashSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 36).getMillis()))
            ),

            new PodState()
                .setHealth(PodState.Status.SICK)
                .setContainerID("0.0.0.0")
                .setHost("0.0.0.0")
                .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis()),

            new PodStateSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))
            )
        },

        // updating NOK elements to NOK
        {
            new PodStateSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))
            ),

            new HashSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 36).getMillis()))
            ),

            null,

            new PodStateSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))
            )
        },

        // updating NOK elements to OK
        {
            new PodStateSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))
            ),

            new HashSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 36).getMillis()))
            ),

            null,

            new PodStateSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))
            )
        },

        // updating NOK where victim poisoning failed
        {
            new PodStateSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))
            ),

            new HashSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 36).getMillis()))
            ),

            null,

            new PodStateSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))
            )
        },

        // update pod that has the same IP but not the same container ID
        {
            new PodStateSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("c1")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("c2")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))
            ),

            null,

            new PodState()
                .setHealth(PodState.Status.SICK)
                .setContainerID("c3")
                .setHost("0.0.0.1")
                .setTimeStamp(new DateTime(1999, 1, 1, 15, 36).getMillis()),

            new PodStateSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("c1")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("c2")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 36).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.SICK)
                        .setContainerID("c3")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 37).getMillis()))
            )
        }
    };
  }

  @Test(dataProvider = test_clean_absent_provider)
  public void test_clean_absent(
      PodStateSet lastStates,
      Set<PodState> freshStates,
      PodStateSet expectedCleansedStates,
      Set<PodState> expectedMissingSates) {

    Set missingPodStates = lastStates.cleanAbsent(freshStates);
    assertEquals(lastStates.toString(), expectedCleansedStates.toString());
    assertEquals(missingPodStates.toString(), expectedMissingSates.toString());
  }

  @DataProvider(name = test_clean_absent_provider)
  public Object[][] test_clean_absent_provider() {
    return new Object[][] {
        new Object[]
        // clean 1 missing element
        {
            new PodStateSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))
            ),

            new HashSet(
                Collections.singleton(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 36).getMillis()))
            ),

            new PodStateSet(
                Collections.singleton(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))
            ),

            new HashSet(
                Collections.singleton(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()))
            ),
        },
        // clean 2 missing element
        {
            new PodStateSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))
            ),

            new HashSet(),

            new PodStateSet(),

            new HashSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))
            ),
        },
        // clean 0 elements
        {
            new PodStateSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))
            ),

            new HashSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 36).getMillis()))
            ),

            new PodStateSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.0")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("0.0.0.1")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))

            ),

            new HashSet(),
        },
        // clean 1 missing element - different IDs
        {
            new PodStateSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("c1")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 30).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("c2")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))
            ),

            new HashSet(
                asList(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("c1")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis()),
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("c3")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis()))
            ),

            new PodStateSet(
                Collections.singleton(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("c1")
                        .setHost("0.0.0.0")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 35).getMillis())
                    )
            ),

            new HashSet(
                Collections.singleton(
                    new PodState()
                        .setHealth(PodState.Status.HEALTHY)
                        .setContainerID("c2")
                        .setHost("0.0.0.1")
                        .setTimeStamp(new DateTime(1999, 1, 1, 15, 31).getMillis()))
            ),
        }
    };
  }
}
