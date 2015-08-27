package com.pinterest.terrapin.base;

import com.twitter.ostrich.stats.Stats;
import com.twitter.util.Duration;
import com.twitter.util.Function0;
import com.twitter.util.Future;
import com.twitter.util.JavaTimer;
import com.twitter.util.Timer;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for FutureUtil.
 */
public class FutureUtilTest {
  private static final String EXCEPTION_MSG = "Future failed.";
  private static final String STATS_PREFIX = "test";
  private static final String BACKUP_FIRED_COUNTER = "test-backup-fired";
  private static final String WON_COUNTER = "test-won";
  private static final String LOST_COUNTER = "test-lost";

  private boolean isBackupFutureInvoked;
  private static Timer timer;

  @Before
  public void setup() {
    this.isBackupFutureInvoked = false;
    Stats.removeCounter(BACKUP_FIRED_COUNTER);
    Stats.removeCounter(WON_COUNTER);
    Stats.removeCounter(LOST_COUNTER);
  }

  @BeforeClass
  public static void setupClass() {
    timer = new JavaTimer();
  }

  private Future<Integer> getFuture(final long futureExecutionTimeMs,
                                    final Integer futureValue,
                                    final boolean isFutureSuccessful) {
    return timer.doLater(
        Duration.fromMilliseconds(futureExecutionTimeMs),
          new Function0<Integer>() {
            public Integer apply() {
              if (isFutureSuccessful) {
                return futureValue;
              } else {
                throw new RuntimeException(EXCEPTION_MSG);
              }
            }
          });
   }

  private Function0<Future<Integer>> getBackupFutureFunctionWithSuccess(
      final long futureExecutionTimeMs,
      final Integer futureValue) {
    return new Function0<Future<Integer>>() {
      public Future<Integer> apply() {
        isBackupFutureInvoked = true;
        return getFuture(futureExecutionTimeMs, futureValue, true);
      }
    };
  }

  private Function0<Future<Integer>> getBackupFutureFunctionWithFailure(
      final long futureExecutionTimeMs,
      final Integer futureValue) {
    return new Function0<Future<Integer>>() {
      public Future<Integer> apply() {
        isBackupFutureInvoked = true;
        return getFuture(futureExecutionTimeMs, futureValue, false);
      }
    };
  }

  private void checkStats(int backupFired, int won, int lost) {
    assertEquals(backupFired, Stats.getCounter(BACKUP_FIRED_COUNTER).apply());
    assertEquals(won, Stats.getCounter(WON_COUNTER).apply());
    assertEquals(lost, Stats.getCounter(LOST_COUNTER).apply());
  }

  /*
   * Following 2 test cases deal with the case when the original future either succeeds
   * or fails before the timeout for speculative execution.
   */
  @Test
  public void testNoBackupFutureExecutedSuccess() {
    // First future succeeds soon enough.
    Future<Integer> originalFuture = getFuture(50, 1, true);
    Function0<Future<Integer>> backupFunctionFuture = getBackupFutureFunctionWithSuccess(500, 2);
    Future<Integer> speculativeFuture = FutureUtil.getSpeculativeFuture(
        originalFuture, backupFunctionFuture, 100, STATS_PREFIX);
    assertEquals(1, speculativeFuture.get().intValue());
    assertFalse(isBackupFutureInvoked);
    checkStats(0, 0, 0);
  }

  @Test
  public void testNoBackupFutureExecutedEarlyFailureForTimeBasedPolicy() {
    // First future fails soon enough.
    Future<Integer> originalFuture = getFuture(50, 1, false);
    Function0<Future<Integer>> backupFunctionFuture = getBackupFutureFunctionWithSuccess(500, 2);
    Future<Integer> speculativeFuture = FutureUtil.getSpeculativeFuture(
        originalFuture,
        backupFunctionFuture,
        100,
        STATS_PREFIX,
        BackupFutureRetryPolicy.TIMEOUT_RETRY_POLICY);
    Exception e = null;
    try {
      speculativeFuture.get();
    } catch (RuntimeException re) {
      e = re;
    }
    assertNotNull(e);
    assertEquals(EXCEPTION_MSG, e.getMessage());
    assertFalse(isBackupFutureInvoked);
    checkStats(0, 0, 0);
  }

  @Test
  public void testBackupFutureExecutedEarlyFailure() {
    // First future fails before timeout and the backup future gets executed
    Future<Integer> originalFuture = getFuture(50, 1, false);
    Function0<Future<Integer>> backupFunctionFuture = getBackupFutureFunctionWithSuccess(500, 2);
    Future<Integer> speculativeFuture = FutureUtil.getSpeculativeFuture(
        originalFuture,
        backupFunctionFuture,
        100,
        STATS_PREFIX,
        BackupFutureRetryPolicy.UN_CONDITIONAL_RETRY_POLICY);
    speculativeFuture.get();
    assertTrue(isBackupFutureInvoked);
    checkStats(1, 0, 1);
  }

  /**
   * Following 2 test cases deal with the case when both futures succeed.
   */
  @Test
  public void testBothFutureSuccessAndOriginalFinishesFirst() {
    Future<Integer> originalFuture = getFuture(200, 1, true);
    Function0<Future<Integer>> backupFunctionFuture = getBackupFutureFunctionWithSuccess(1000, 2);
    Future<Integer> speculativeFuture = FutureUtil.getSpeculativeFuture(
        originalFuture, backupFunctionFuture, 100, STATS_PREFIX);
    assertEquals(1, speculativeFuture.get().intValue());
    assertTrue(isBackupFutureInvoked);
    checkStats(1, 1, 0);
  }

  @Test
  public void testBothFutureSuccessAndBackupFinishesFirst() {
    Future<Integer> originalFuture = getFuture(1000, 1, true);
    Function0<Future<Integer>> backupFunctionFuture = getBackupFutureFunctionWithSuccess(200, 2);
    Future<Integer> speculativeFuture = FutureUtil.getSpeculativeFuture(
        originalFuture, backupFunctionFuture, 100, STATS_PREFIX);
    int value = speculativeFuture.get().intValue();
    assertTrue(isBackupFutureInvoked);
    assertEquals(2, value);
    checkStats(1, 0, 1);
  }

  /**
   * Following 2 test cases deal with the case when the first future that finishes throws an
   * exception.
   */
  @Test
  public void testOriginalFailsAndBackupFinishesSecond() {
    Future<Integer> originalFuture = getFuture(300, 1, false);
    Function0<Future<Integer>> backupFunctionFuture = getBackupFutureFunctionWithSuccess(800, 2);
    Future<Integer> speculativeFuture = FutureUtil.getSpeculativeFuture(
        originalFuture, backupFunctionFuture, 100, STATS_PREFIX);
    assertEquals(2, speculativeFuture.get().intValue());
    assertTrue(isBackupFutureInvoked);
    checkStats(1, 0, 1);
  }

  @Test
  public void testBackupFailsAndOriginalFinishesSecond() {
    Future<Integer> originalFuture = getFuture(1000, 1, true);
    Function0<Future<Integer>> backupFunctionFuture = getBackupFutureFunctionWithFailure(400, 2);
    Future<Integer> speculativeFuture = FutureUtil.getSpeculativeFuture(
        originalFuture, backupFunctionFuture, 100, STATS_PREFIX);
    assertEquals(1, speculativeFuture.get().intValue());
    assertTrue(isBackupFutureInvoked);
    checkStats(1, 1, 0);
  }

  /*
   * Following 2 test cases deal with the case when the first future to return succeeds while the
   * future that runs late errors out.
   */
  @Test
  public void testOriginalFailsAndBackupFinishesFirst() {
    Future<Integer> originalFuture = getFuture(500, 1, false);
    Function0<Future<Integer>> backupFunctionFuture = getBackupFutureFunctionWithSuccess(10, 2);
    Future<Integer> speculativeFuture = FutureUtil.getSpeculativeFuture(
        originalFuture, backupFunctionFuture, 100, STATS_PREFIX);
    assertEquals(2, speculativeFuture.get().intValue());
    assertTrue(isBackupFutureInvoked);
    checkStats(1, 0, 1);
  }

  @Test
  public void testBackupFailsAndOriginalFinishesFirst() {
    Future<Integer> originalFuture = getFuture(300, 1, true);
    Function0<Future<Integer>> backupFunctionFuture = getBackupFutureFunctionWithFailure(800, 2);
    Future<Integer> speculativeFuture = FutureUtil.getSpeculativeFuture(
        originalFuture, backupFunctionFuture, 100, STATS_PREFIX);
    assertEquals(1, speculativeFuture.get().intValue());
    assertTrue(isBackupFutureInvoked);
    checkStats(1, 1, 0);
  }

  /*
   * Following test case deals with the case when both futures error out.
   */
  @Test
  public void testBothFutureFailure() {
    Future<Integer> originalFuture = getFuture(200, 1, false);
    Function0<Future<Integer>> backupFunctionFuture = getBackupFutureFunctionWithFailure(200, 2);
    Future<Integer> speculativeFuture = FutureUtil.getSpeculativeFuture(
        originalFuture, backupFunctionFuture, 100, STATS_PREFIX);
    Exception e = null;
    try {
      speculativeFuture.get();
    } catch (RuntimeException re) {
      e = re;
    }
    assertNotNull(e);
    assertEquals(EXCEPTION_MSG, e.getMessage());
    assertTrue(isBackupFutureInvoked);
    checkStats(1, 0, 0);
  }
}
