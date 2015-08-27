package com.pinterest.terrapin.base;

import com.twitter.ostrich.stats.Stats;
import com.twitter.util.Duration;
import com.twitter.util.Function;
import com.twitter.util.Function0;
import com.twitter.util.Future;
import com.twitter.util.JavaTimer;
import com.twitter.util.Return;
import com.twitter.util.Throw;
import com.twitter.util.Timer;
import com.twitter.util.Try;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Utility functions for dealing with Future(s)
 */
public class FutureUtil {
  private static final Timer timer = new JavaTimer();

  /**
   * Run speculative execution for a set of futures. This is especially useful for improving latency
   * and surviving failures. The idea is to fire a backup future if the original future does not
   * return within a certain time period. The 1st future which returns successfully is returned.
   *
   * @param originalFuture The original future which has already been issued.
   * @param functionBackupFuture A function which can issue the backup future once @waitTimeMillis
   *                             has expired.
   * @param waitTimeMillis The time to wait in milliseconds before issuing the backup future.
   * @param statsPrefix The stats prefix to be prefixed with recorded stats such as "won", "lost"
   *                    and "backup-fired".
   * @param <T>
   * @return Returns a future which encapsulates the speculative execution strategy.
   */
  public static <T> Future<T> getSpeculativeFuture(
      final Future<T> originalFuture,
      final Function0<Future<T>> functionBackupFuture,
      long waitTimeMillis,
      final String statsPrefix) {
    return getSpeculativeFuture(
        originalFuture,
        functionBackupFuture,
        waitTimeMillis,
        statsPrefix,
        BackupFutureRetryPolicy.UN_CONDITIONAL_RETRY_POLICY);
  }

  /**
   * Run speculative execution for a set of futures. This is especially useful for improving latency
   * and surviving failures. The provided BackupFutureRetryPolicy will decide whether the backup
   * future should be fired or not. The 1st future which returns successfully is returned.
   *
   * @param originalFuture The original future which has already been issued.
   * @param functionBackupFuture A function which can issue the backup future once @waitTimeMillis
   *                             has expired.
   * @param waitTimeMillis The time to wait in milliseconds before issuing the backup future.
   * @param statsPrefix The stats prefix to be prefixed with recorded stats such as "won", "lost"
   *                    and "backup-fired".
   * @param retryPolicy decides whether the backup future should be fired if the original future
   *                    fails
   * @param <T>
   * @return Returns a future which encapsulates the speculative execution strategy.
   */
  public static <T> Future<T> getSpeculativeFuture(
      final Future<T> originalFuture,
      final Function0<Future<T>> functionBackupFuture,
      long waitTimeMillis,
      final String statsPrefix,
      final BackupFutureRetryPolicy retryPolicy) {
    return originalFuture.within(
        Duration.fromMilliseconds(waitTimeMillis), timer).
        rescue(new Function<Throwable, Future<T>>() {
          public Future<T> apply(Throwable t) {
            if (retryPolicy.isRetriable(t)) {
              final Future<T> backupFuture = functionBackupFuture.apply();
              Stats.incr(statsPrefix + "-backup-fired");
              // Build information into each future as to whether this is the original
              // future or the backup future.
              final Future<Pair<Boolean, T>> originalFutureWithInfo = originalFuture.map(
                  new Function<T, Pair<Boolean, T>>() {
                    public Pair<Boolean, T> apply(T t) {
                      return new ImmutablePair(true, t);
                    }
                  });
              final Future<Pair<Boolean, T>> backupFutureWithInfo = backupFuture.map(
                  new Function<T, Pair<Boolean, T>>() {
                    public Pair<Boolean, T> apply(T t) {
                      return new ImmutablePair(false, t);
                    }
                  });
              // If there is an exception, the first future throwing the exception will
              // return. Instead we want the 1st future which is successful.
              Future<Pair<Boolean, T>> origFutureSuccessful = originalFutureWithInfo
                  .rescue(new Function<Throwable, Future<Pair<Boolean, T>>>() {
                    public Future<Pair<Boolean, T>> apply(Throwable t) {
                      // Fall back to back up future which may also fail in which case we bubble
                      // up the exception.
                      return backupFutureWithInfo;
                    }
                  });
              Future<Pair<Boolean, T>> backupFutureSuccessful = backupFutureWithInfo.rescue(
                  new Function<Throwable, Future<Pair<Boolean, T>>>() {
                    public Future<Pair<Boolean, T>> apply(Throwable t) {
                      // Fall back to original Future which may also fail in which case we bubble
                      // up the exception.
                      return originalFutureWithInfo;
                    }
                  });
              return origFutureSuccessful.select(backupFutureSuccessful).map(
                  new Function<Pair<Boolean, T>, T>() {
                    public T apply(Pair<Boolean, T> tuple) {
                      if (tuple.getLeft()) {
                        Stats.incr(statsPrefix + "-won");
                      } else {
                        Stats.incr(statsPrefix + "-lost");
                      }
                      return tuple.getRight();
                    }
                  }
              );
            } else {
              return Future.exception(t);
            }
          }
        });
  }

  public static <T> Future<Try<T>> lifeToTry(Future<T> future) {
    return future.map(new Function<T, Try<T>>() {
      @Override
      public Try<T> apply(T o) {
        return new Return(o);
      }
    }).handle(new Function<Throwable, Try<T>>() {
      @Override
      public Try<T> apply(Throwable throwable) {
        return new Throw(throwable);
      }
    });
  }
}
