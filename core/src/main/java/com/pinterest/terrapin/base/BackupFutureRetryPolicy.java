package com.pinterest.terrapin.base;

import java.util.concurrent.TimeoutException;

/**
 * Retry policy to decide whether we should fire the backup future
 */
public interface BackupFutureRetryPolicy {
  public static final UnConditionalRetryPolicy UN_CONDITIONAL_RETRY_POLICY =
      new UnConditionalRetryPolicy();
  public static final TimeoutRetryPolicy TIMEOUT_RETRY_POLICY = new TimeoutRetryPolicy();

  /***
   * Check to see whether we should fire the backup future or not
   * @param e the throwable to check
   * @return boolean
   */
  public boolean isRetriable(Throwable e);


  /***
   * Only fire the backup future if the primary future timeouts
   */
  public static class TimeoutRetryPolicy implements BackupFutureRetryPolicy {

    @Override
    public boolean isRetriable(Throwable e) {
      return e instanceof TimeoutException;
    }
  }

  /***
   * Always fire the backup future if the primary future fails
   */
  public static class UnConditionalRetryPolicy implements BackupFutureRetryPolicy {

    @Override
    public boolean isRetriable(Throwable e) {
      return true;
    }
  }
}
