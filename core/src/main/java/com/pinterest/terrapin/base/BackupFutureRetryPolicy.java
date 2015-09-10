/**
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
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
