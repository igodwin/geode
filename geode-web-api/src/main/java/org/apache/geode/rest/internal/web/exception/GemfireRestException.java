/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.rest.internal.web.exception;

import org.apache.geode.GemFireException;

/**
 * Indicates that error encountered while processing REST request.
 * <p/>
 * 
 * @since GemFire 8.0
 */

public class GemfireRestException extends GemFireException {

  /**
   * Create the exception with the given message.
   * 
   * @param message the message of the new exception
   */
  public GemfireRestException(String message) {
    super(message);
  }

  /**
   * Create the exception with the given message and cause.
   * 
   * @param message the message of the new exception
   * @param cause the cause of the new exception
   */
  public GemfireRestException(String message, Throwable cause) {
    super(message, cause);
  }

}
