/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.amqp.client;

import javax.jms.JMSException;

/**
 * Exception that indicates a blocking operation timed out while waiting for the remote to acknowledge or process it.
 */
public class AmqpOperationTimedOutException extends JMSException {

   private static final long serialVersionUID = -2509921066407708297L;

   public AmqpOperationTimedOutException(String reason) {
      super(reason);
   }

   public AmqpOperationTimedOutException(String reason, String errorCode) {
      super(reason, errorCode);
   }
}
