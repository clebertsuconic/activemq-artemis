/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.api.core;

import static org.apache.activemq.api.core.ActiveMQExceptionType.SESSION_CREATION_REJECTED;

/**
 * The creation of a session was rejected by the server (e.g. if the server is starting and has not
 * finish to be initialized.
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a> 5/2/12
 */
public final class ActiveMQSessionCreationException extends ActiveMQException
{
   private static final long serialVersionUID = -4486139158452585895L;

   public ActiveMQSessionCreationException()
   {
      super(SESSION_CREATION_REJECTED);
   }

   public ActiveMQSessionCreationException(String msg)
   {
      super(SESSION_CREATION_REJECTED, msg);
   }
}
