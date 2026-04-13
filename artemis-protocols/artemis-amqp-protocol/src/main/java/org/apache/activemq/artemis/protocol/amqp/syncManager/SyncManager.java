/*
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

package org.apache.activemq.artemis.protocol.amqp.syncManager;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.server.ActiveMQScheduledComponent;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.selector.strict.Token;
import org.apache.activemq.artemis.utils.UUID;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncManager extends ActiveMQScheduledComponent {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   // short for Sync Property
   private final SimpleString TOKEN_PROPERTY = SimpleString.of("SNCPRP");

   private final ConcurrentHashMap<UUID, SyncToken> tokens = new ConcurrentHashMap<>();

   public SyncManager(ScheduledExecutorService scheduledExecutor, Executor executor, long checkPeriod) {
      super(scheduledExecutor, executor, checkPeriod, checkPeriod, TimeUnit.SECONDS, false);
   }

   @Override
   public void run() {
      tokens.forEach((a, b) -> {
         b.scan();
      });
   }


   public SyncToken messageSend(Message message, OperationContext context) {
      SyncToken token;

      try {
          token = (SyncToken) message.getUserContext(TOKEN_PROPERTY);
      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
         // this is not expected to happen, since we only set bytes properties in this
         throw new RuntimeException(e.getMessage(), e);
      }

      if (token != null) {
         return token;
      } else {
         UUID uuid = UUIDGenerator.getInstance().generateUUID();
         byte[] uuidBytes = uuid.asBytes();
         token = new SyncToken(uuid, context, this);
         this.tokens.put(uuid, token);
         message.setUserContext(TOKEN_PROPERTY, token);
         message.putExtraBytesProperty(TOKEN_PROPERTY, uuidBytes);
         return token;
      }
   }

   public void done(SyncToken token) {
      tokens.remove(token.getKey());
   }

   private UUID getUUIDFromExtraProperty(Message message) {
      try {
         byte[] extraProperty = message.getExtraBytesProperty(TOKEN_PROPERTY);
         if (extraProperty != null) {
            return UUID.of(extraProperty);
         } else {
            return null;
         }
      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
         return null;
      }
   }


   public void messageSendDone(MessageReference reference) {
      logger.info("Message done {}", reference);
      SyncToken token = reference.getProtocolData(SyncToken.class);
      if (token != null) {
         token.done();
      } else {
         token = (SyncToken) reference.getMessage().getUserContext(TOKEN_PROPERTY);
         if (token == null) {
            UUID tokenUUID = getUUIDFromExtraProperty(reference.getMessage());
            if (tokenUUID != null) {
               token = tokens.get(tokenUUID);
            }
         }

         if (token != null) {
            token.done();
         }
      }
   }

   public void messageAck(MessageReference reference, OperationContext context) {
      if (context != null) {
         SyncToken token = reference.getProtocolData(SyncToken.class);
         if (token == null) {
            UUID uuid = UUIDGenerator.getInstance().generateUUID();
            token = new SyncToken(uuid, context, this);
            tokens.put(uuid, token);
            reference.setProtocolData(SyncToken.class, token);
         }

      }
   }


}
