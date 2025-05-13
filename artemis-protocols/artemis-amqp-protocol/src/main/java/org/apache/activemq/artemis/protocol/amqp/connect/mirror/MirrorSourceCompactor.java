/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.protocol.amqp.connect.mirror;

import java.lang.invoke.MethodHandles;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.impl.TransactionImpl;
import org.apache.activemq.artemis.utils.collections.LinkedListIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MirrorSourceCompactor {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   final StorageManager storageManager;
   final Queue snfQueue;

   public MirrorSourceCompactor(StorageManager storageManager, Queue snfQueue) {
      this.storageManager = storageManager;
      this.snfQueue = snfQueue;
   }

   // To be executed within the Queue's executor
   public void compact() throws Exception {
      Transaction tx = new TransactionImpl(storageManager);
      logger.info("Queue {} has {} messages", snfQueue.getName(), snfQueue.getMessageCount());
      try (LinkedListIterator<MessageReference> messages = snfQueue.iterator()) {
         while (messages.hasNext()) {
            MessageReference ref = messages.next();
            Message message = ref.getMessage();

            if (message.getAddress().equals(String.valueOf(snfQueue.getAddress()))) {
               logger.info("snf {}", message);
            } else {
               logger.info("compacting message with {} references, message={}", message.getRefCount(), message);
            }
         }
      }
   }

}
