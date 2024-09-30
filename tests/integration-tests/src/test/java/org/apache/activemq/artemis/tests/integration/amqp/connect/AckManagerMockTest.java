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

package org.apache.activemq.artemis.tests.integration.amqp.connect;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;

import io.netty.util.collection.LongObjectHashMap;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.journal.collections.JournalHashMap;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.AckRetry;
import org.apache.activemq.artemis.core.persistence.impl.nullpm.NullStorageManager;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.AckReason;
import org.apache.activemq.artemis.core.server.impl.QueueImpl;
import org.apache.activemq.artemis.protocol.amqp.connect.mirror.AckManager;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AckManagerMockTest extends ActiveMQTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   @Test
   public void testAckManager() throws Exception {
      SimpleString fakeID = SimpleString.of("FAKE_ID");
      ConfigurationImpl configuration = new ConfigurationImpl();
      ActiveMQServer server = Mockito.mock(ActiveMQServer.class);
      NullStorageManager nullStorageManager = new NullStorageManager();
      Mockito.when(server.getStorageManager()).thenReturn(nullStorageManager);
      Mockito.when(server.getConfiguration()).thenReturn(configuration);
      Mockito.when(server.getNodeID()).thenReturn(fakeID);

      QueueImpl mockQueue = Mockito.mock(QueueImpl.class);

      PostOffice postOffice = Mockito.mock(PostOffice.class);
      Mockito.when(postOffice.findQueue(Mockito.anyLong())).thenReturn(mockQueue);

      Mockito.when(server.getPostOffice()).thenReturn(postOffice);
      AckManager manager = new AckManager(server);

      for (int i = 0; i < 100; i++) {
         manager.addRetry(fakeID.toString(), mockQueue, i, AckReason.NORMAL);
      }
      HashMap<SimpleString, LongObjectHashMap<JournalHashMap<AckRetry, AckRetry, Queue>>> retries =  manager.sortRetries();


   }

}
