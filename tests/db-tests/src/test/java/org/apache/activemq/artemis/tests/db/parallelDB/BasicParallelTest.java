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
package org.apache.activemq.artemis.tests.db.parallelDB;

import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.sql.Connection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.ParallelDBStorageManager;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCConnectionProvider;
import org.apache.activemq.artemis.tests.extensions.parameterized.ParameterizedTestExtension;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@DisabledIf("isNoDatabaseSelected")
@ExtendWith(ParameterizedTestExtension.class)
public class BasicParallelTest extends AbstractStatementTest {

   @TestTemplate
   public void testStoreMessage() throws Exception {
      ParallelDBStorageManager parallelDBStorageManager = new ParallelDBStorageManager(configuration,
                                   criticalAnalyzer,
                                   executorFactory,
                                   executorFactory,
                                   scheduledExecutorService);
      parallelDBStorageManager.start();

      CoreMessage message = new CoreMessage().initBuffer(10 * 1024).setDurable(true);

      message.setMessageID(333);
      message.getBodyBuffer().writeByte((byte)'Z');

      parallelDBStorageManager.storeMessage(message);
      parallelDBStorageManager.storeReference(1, 333, true);

      CountDownLatch done = new CountDownLatch(1);
      parallelDBStorageManager.getContext().executeOnCompletion(new IOCallback() {
         @Override
         public void done() {
            done.countDown();
         }

         @Override
         public void onError(int errorCode, String errorMessage) {

         }
      });

      assertTrue(done.await(10, TimeUnit.SECONDS));
   }


   @TestTemplate
   public void testStoreMessageFromServer() throws Exception {

      ActiveMQServer server = createServer(true, configuration);

      server.start();

      int nMessages = 0;

      String[] protocols = {"CORE", "AMQP", "OPENWIRE"};
      for (String p : protocols) {
         ConnectionFactory factory = CFUtil.createConnectionFactory( p, "tcp://localhost:61616");
         try (javax.jms.Connection connection = factory.createConnection()) {
            try (Session session = connection.createSession(true, Session.SESSION_TRANSACTED)) {
               MessageProducer producer = session.createProducer(session.createQueue("TEST"));
               producer.send(session.createTextMessage("test: " + p));
               session.commit();
               nMessages++;
               checkMessageCounts(nMessages);
            }
            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
               MessageProducer producer = session.createProducer(session.createQueue("TEST"));
               producer.send(session.createTextMessage("test: " + p));
               nMessages++;
               checkMessageCounts(nMessages);
            }
         }
      }
      checkMessageCounts(nMessages);
      server.stop();

   }

   @TestTemplate
   public void testStoreMessageOnBatchableStatement() throws Exception {
      ParallelDBStorageManager parallelDBStorageManager = new ParallelDBStorageManager(configuration,
                                                                                       criticalAnalyzer,
                                                                                       executorFactory,
                                                                                       executorFactory,
                                                                                       scheduledExecutorService);
      parallelDBStorageManager.start();

      JDBCConnectionProvider connectionProvider = storageConfiguration.getConnectionProvider();

      int nrecords = 100;

      CountDownLatch latch = new CountDownLatch(nrecords);

      try (Connection connection = connectionProvider.getConnection()) {
         connection.setAutoCommit(false);
         for (int i = 1; i <= 100; i++) {
            CoreMessage message = new CoreMessage().initBuffer(1 * 1024).setDurable(true);
            message.setMessageID(i);
            message.getBodyBuffer().writeByte((byte) 'Z');
            parallelDBStorageManager.storeMessage(message);
         }
         parallelDBStorageManager.getStatementsManager().flushTL();
         latch.await(10, TimeUnit.SECONDS);
         assertTrue(latch.await(10, TimeUnit.SECONDS));
         assertEquals(100, selectCount(connection, storageConfiguration.getParallelDBMessages()));
      }
   }


   @TestTemplate
   public void testStoreMessageOnBatchableStatementTX() throws Exception {
      ParallelDBStorageManager parallelDBStorageManager = new ParallelDBStorageManager(configuration,
                                                                                       criticalAnalyzer,
                                                                                       executorFactory,
                                                                                       executorFactory,
                                                                                       scheduledExecutorService);
      parallelDBStorageManager.start();

      JDBCConnectionProvider connectionProvider = storageConfiguration.getConnectionProvider();

      int nrecords = 100;

      CountDownLatch latch = new CountDownLatch(nrecords);

      try (Connection connection = connectionProvider.getConnection()) {
         connection.setAutoCommit(false);
         for (int i = 1; i <= 100; i++) {
            CoreMessage message = new CoreMessage().initBuffer(1 * 1024).setDurable(true);
            message.setMessageID(i);
            message.getBodyBuffer().writeByte((byte) 'Z');
            parallelDBStorageManager.storeMessageTransactional(3, message);
            parallelDBStorageManager.storeReferenceTransactional(3, 3, message.getMessageID());
            parallelDBStorageManager.commit(3, true, true, true);
         }
         latch.await(10, TimeUnit.SECONDS);
         assertTrue(latch.await(10, TimeUnit.SECONDS));
         assertEquals(100, selectCount(connection, storageConfiguration.getParallelDBMessages()));
      }
   }

}