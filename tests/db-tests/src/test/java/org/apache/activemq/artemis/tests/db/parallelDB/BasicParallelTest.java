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

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.persistence.impl.journal.OperationContextImpl;
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
                                   scheduledExecutorService, executorFactory.getExecutor());
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
   public void testStoreMessageFromServerNonTX() throws Exception {

      ActiveMQServer server = createServer(true, configuration);

      server.start();

      int nMessages = 0;

      String[] protocols = {"CORE", "AMQP", "OPENWIRE"};
      for (String p : protocols) {
         ConnectionFactory factory = CFUtil.createConnectionFactory( p, "tcp://localhost:61616");
         try (javax.jms.Connection connection = factory.createConnection()) {
            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
               MessageProducer producer = session.createProducer(session.createQueue("TEST"));
               for (int i = 0; i < 10; i++) {
                  producer.send(session.createTextMessage("test: " + p));
                  nMessages++;
               }
               checkMessageCounts(nMessages);
            }
         }
      }
      checkMessageCounts(nMessages);
      server.stop();

   }


   @TestTemplate
   public void testStoreMessageFromServerTX() throws Exception {

      ActiveMQServer server = createServer(true, configuration);
      configuration.addQueueConfiguration(QueueConfiguration.of("TEST").setRoutingType(RoutingType.ANYCAST));

      server.start();

      int nMessages = 0;

      String[] protocols = {"CORE", "AMQP", "OPENWIRE"};
      for (String p : protocols) {
         ConnectionFactory factory = CFUtil.createConnectionFactory( p, "tcp://localhost:61616");
         try (javax.jms.Connection connection = factory.createConnection()) {
            try (Session session = connection.createSession(true, Session.SESSION_TRANSACTED)) {
               MessageProducer producer = session.createProducer(session.createQueue("TEST"));
               for (int i = 0; i < 10; i++){
                  producer.send(session.createTextMessage("test: " + p));
                  nMessages++;
               }
               session.commit();
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
                                                                                       scheduledExecutorService,
                                                                                       executorFactory.getExecutor());
      parallelDBStorageManager.start();

      JDBCConnectionProvider connectionProvider = storageConfiguration.getConnectionProvider();

      int nrecords = 10;

      try (Connection connection = connectionProvider.getConnection()) {
         connection.setAutoCommit(false);
         for (int i = 1; i <= nrecords; i++) {
            CoreMessage message = new CoreMessage().initBuffer(1 * 1024).setDurable(true);
            message.setMessageID(i);
            message.getBodyBuffer().writeByte((byte) 'Z');
            parallelDBStorageManager.storeMessage(message);
         }
         parallelDBStorageManager.getStatementsManager().flushTL();
         OperationContextImpl.getContext().waitCompletion();
         assertEquals(nrecords, selectCount(connection, storageConfiguration.getParallelDBMessages()));
      }
   }


   @TestTemplate
   public void testStoreMessageOnBatchableStatementTX() throws Exception {
      ParallelDBStorageManager parallelDBStorageManager = new ParallelDBStorageManager(configuration,
                                                                                       criticalAnalyzer,
                                                                                       executorFactory,
                                                                                       executorFactory,
                                                                                       scheduledExecutorService,
                                                                                       executorFactory.getExecutor());
      parallelDBStorageManager.start();

      JDBCConnectionProvider connectionProvider = storageConfiguration.getConnectionProvider();

      int nrecords = 10;

      try (Connection connection = connectionProvider.getConnection()) {
         connection.setAutoCommit(false);
         for (int i = 1; i <= 10; i++) {
            CoreMessage message = new CoreMessage().initBuffer(1 * 1024).setDurable(true);
            message.setMessageID(i);
            message.getBodyBuffer().writeByte((byte) 'Z');
            parallelDBStorageManager.storeMessageTransactional(3, message);
            parallelDBStorageManager.storeReferenceTransactional(3, 3, message.getMessageID());
            parallelDBStorageManager.commit(3, true, true, true);
         }
         OperationContextImpl.getContext().waitCompletion();
         assertEquals(nrecords, selectCount(connection, storageConfiguration.getParallelDBMessages()));
      }
   }

}