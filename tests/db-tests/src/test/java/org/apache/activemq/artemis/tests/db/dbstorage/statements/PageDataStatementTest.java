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
package org.apache.activemq.artemis.tests.db.dbstorage.statements;

import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.persistence.impl.database.DatabaseStorageManager;
import org.apache.artemis.database.data.DeletePageData;
import org.apache.artemis.database.data.PageData;
import org.apache.artemis.database.statements.InsertPageStatement;
import org.apache.artemis.database.statements.DeletePageStatement;
import org.apache.activemq.artemis.tests.db.dbstorage.CountDownCompletion;
import org.apache.activemq.artemis.tests.extensions.parameterized.ParameterizedTestExtension;
import org.apache.artemis.database.DatabaseProvider;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisabledIf("isNoDatabaseSelected")
@ExtendWith(ParameterizedTestExtension.class)
public class PageDataStatementTest extends AbstractStatementTest {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   @TestTemplate
   public void testInsertPageDirectly() throws Exception {
      DatabaseStorageManager databaseStorageManager = new DatabaseStorageManager(configuration,
                                                                                 criticalAnalyzer,
                                                                                 executorFactory,
                                                                                 executorFactory,
                                                                                 scheduledExecutorService,
                                                                                 executorService,
                                                                                 null);
      databaseStorageManager.start();

      DatabaseProvider databaseProvider = storageConfiguration.getDatabaseProvider();

      int nrecords = 100;

      CountDownCompletion latch = new CountDownCompletion(nrecords);

      try (Connection connection = databaseProvider.getConnection()) {
         connection.setAutoCommit(false);
         InsertPageStatement insertPageStatement = new InsertPageStatement(databaseProvider, connection, nrecords);
         for (int i = 1; i <= nrecords; i++) {
            CoreMessage message = new CoreMessage().initBuffer(1024).setDurable(true);
            message.setMessageID(i);
            message.getBodyBuffer().writeByte((byte) 'Z');
            PageData task = new PageData(1, 1, i, message.getMessageID(), () -> encodeMessage(message), null, latch);
            insertPageStatement.addElement(task, latch);
         }
         insertPageStatement.flushPending(true);

         assertEquals(nrecords, selectCount(connection, "DB_PAGE"));
      }

      assertTrue(latch.await(10, TimeUnit.SECONDS));
   }

   @TestTemplate
   public void testInsertAndDeletePage() throws Exception {
      DatabaseStorageManager databaseStorageManager = new DatabaseStorageManager(configuration,
                                                                                 criticalAnalyzer,
                                                                                 executorFactory,
                                                                                 executorFactory,
                                                                                 scheduledExecutorService,
                                                                                 executorService,
                                                                                 null);
      databaseStorageManager.start();

      DatabaseProvider databaseProvider = storageConfiguration.getDatabaseProvider();

      int nrecords = 50;
      int nPages = 2;

      CountDownCompletion insertLatch = new CountDownCompletion(nrecords * nPages);

      try (Connection connection = databaseProvider.getConnection()) {
         connection.setAutoCommit(false);
         InsertPageStatement insertPageStatement = new InsertPageStatement(databaseProvider, connection, nrecords * nPages);
         for (int page = 1; page <= nPages; page++) {
            for (int i = 1; i <= nrecords; i++) {
               CoreMessage message = new CoreMessage().initBuffer(1024).setDurable(true);
               message.setMessageID(page * 1000 + i);
               message.getBodyBuffer().writeByte((byte) 'Z');
               PageData task = new PageData(1, page, i, message.getMessageID(), () -> encodeMessage(message), null, insertLatch);
               insertPageStatement.addElement(task, insertLatch);
            }
         }
         insertPageStatement.flushPending(true);

         assertEquals(nrecords * nPages, selectCount(connection, "DB_PAGE"));

         CountDownCompletion deleteLatch = new CountDownCompletion(1);
         DeletePageStatement deletePageStatement = new DeletePageStatement(databaseProvider, connection, 1);
         deletePageStatement.addElement(new DeletePageData(1, 1, deleteLatch), deleteLatch);
         deletePageStatement.flushPending(true);

         assertEquals(nrecords, selectCount(connection, "DB_PAGE"));
      }

      assertTrue(insertLatch.await(10, TimeUnit.SECONDS));
   }

   private static ActiveMQBuffer encodeMessage(Message message) {
      int size = message.getPersister().getEncodeSize(message);
      ActiveMQBuffer buffer = ActiveMQBuffers.fixedBuffer(size);
      message.getPersister().encode(buffer, message);
      return buffer;
   }
}
