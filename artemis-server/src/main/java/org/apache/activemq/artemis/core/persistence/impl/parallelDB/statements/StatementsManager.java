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

package org.apache.activemq.artemis.core.persistence.impl.parallelDB.statements;

import java.lang.invoke.MethodHandles;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.core.journal.IOCompletion;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.statements.tasks.Task;
import org.apache.activemq.artemis.core.server.ActiveMQScheduledComponent;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCConnectionProvider;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.statements.tasks.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatementsManager extends ActiveMQScheduledComponent {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   final DatabaseStorageConfiguration databaseConfiguration;
   final JDBCConnectionProvider connectionProvider;
   final int batchSize;

   DatabaseWorker worker;

   // We store messages and references on a ThreadLocal buffer until the last attribute is sent,
   // at that time we flush everything.
   // this is to make sure that we store everything as part of the same DB operation
   private static final ThreadLocal<List<Task>> bufferMessagesList = new ThreadLocal<>();

   private List<Task> getTLTaskList() {
      List<Task> tlTaskList = bufferMessagesList.get();

      if (tlTaskList == null) {
         tlTaskList = new ArrayList<>();
         bufferMessagesList.set(tlTaskList);
      }

      return tlTaskList;
   }

   ArrayList<Task> pendingTasks = new ArrayList<>();

   public MessageReferenceTask newReferenceTask(long messageID, long queueID, Long txID, IOCompletion context) {
      return new MessageReferenceTask(messageID, queueID, txID, context);
   }

   public MessageTask newMessageTask(Message message, Long txID, IOCompletion context) {
      return new MessageTask(message, txID, context);
   }


   public StatementsManager(ScheduledExecutorService scheduledExecutorService,
                            Executor executor,
                            long flushTime,
                            DatabaseStorageConfiguration databaseConfiguration,
                            JDBCConnectionProvider connectionProvider,
                            int batchSize) throws SQLException {
      super(scheduledExecutorService, executor, 0, flushTime, TimeUnit.MILLISECONDS, true);

      logger.info("FlushTime {}", flushTime);
      this.databaseConfiguration = databaseConfiguration;
      this.connectionProvider = connectionProvider;
      this.batchSize = batchSize;
      init();
   }

   public void init() throws SQLException {
      worker = new DatabaseWorker(connectionProvider, databaseConfiguration, batchSize);
   }

   public void close() throws SQLException {
      worker.close();
      worker = null;
   }

   public void storeTX(long txID, boolean messages, boolean references, IOCompletion callback) {
      getTLTaskList().add(new TXTask(txID, messages, references, callback));
   }

   public void storeMessage(Message message, Long tx, IOCompletion callback) {
      getTLTaskList().add(new MessageTask(message, tx, callback));
   }

   public void storeReference(long messageID, long queueID, Long txID, IOCompletion callback) {
      getTLTaskList().add(new MessageReferenceTask(messageID, queueID, txID, callback));
   }

   public void flushTL() {
      synchronized (this) {
         pendingTasks.addAll(getTLTaskList());
      }
      getTLTaskList().clear();
      delay();
   }

   private List<Task> extractTaskList() {
      ArrayList<Task> tasksToRun;
      synchronized (this) {
         tasksToRun = new ArrayList<>(pendingTasks);
         pendingTasks.clear();
      }
      return tasksToRun;
   }

   @Override
   public void run() {
      logger.info("Flushing::");
      try {
         flush();
      } catch (SQLException e) {
         logger.warn(e.getMessage(), e);
      } catch (Throwable e) {
         logger.warn(e.getMessage(), e);
      }
   }

   public void flush() throws SQLException {
      List<Task> taskList = extractTaskList();
      worker.doRun(taskList);
   }
}
