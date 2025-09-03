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
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.core.journal.IOCompletion;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.tasks.MessageReferenceTask;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.tasks.MessageTask;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.tasks.TXTask;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.tasks.Task;
import org.apache.activemq.artemis.core.server.ActiveMQScheduledComponent;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatementsManager extends ActiveMQScheduledComponent {

   // TODO-IMPORTANT configure this
   private static final int NUMBER_OF_CONNECTIONS = 10;

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   final DatabaseStorageConfiguration databaseConfiguration;
   final JDBCConnectionProvider connectionProvider;
   final int batchSize;
   final Executor executorService;


   List<Worker> allWorkers;
   LinkedBlockingDeque<Worker> workers;

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
                            Executor executorService,
                            long flushTime,
                            DatabaseStorageConfiguration databaseConfiguration,
                            JDBCConnectionProvider connectionProvider,
                            int batchSize) throws SQLException {
      super(scheduledExecutorService, executor, 0, flushTime, TimeUnit.NANOSECONDS, true);

      allWorkers = new ArrayList<>();
      workers = new LinkedBlockingDeque<>(NUMBER_OF_CONNECTIONS);
      for (int i = 0; i < NUMBER_OF_CONNECTIONS; i++) {
         Worker worker =  new Worker(this, connectionProvider, databaseConfiguration, batchSize, "worker " + i);
         allWorkers.add(worker);
         workers.offer(worker);
      }

      this.executorService = executorService;

      logger.info("FlushTime {}", flushTime);
      this.databaseConfiguration = databaseConfiguration;
      this.connectionProvider = connectionProvider;
      this.batchSize = batchSize;
      init();
   }

   public void init() throws SQLException {
   }

   public void close() throws SQLException {
      allWorkers.forEach(w -> w.close());
      allWorkers.clear();
      workers.clear();

      // TODO close workers
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
      try {
         flush();
      } catch (Throwable e) {
         logger.warn(e.getMessage(), e);
      } finally {

      }
   }

   public void workerDone(Worker worker) {
      this.workers.offer(worker);
   }

   public void flush() {
      Worker worker = workers.poll();
      if (worker == null) {
         logger.info("nothing...");
         this.delay();
         return;
      }

      List<Task> taskList = extractTaskList();
      worker.setTaskList(taskList);

      executorService.execute(worker);
   }
}
