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

package org.apache.activemq.artemis.core.persistence.impl.parallelDB.worker;

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
import org.apache.activemq.artemis.core.persistence.StorageTX;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.ParallelDBStoreTX;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.dbdata.DBData;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.dbdata.DeleteMessageData;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.dbdata.DeleteReferenceData;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.dbdata.MessageData;
import org.apache.activemq.artemis.core.persistence.impl.parallelDB.dbdata.MessageReferenceData;
import org.apache.activemq.artemis.core.server.ActiveMQScheduledComponent;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataManager extends ActiveMQScheduledComponent {

   // TODO-IMPORTANT configure this
   private static final int NUMBER_OF_CONNECTIONS = 10;

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   final DatabaseStorageConfiguration databaseConfiguration;
   final JDBCConnectionProvider connectionProvider;
   final int batchSize;
   final Executor executorService;

   List<DataWorker> allWorkers;
   LinkedBlockingDeque<DataWorker> workers;

   ArrayList<DBData> pendingData = new ArrayList<>();

   public MessageReferenceData newReferenceTask(long messageID, long queueID, Long txID, IOCompletion context) {
      return new MessageReferenceData(messageID, queueID, txID, context);
   }

   public MessageData newMessageTask(Message message, Long txID, IOCompletion context) {
      return new MessageData(message, txID, context);
   }


   public DataManager(ScheduledExecutorService scheduledExecutorService,
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
         DataWorker worker =  new DataWorker(this, connectionProvider, databaseConfiguration, batchSize, "worker " + i);
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

   public void storeTX(StorageTX storageTX) {
      flushData(castTX(storageTX).dataList);
   }

   private void flushData(List<DBData> dbData) {
      synchronized (this) {
         pendingData.addAll(dbData);
      }
      delay();
   }

   private void flushData(DBData dbData) {
      synchronized (this) {
         pendingData.add(dbData);
      }
      delay();
   }

   private ParallelDBStoreTX castTX(StorageTX storageTX) {
      return (ParallelDBStoreTX) storageTX;
   }

   public void storeMessage(StorageTX storageTX, Message message, Long tx, IOCompletion callback) {
      castTX(storageTX).addData(new MessageData(message, tx, callback));
   }

   public void storeMessage(Message message, Long tx, IOCompletion callback) {
      flushData(new MessageData(message, tx, callback));
   }

   public void deleteMessage(long messageID, IOCompletion callback) {
      flushData(new DeleteMessageData(messageID, callback));
   }

   public void ackMessage(long queueID, long messageID, IOCompletion callback) {
      flushData(new DeleteReferenceData(queueID, messageID, callback));
   }

   public void ackMessage(StorageTX storageTX, long txID, long queueID, long messageID, IOCompletion callback) {
      castTX(storageTX).addData(new DeleteReferenceData(queueID, messageID, callback));
   }

   public void storeReference(StorageTX storageTX, long messageID, long queueID, Long txID, IOCompletion callback) {
      castTX(storageTX).addData(new MessageReferenceData(messageID, queueID, txID, callback));
   }

   public void storeReference(long messageID, long queueID, Long txID, IOCompletion callback) {
      flushData(new MessageReferenceData(messageID, queueID, txID, callback));
   }

   private List<DBData> extractTaskList() {
      ArrayList<DBData> tasksToRun;
      synchronized (this) {
         tasksToRun = new ArrayList<>(pendingData);
         pendingData.clear();
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

   public void workerDone(DataWorker worker) {
      this.workers.offer(worker);
   }

   public void flush() {
      DataWorker worker = workers.poll();
      if (worker == null) {
         logger.info("nothing...");
         this.delay();
         return;
      }

      List<DBData> dataList = extractTaskList();
      worker.setTaskList(dataList);

      executorService.execute(worker);
   }
}
