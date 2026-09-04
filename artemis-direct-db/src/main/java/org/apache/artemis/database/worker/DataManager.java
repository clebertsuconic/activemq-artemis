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

package org.apache.artemis.database.worker;

import java.lang.invoke.MethodHandles;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.journal.IOCompletion;
import org.apache.activemq.artemis.core.journal.StorageTX;
import org.apache.activemq.artemis.core.server.ActiveMQScheduledComponent;
import org.apache.artemis.database.DatabaseProvider;
import org.apache.artemis.database.DatabaseStoreTX;
import org.apache.artemis.database.data.AddressData;
import org.apache.artemis.database.data.DBData;
import org.apache.artemis.database.data.DeleteAddressData;
import org.apache.artemis.database.data.DeleteAllPageRefData;
import org.apache.artemis.database.data.DeleteGenericData;
import org.apache.artemis.database.data.DeleteMessageData;
import org.apache.artemis.database.data.DeletePageData;
import org.apache.artemis.database.data.DeletePageRefData;
import org.apache.artemis.database.data.DeleteQueueData;
import org.apache.artemis.database.data.DeleteReferenceData;
import org.apache.artemis.database.data.GenericData;
import org.apache.artemis.database.data.MessageData;
import org.apache.artemis.database.data.MessageReferenceData;
import org.apache.artemis.database.data.PageData;
import org.apache.artemis.database.data.PageRefData;
import org.apache.artemis.database.data.QueueData;
import org.apache.artemis.database.data.TXDone;
import org.apache.artemis.database.data.UpdateGenericData;
import org.apache.artemis.database.data.UpdateQueueData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataManager extends ActiveMQScheduledComponent {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   final DatabaseProvider databaseProvider;
   final int batchSize;
   final IntSupplier maxRetriesSupplier;
   final LongSupplier retryIntervalMillisSupplier;
   final Consumer<Throwable> criticalErrorListener;
   final Executor executorService;

   List<DataWorker> allWorkers;
   ConcurrentLinkedQueue<DataWorker> workers;
   final ConcurrentLinkedQueue<QueryInterceptor> scheduledQueries = new ConcurrentLinkedQueue<>();

   final ArrayList<DBData> pendingData = new ArrayList<>();

   public MessageReferenceData newReferenceTask(long messageID,
                                                long queueID,
                                                boolean pendingDelivery,
                                                Long txID,
                                                IOCompletion context) {
      return new MessageReferenceData(messageID, queueID, pendingDelivery, txID, context);
   }

   public MessageData newMessageTask(long messageID,
                                     Supplier<ActiveMQBuffer> messageBufferSupplier,
                                     Long txID,
                                     IOCompletion context) {
      return new MessageData(messageID, messageBufferSupplier, txID, context);
   }

   public DataManager(ScheduledExecutorService scheduledExecutorService,
                      Executor executor,
                      Executor executorService,
                      long flushTimeNanos,
                      DatabaseProvider databaseProvider,
                      int batchSize,
                      int numberOfConnections,
                      IntSupplier maxRetriesSupplier,
                      LongSupplier retryIntervalMillisSupplier,
                      Consumer<Throwable> criticalErrorListener) throws SQLException {
      super(scheduledExecutorService, executor, 0, flushTimeNanos, TimeUnit.NANOSECONDS, true);

      this.maxRetriesSupplier = maxRetriesSupplier;
      this.retryIntervalMillisSupplier = retryIntervalMillisSupplier;
      this.criticalErrorListener = criticalErrorListener;
      allWorkers = new ArrayList<>();
      workers = new ConcurrentLinkedQueue<>();
      for (int i = 0; i < numberOfConnections; i++) {
         DataWorker worker = new DataWorker(this, databaseProvider, batchSize, "worker " + i);
         allWorkers.add(worker);
         workers.offer(worker);
      }

      this.executorService = executorService;

      logger.info("FlushTime {}", flushTimeNanos);
      this.databaseProvider = databaseProvider;
      this.batchSize = batchSize;
      init();
   }

   public void init() throws SQLException {
   }

   public void close() {
      allWorkers.forEach(DataWorker::close);
      allWorkers.clear();
      workers.clear();
   }

   public int getMaxRetries() {
      return maxRetriesSupplier.getAsInt();
   }

   public long getRetryIntervalMillis() {
      return retryIntervalMillisSupplier.getAsLong();
   }

   public void criticalError(Throwable error) {
      criticalErrorListener.accept(error);
   }

   public void storeTX(StorageTX storageTX) {
      flushData(castTX(storageTX).dataList, storageTX);
   }

   private void flushData(List<DBData> dbData, StorageTX storageTX) {
      synchronized (pendingData) {
         dbData.forEach(DBData::lineUp);
         pendingData.addAll(dbData);
         pendingData.add(new TXDone((DatabaseStoreTX) storageTX));
      }
      delay();
   }

   private void flushData(List<DBData> dbData) {
      synchronized (pendingData) {
         dbData.forEach(DBData::lineUp);
         pendingData.addAll(dbData);
      }
      delay();
   }

   private void flushData(DBData dbData) {
      synchronized (pendingData) {
         dbData.lineUp();
         pendingData.add(dbData);
      }
      delay();
   }

   private DatabaseStoreTX castTX(StorageTX storageTX) {
      return (DatabaseStoreTX) storageTX;
   }

   public void storeMessage(StorageTX storageTX,
                            long messageID,
                            Supplier<ActiveMQBuffer> messageBufferSupplier,
                            Long tx,
                            IOCompletion callback) {
      castTX(storageTX).addData(new MessageData(messageID, messageBufferSupplier, tx, callback));
   }

   public void storeMessage(long messageID,
                            Supplier<ActiveMQBuffer> messageBufferSupplier,
                            Long tx,
                            IOCompletion callback) {
      flushData(new MessageData(messageID, messageBufferSupplier, tx, callback));
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

   public void storeReference(StorageTX storageTX,
                              long messageID,
                              long queueID,
                              boolean pendingDelivery,
                              Long txID,
                              IOCompletion callback) {
      castTX(storageTX).addData(new MessageReferenceData(messageID, queueID, pendingDelivery, txID, callback));
   }

   public void storeQueue(StorageTX storageTX,
                          long addressId,
                          long id,
                          String name,
                          String filter,
                          RoutingType routingType,
                          String queueConfigJson,
                          IOCompletion callback) {

      castTX(storageTX).addData(new QueueData(addressId, id, name, filter, routingType == RoutingType.MULTICAST, routingType == RoutingType.ANYCAST, queueConfigJson, callback));
   }

   public void updateQueue(StorageTX storageTX,
                           long addressId,
                           long id,
                           String name,
                           String filter,
                           RoutingType routingType,
                           String queueConfigJson,
                           IOCompletion callback) {
      castTX(storageTX).addData(new UpdateQueueData(addressId, id, name, filter, routingType == RoutingType.MULTICAST, routingType == RoutingType.ANYCAST, queueConfigJson, callback));
   }

   public void deleteQueue(StorageTX storageTX, long queueId, IOCompletion callback) {
      castTX(storageTX).addData(new DeleteQueueData(queueId, callback));
   }

   public void deleteQueue(long queueId, IOCompletion callback) {
      flushData(new DeleteQueueData(queueId, callback));
   }

   public void storeReference(long messageID, long queueID, boolean pendingDelivery, Long txID, IOCompletion callback) {
      flushData(new MessageReferenceData(messageID, queueID, pendingDelivery, txID, callback));
   }

   public void deleteAddress(StorageTX storageTX, long addressId, IOCompletion callback) {
      castTX(storageTX).addData(new DeleteAddressData(addressId, callback));
   }

   public void deleteAddress(long addressId, IOCompletion callback) {
      flushData(new DeleteAddressData(addressId, callback));
   }

   public void storeAddressInfo(StorageTX storageTX,
                                long id,
                                String address,
                                boolean isMulticast,
                                boolean isAnycast,
                                IOCompletion callback) {
      castTX(storageTX).addData(new AddressData(id, address, isMulticast, isAnycast, callback));
   }

   public void storePage(StorageTX storageTX,
                         long addressID,
                         long pageID,
                         long pageNR,
                         long messageID,
                         Supplier<ActiveMQBuffer> messageBufferSupplier,
                         Long txID,
                         IOCompletion callback) {
      castTX(storageTX).addData(new PageData(addressID, pageID, pageNR, messageID, messageBufferSupplier, txID, callback));
   }

   public void storePage(long addressID,
                         long pageID,
                         long pageNR,
                         long messageID,
                         Supplier<ActiveMQBuffer> messageBufferSupplier,
                         Long txID,
                         IOCompletion callback) {
      flushData(new PageData(addressID, pageID, pageNR, messageID, messageBufferSupplier, txID, callback));
   }

   public void deletePage(long addressID, long pageID, IOCompletion callback) {
      flushData(new DeletePageData(addressID, pageID, callback));
   }

   public void storePageRef(StorageTX storageTX,
                            long addressID,
                            long pageID,
                            long pageNR,
                            long queueID,
                            IOCompletion callback) {
      castTX(storageTX).addData(new PageRefData(addressID, pageID, pageNR, queueID, callback));
   }

   public void storePageRef(long addressID, long pageID, long pageNR, long queueID, IOCompletion callback) {
      flushData(new PageRefData(addressID, pageID, pageNR, queueID, callback));
   }

   public void deletePageRef(long addressID, long pageID, long pageNR, long queueID, IOCompletion callback) {
      flushData(new DeletePageRefData(addressID, pageID, pageNR, queueID, callback));
   }

   public void deletePageReferences(StorageTX storageTX, long addressID, long pageID, IOCompletion callback) {
      castTX(storageTX).addData(new DeleteAllPageRefData(addressID, pageID, callback));
   }

   public void deletePageReferences(long addressID, long pageID, IOCompletion callback) {
      flushData(new DeleteAllPageRefData(addressID, pageID, callback));
   }

   public void storeGenericData(long id,
                                byte recordType,
                                Long txId,
                                Supplier<ActiveMQBuffer> dataSupplier,
                                IOCompletion callback) {
      flushData(new GenericData(id, recordType, txId, dataSupplier, callback));
   }

   public void storeGenericData(StorageTX storageTX,
                                long id,
                                byte recordType,
                                Long txId,
                                Supplier<ActiveMQBuffer> dataSupplier,
                                IOCompletion callback) {
      castTX(storageTX).addData(new GenericData(id, recordType, txId, dataSupplier, callback));
   }

   public void updateGenericData(long id, Long txId, Supplier<ActiveMQBuffer> dataSupplier, IOCompletion callback) {
      flushData(new UpdateGenericData(id, txId, dataSupplier, callback));
   }

   public void updateGenericData(StorageTX storageTX,
                                 long id,
                                 Long txId,
                                 Supplier<ActiveMQBuffer> dataSupplier,
                                 IOCompletion callback) {
      castTX(storageTX).addData(new UpdateGenericData(id, txId, dataSupplier, callback));
   }

   public void deleteGenericData(long id, IOCompletion callback) {
      flushData(new DeleteGenericData(id, callback));
   }

   public void deleteGenericData(StorageTX storageTX, long id, IOCompletion callback) {
      castTX(storageTX).addData(new DeleteGenericData(id, callback));
   }

   public void storeBindingsGenericData(long id,
                                        byte recordType,
                                        Long txId,
                                        Supplier<ActiveMQBuffer> dataSupplier,
                                        IOCompletion callback) {
      flushData(new GenericData(id, recordType, txId, dataSupplier, true, callback));
   }

   public void storeBindingsGenericData(StorageTX storageTX,
                                        long id,
                                        byte recordType,
                                        Long txId,
                                        Supplier<ActiveMQBuffer> dataSupplier,
                                        IOCompletion callback) {
      castTX(storageTX).addData(new GenericData(id, recordType, txId, dataSupplier, true, callback));
   }

   public void updateBindingsGenericData(long id,
                                         Long txId,
                                         Supplier<ActiveMQBuffer> dataSupplier,
                                         IOCompletion callback) {
      flushData(new UpdateGenericData(id, txId, dataSupplier, true, callback));
   }

   public void updateBindingsGenericData(StorageTX storageTX,
                                         long id,
                                         Long txId,
                                         Supplier<ActiveMQBuffer> dataSupplier,
                                         IOCompletion callback) {
      castTX(storageTX).addData(new UpdateGenericData(id, txId, dataSupplier, true, callback));
   }

   public void deleteBindingsGenericData(long id, IOCompletion callback) {
      flushData(new DeleteGenericData(id, true, callback));
   }

   public void deleteBindingsGenericData(StorageTX storageTX, long id, IOCompletion callback) {
      castTX(storageTX).addData(new DeleteGenericData(id, true, callback));
   }

   private List<DBData> extractTaskList() {
      ArrayList<DBData> tasksToRun;
      synchronized (pendingData) {
         if (pendingData.isEmpty()) {
            return null;
         }
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
      }
   }

   public void workerDone(DataWorker worker) {
      this.workers.offer(worker);
   }

   public void flush() {
      DataWorker worker = workers.poll();
      if (worker == null) {
         this.delay();
         return;
      }

      List<DBData> dataList = extractTaskList();
      if (dataList != null && !dataList.isEmpty()) {
         logger.info("Extracted dataList with {} elements", dataList.size());
         worker.setTaskList(dataList);
         executorService.execute(worker);
      } else {
         workerDone(worker);
      }

      QueryInterceptor queryInterceptor;
      while ((queryInterceptor = scheduledQueries.poll()) != null) {
         if (!dispatchQuery(queryInterceptor)) {
            return;
         }
      }
   }

   public void executeQuery(Executor targetExecutor, SQLConsumer<DataWorker> consumer, Runnable afterCommit) {
      dispatchQuery(new QueryInterceptor(targetExecutor, consumer, afterCommit));
   }

   private boolean dispatchQuery(QueryInterceptor workerInterceptor) {
      DataWorker worker = workers.poll();
      if (worker != null) {
         workerInterceptor.setWorker(worker);
         workerInterceptor.getExecutor().execute(workerInterceptor);
         return true;
      } else {
         scheduledQueries.offer(workerInterceptor);
         delay();
         return false;
      }
   }

   private class QueryInterceptor implements Runnable {

      final SQLConsumer<DataWorker> consumer;
      final Executor executor;
      final Runnable afterCommit;
      DataWorker worker;

      public Executor getExecutor() {
         return executor;
      }

      public QueryInterceptor setWorker(DataWorker worker) {
         this.worker = worker;
         return this;
      }

      QueryInterceptor(Executor executor, SQLConsumer<DataWorker> consumer, Runnable afterCommit) {
         this.consumer = consumer;
         this.executor = executor;
         this.afterCommit = afterCommit;
      }

      @Override
      public void run() {
         try {
            SQLException retryException = worker.executeWithRetry(consumer);
            if (retryException != null) {
               criticalError(retryException);
               return;
            }
            if (afterCommit != null) {
               try {
                  worker.connection.commit();
               } catch (SQLException e) {
                  criticalError(e);
                  return;
               }
               afterCommit.run();
            }
         } finally {
            workerDone(worker);
         }
      }
   }

}
