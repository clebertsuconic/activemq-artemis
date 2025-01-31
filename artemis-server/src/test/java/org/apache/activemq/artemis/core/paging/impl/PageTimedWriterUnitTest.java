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

package org.apache.activemq.artemis.core.paging.impl;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.io.OperationConsistencyLevel;
import org.apache.activemq.artemis.core.journal.Journal;
import org.apache.activemq.artemis.core.journal.impl.JournalImpl;
import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalStorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalStorageManagerAccessor;
import org.apache.activemq.artemis.core.persistence.impl.journal.OperationContextImpl;
import org.apache.activemq.artemis.core.replication.ReplicationManager;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.core.server.RouteContextList;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.TransactionOperationAbstract;
import org.apache.activemq.artemis.core.transaction.impl.TransactionImpl;
import org.apache.activemq.artemis.tests.util.ArtemisTestCase;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.apache.activemq.artemis.utils.actors.OrderedExecutorFactory;
import org.apache.activemq.artemis.utils.critical.CriticalAnalyzer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PageTimedWriterUnitTest extends ArtemisTestCase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   ScheduledExecutorService scheduledExecutorService;
   ExecutorService executorService;
   OrderedExecutorFactory executorFactory;
   OperationContext context;

   // almost real as we use an extension that can allow mocking internal objects such as journals and replicationManager
   JournalStorageManager realJournalStorageManager;

   PagingStoreImpl mockPageStore;

   CountDownLatch allowRunning;

   PageTimedWriter timer;

   Configuration configuration;

   Journal mockBindingsJournal;
   Journal mockMessageJournal;

   AtomicBoolean useReplication = new AtomicBoolean(false);
   AtomicBoolean returnSynchronizing = new AtomicBoolean(false);
   ReplicationManager mockReplicationManager;


   class MockableJournalStorageManager extends JournalStorageManager {

      public MockableJournalStorageManager(Configuration config,
                                           Journal bindingsJournal,
                                           Journal messagesJournal,
                                           ExecutorFactory executorFactory,
                                           ExecutorFactory ioExecutors) {
         super(config, Mockito.mock(CriticalAnalyzer.class), executorFactory, ioExecutors);
         this.bindingsJournal = bindingsJournal;
         this.messageJournal = messagesJournal;
      }

      @Override
      public void start() throws Exception {
         super.start();
         idGenerator.forceNextID(1);
      }

      @Override
      protected void createDirectories() {
         // not creating any folders
      }
   }


   @BeforeEach
   public void setupMocks() throws Exception {
      configuration = new ConfigurationImpl();
      configuration.setJournalType(JournalType.NIO);
      scheduledExecutorService = Executors.newScheduledThreadPool(10);
      executorService = Executors.newFixedThreadPool(10);
      runAfter(scheduledExecutorService::shutdownNow);
      runAfter(executorService::shutdownNow);
      runAfter(() -> OperationContextImpl.clearContext());
      executorFactory = new OrderedExecutorFactory(executorService);
      context = OperationContextImpl.getContext(executorFactory);
      assertNotNull(context);

      mockBindingsJournal = Mockito.mock(Journal.class);
      mockMessageJournal = Mockito.mock(Journal.class);

      mockReplicationManager = Mockito.mock(ReplicationManager.class);
      Mockito.when(mockReplicationManager.isStarted()).thenAnswer(a -> useReplication.get());
      Mockito.when(mockReplicationManager.isSynchronizing()).thenAnswer(a -> returnSynchronizing.get());
      Mockito.doAnswer(a -> {
         if (useReplication.get()) {
            OperationContext ctx = OperationContextImpl.getContext();
            if (ctx != null) {
               ctx.replicationDone();
            }
         }
         return null;
      }).when(mockReplicationManager).pageWrite(Mockito.any(SimpleString.class), Mockito.any(PagedMessage.class), Mockito.anyLong(), Mockito.anyBoolean());

      realJournalStorageManager = new MockableJournalStorageManager(configuration, mockBindingsJournal, mockMessageJournal, executorFactory, executorFactory);
      realJournalStorageManager.start();

      JournalStorageManagerAccessor.setReplicationManager(realJournalStorageManager, mockReplicationManager);

      allowRunning = new CountDownLatch(1);

      mockPageStore = Mockito.mock(PagingStoreImpl.class);
      Mockito.doAnswer(a -> {
         realJournalStorageManager.pageWrite(SimpleString.of("whatever"), a.getArgument(0), 1L, a.getArgument(1), a.getArgument(2));
         return null;
      }).when(mockPageStore).directWritePage(Mockito.any(PagedMessage.class), Mockito.anyBoolean(), Mockito.anyBoolean());


      timer = new PageTimedWriter(realJournalStorageManager, mockPageStore, scheduledExecutorService, executorFactory.getExecutor(), 100) {
         @Override
         public void run() {
            try {
               allowRunning.await();
            } catch (InterruptedException e) {
               logger.warn(e.getMessage(), e);

            }
            super.run();
         }
      };

      timer.start();

   }

   // a test to validate if the Mocks are correctly setup
   @Test
   public void testValidateMocks() throws Exception {
      TransactionImpl tx = new TransactionImpl(realJournalStorageManager);
      tx.setContainsPersistent();
      AtomicInteger count = new AtomicInteger(0);
      tx.addOperation(new TransactionOperationAbstract() {
         @Override
         public void afterCommit(Transaction tx) {
            count.incrementAndGet();
         }
      });
      assertEquals(0, count.get());
      tx.commit();
      assertEquals(1, count.get(), "tx.commit is not correctly wired on mocking");


      realJournalStorageManager.afterCompleteOperations(new IOCallback() {
         @Override
         public void done() {
            count.incrementAndGet();
         }

         @Override
         public void onError(int errorCode, String errorMessage) {

         }
      });

      realJournalStorageManager.afterCompleteOperations(new IOCallback() {
         @Override
         public void done() {
            count.incrementAndGet();
         }

         @Override
         public void onError(int errorCode, String errorMessage) {

         }
      }, OperationConsistencyLevel.FULL);

      assertEquals(3, count.get(), "afterCompletion is not correctly wired on mocking");

      long id = realJournalStorageManager.generateID();
      long newID = realJournalStorageManager.generateID();
      assertEquals(1L, newID - id);

   }

   @Test
   public void testIOCompletion() throws Exception {
      CountDownLatch latch = new CountDownLatch(1);

      timer.addTask(context, Mockito.mock(PagedMessage.class), null, Mockito.mock(RouteContextList.class));

      context.executeOnCompletion(new IOCallback() {
         @Override
         public void done() {
            latch.countDown();
         }

         @Override
         public void onError(int errorCode, String errorMessage) {
         }
      });

      assertFalse(latch.await(10, TimeUnit.MILLISECONDS));
      allowRunning.countDown();
      assertTrue(latch.await(1, TimeUnit.MINUTES));

   }

   @Test
   public void testIOCompletionWhileReplica() throws Exception {
      CountDownLatch latch = new CountDownLatch(1);

      useReplication.set(true);

      timer.addTask(context, Mockito.mock(PagedMessage.class), null, Mockito.mock(RouteContextList.class));

      context.executeOnCompletion(new IOCallback() {
         @Override
         public void done() {
            latch.countDown();
         }

         @Override
         public void onError(int errorCode, String errorMessage) {
         }
      });

      assertFalse(latch.await(10, TimeUnit.MILLISECONDS));
      allowRunning.countDown();
      assertTrue(latch.await(10, TimeUnit.MINUTES));
   }

   @Test
   public void testTXCompletionWhileReplica() throws Exception {
      CountDownLatch latch = new CountDownLatch(1);

      useReplication.set(true);

      TransactionImpl transaction = new TransactionImpl(realJournalStorageManager);
      transaction.setContainsPersistent();
      transaction.addOperation(new TransactionOperationAbstract() {
         @Override
         public void afterCommit(Transaction tx) {
            super.afterCommit(tx);
            latch.countDown();
         }
      });

      timer.addTask(context, Mockito.mock(PagedMessage.class), transaction, Mockito.mock(RouteContextList.class));

      transaction.commit();

      assertFalse(latch.await(10, TimeUnit.MILLISECONDS));
      allowRunning.countDown();
      assertTrue(latch.await(10, TimeUnit.MINUTES));
   }

   @Test
   public void testTXCompletionWhileDisableReplica() throws Exception {
      CountDownLatch latch = new CountDownLatch(1);

      useReplication.set(true);

      TransactionImpl transaction = new TransactionImpl(realJournalStorageManager);
      transaction.setContainsPersistent();
      transaction.addOperation(new TransactionOperationAbstract() {
         @Override
         public void afterCommit(Transaction tx) {
            super.afterCommit(tx);
            latch.countDown();
         }
      });

      timer.addTask(context, Mockito.mock(PagedMessage.class), transaction, Mockito.mock(RouteContextList.class));

      transaction.commit();

      assertFalse(latch.await(10, TimeUnit.MILLISECONDS));
      useReplication.set(false);
      allowRunning.countDown();
      assertTrue(latch.await(10, TimeUnit.MINUTES));
   }

   // add a task while replicating, process it when no longer replicating (disconnect a node scenario)
   @Test
   public void testDisableReplica() throws Exception {
      CountDownLatch latch = new CountDownLatch(1);

      useReplication.set(true);

      timer.addTask(context, Mockito.mock(PagedMessage.class), null, Mockito.mock(RouteContextList.class));

      context.executeOnCompletion(new IOCallback() {
         @Override
         public void done() {
            latch.countDown();
         }

         @Override
         public void onError(int errorCode, String errorMessage) {
         }
      });

      assertFalse(latch.await(10, TimeUnit.MILLISECONDS));
      useReplication.set(false);
      allowRunning.countDown();
      assertTrue(latch.await(10, TimeUnit.SECONDS));
   }

   // add a task while not replicating, process it when is now replicating (reconnect a node scenario)
   // this is the oppostie from testDisableReplica
   @Test
   public void testEnableReplica() throws Exception {
      CountDownLatch latch = new CountDownLatch(1);

      useReplication.set(false);

      timer.addTask(context, Mockito.mock(PagedMessage.class), null, Mockito.mock(RouteContextList.class));

      context.executeOnCompletion(new IOCallback() {
         @Override
         public void done() {
            latch.countDown();
         }

         @Override
         public void onError(int errorCode, String errorMessage) {
         }
      });

      assertFalse(latch.await(10, TimeUnit.MILLISECONDS));
      useReplication.set(true);
      allowRunning.countDown();
      assertTrue(latch.await(10, TimeUnit.SECONDS));
   }

   @Test
   public void testTXCompletion() throws Exception {
      ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(10);
      ExecutorService executorService = Executors.newFixedThreadPool(10);
      runAfter(scheduledExecutorService::shutdownNow);
      runAfter(executorService::shutdownNow);
      runAfter(() -> OperationContextImpl.clearContext());

      OrderedExecutorFactory executorFactory = new OrderedExecutorFactory(executorService);

      OperationContextImpl.clearContext();

      OperationContext context = OperationContextImpl.getContext(executorFactory);

      CountDownLatch latch = new CountDownLatch(1);

      Transaction tx = new TransactionImpl(realJournalStorageManager, Integer.MAX_VALUE);
      tx.setContainsPersistent();

      timer.addTask(context, Mockito.mock(PagedMessage.class), tx, Mockito.mock(RouteContextList.class));
      tx.addOperation(new TransactionOperationAbstract() {
         @Override
         public void afterCommit(Transaction tx) {
            latch.countDown();
         }
      });
      tx.commit();

      assertFalse(latch.await(10, TimeUnit.MILLISECONDS));

      allowRunning.countDown();

      assertTrue(latch.await(10, TimeUnit.SECONDS));

   }

}
