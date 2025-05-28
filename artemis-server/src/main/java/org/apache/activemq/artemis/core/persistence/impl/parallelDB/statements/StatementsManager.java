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
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.server.ActiveMQScheduledComponent;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatementsManager extends ActiveMQScheduledComponent {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   final DatabaseStorageConfiguration databaseConfiguration;
   final JDBCConnectionProvider connectionProvider;
   final int batchSize;

   Connection connection;
   MessageStatement messageStatement;
   ReferencesStatement referencesStatement;
   UpdateTXStatement txMessagesStatement;
   UpdateTXStatement txReferencesStatement;

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

   abstract class Task {
      final IOCallback context;
      Task(IOCallback context) {
         this.context = context;
      }
      public abstract void store();
   }

   public class MessageTask extends Task {
      public MessageTask(Message message, Long tx, IOCallback context) {
         super(context);
         this.message = message;
         this.tx = tx;
      }

      final Message message;
      final Long tx;

      public void store() {
         messageStatement.addData(this, context);
      }

      @Override
      public String toString() {
         return "MessageTask{" + "message=" + message + ", tx=" + tx + '}';
      }
   }

   public class TXTask extends Task {
      long txID;
      boolean messages;
      boolean references;

      public TXTask(long txID, boolean messages, boolean references, IOCallback context) {
         super(context);
         this.txID = txID;
         this.messages = messages;
         this.references = references;
      }

      public void store() {
         if (messages) {
            txMessagesStatement.addData(this, context);
         }

         if (references) {
            txReferencesStatement.addData(this, context);
         }
      }

      @Override
      public String toString() {
         return "TXTask{" + "txID=" + txID + ", messages=" + messages + ", references=" + references + '}';
      }
   }


   public class MessageReferenceTask extends Task {
      long messageID;
      long queueID;
      Long txID;
      public MessageReferenceTask(long messageID, long queueID, Long txID, IOCallback context) {
         super(context);
         this.messageID = messageID;
         this.queueID = queueID;
         this.txID = txID;
      }

      public void store() {
         referencesStatement.addData(this, context);
      }

      @Override
      public String toString() {
         return "MessageReferenceTask{" + "messageID=" + messageID + ", queueID=" + queueID + ", txID=" + txID + '}';
      }
   }

   public MessageReferenceTask newReferenceTask(long messageID, long queueID, Long txID, IOCallback context) {
      return new MessageReferenceTask(messageID, queueID, txID, context);
   }

   public MessageTask newMessageTask(Message message, Long txID, IOCallback context) {
      return new MessageTask(message, txID, context);
   }


   public StatementsManager(ScheduledExecutorService scheduledExecutorService,
                            Executor executor,
                            long flushTime,
                            DatabaseStorageConfiguration databaseConfiguration,
                            JDBCConnectionProvider connectionProvider,
                            int batchSize) throws SQLException {
      super(scheduledExecutorService, executor, 100, 100, TimeUnit.MILLISECONDS, true);

      logger.info("FlushTime {}", flushTime);
      this.databaseConfiguration = databaseConfiguration;
      this.connectionProvider = connectionProvider;
      this.batchSize = batchSize;
      init();
   }

   public void init() throws SQLException {
      connection = connectionProvider.getConnection();
      connection.setAutoCommit(false);
      messageStatement = new MessageStatement(connection, connectionProvider, databaseConfiguration.getParallelDBMessages(), batchSize);
      referencesStatement = new ReferencesStatement(connection, connectionProvider, databaseConfiguration.getParallelDBReferences(), batchSize);
      txMessagesStatement = new UpdateTXStatement(connection, connectionProvider, databaseConfiguration.getParallelDBMessages(), batchSize);
      txReferencesStatement = new UpdateTXStatement(connection, connectionProvider, databaseConfiguration.getParallelDBReferences(), batchSize);
   }

   public void close() throws SQLException {
      connection.close();
   }

   public void storeTX(long txID, boolean messages, boolean references, OperationContext context) {
      getTLTaskList().add(new TXTask(txID, messages, references, context));
   }

   public void storeMessage(Message message, Long tx, OperationContext callback) {
      callback.storeLineUp();
      getTLTaskList().add(new MessageTask(message, tx, callback));
   }

   public void storeReference(long messageID, long queueID, Long txID, OperationContext callback) {
      callback.storeLineUp();
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
      } catch (SQLException e) {
         logger.warn(e.getMessage(), e);
      } catch (Throwable e) {
         logger.warn(e.getMessage(), e);
      }
   }

   public void flush() throws SQLException {
      List<Task> taskList = extractTaskList();
      taskList.forEach(this::doTask);
      try {
         messageStatement.flushPending(false);
         referencesStatement.flushPending(false);
         txReferencesStatement.flushPending(false);
         txMessagesStatement.flushPending(false);
      } catch (SQLException e) {
         try {
            connection.rollback();
         } catch (Throwable ignored) {
         }
         throw e;
      }
      connection.commit();
      messageStatement.confirmData();
      referencesStatement.confirmData();
      txReferencesStatement.confirmData();
      txMessagesStatement.confirmData();
   }

   private void doTask(Task task) {
      task.store();
   }
}
