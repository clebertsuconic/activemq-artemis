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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCConnectionProvider;

public class StatementsManager {

   final DatabaseStorageConfiguration databaseConfiguration;
   final JDBCConnectionProvider connectionProvider;
   final int batchSize;

   Connection connection;
   MessageStatement messageStatement;
   ReferencesTXStatement referencesTXStatement;
   ReferencesStatement referencesStatement;

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
      final OperationContext context;
      Task(OperationContext context) {
         this.context = context;
      }
      public abstract void store();
   }

   class MessageTask extends Task {
      public MessageTask(Message message, OperationContext context) {
         super(context);
         this.message = message;
      }

      final Message message;
      public void store() {
         messageStatement.addData(message, context);
      }
   }

   class MessageReferenceTask extends Task {
      public MessageReferenceTask(MessageReference reference, OperationContext context) {
         super(context);
         this.reference = reference;
      }

      final MessageReference reference;
      public void store() {
         referencesStatement.addData(reference, context);
      }
   }

   class MessageReferenceTXTask extends Task {
      public MessageReferenceTXTask(MessageReference reference, long txID, OperationContext context) {
         super(context);
         this.reference = ReferencesTXStatement.withTX(reference, txID);
      }

      final ReferencesTXStatement.ReferenceWithTX reference;
      public void store() {
         referencesTXStatement.addData(reference, context);
      }
   }



   public StatementsManager(DatabaseStorageConfiguration databaseConfiguration, JDBCConnectionProvider connectionProvider, int batchSize) throws SQLException {
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
      referencesTXStatement = new ReferencesTXStatement(connection, connectionProvider, databaseConfiguration.getParallelDBReferences(), batchSize);
   }

   public void close() throws SQLException {
      connection.close();
   }

   public void storeMessage(Message message, OperationContext callback) {
      callback.storeLineUp();
      synchronized (this) {
         pendingTasks.add(new MessageTask(message, callback));
      }
   }

   public void storeReference(MessageReference reference, OperationContext callback) {
      callback.storeLineUp();
      synchronized (this) {
         pendingTasks.add(new MessageReferenceTask(reference, callback));
      }
   }

   public void storeReferenceTX(MessageReference reference, long txID, OperationContext callback) {
      callback.storeLineUp();
      synchronized (this) {
         pendingTasks.add(new MessageReferenceTXTask(reference, txID, callback));
      }
   }

   private List<Task> extractTaskList() {
      ArrayList<Task> tasksToRun;
      synchronized (this) {
         tasksToRun = new ArrayList<>(pendingTasks);
         pendingTasks.clear();
      }
      return tasksToRun;
   }

   public void flush() throws SQLException {
      List<Task> taskList = extractTaskList();
      taskList.forEach(this::doTask);
      try {
         messageStatement.flushPending(false);
         referencesTXStatement.flushPending(false);
         referencesStatement.flushPending(false);
      } catch (SQLException e) {
         try {
            connection.rollback();
         } catch (Throwable ignored) {
         }
         throw e;
      }
      connection.commit();
   }

   private void doTask(Task task) {
      task.store();
   }
}
