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
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCConnectionProvider;

public class StatementsManager {

   final DatabaseStorageConfiguration databaseConfiguration;
   final JDBCConnectionProvider connectionProvider;
   final int batchSize;

   Connection connection;
   MessageStatement messageStatement;

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
