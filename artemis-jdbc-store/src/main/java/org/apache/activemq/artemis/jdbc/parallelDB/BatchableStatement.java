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

package org.apache.activemq.artemis.jdbc.parallelDB;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCConnectionProvider;
import org.apache.activemq.artemis.utils.ActiveMQBufferInputStream;

public abstract class BatchableStatement<E> {

   final JDBCConnectionProvider connectionProvider;
   final Connection connection;
   final String statement;
   protected PreparedStatement preparedStatement;

   List<E> pendingList;
   List<IOCallback> callbacks;

   public BatchableStatement(JDBCConnectionProvider connectionProvider, Connection connection, String statement, int expectedSize) throws SQLException {
      this.connectionProvider = connectionProvider;
      this.connection = connection;
      this.connection.prepareStatement(statement);
      this.pendingList = new ArrayList<>(expectedSize);
      this.callbacks = new ArrayList<>(expectedSize);
      this.statement = statement;
      init();
   }

   protected void init() throws SQLException {
      this.preparedStatement = connection.prepareStatement(statement);
   }

   public void addData(E element, IOCallback callback) {
      pendingList.add(element);
      callbacks.add(callback);
   }

   public void flushPending(boolean commit) throws SQLException {
      pendingList.forEach(this::flushOne);
      try {
         preparedStatement.executeBatch();
      } catch (SQLException e) {
         callbacks.forEach(c -> c.onError(-1, e.getMessage()));
         if (commit) {
            try {
               connection.rollback();
            } catch (Throwable ignored) {
            }
         }
         pendingList.clear();
         callbacks.clear();
         throw e;
      }
      if (commit) {
         connection.commit();
      }
      callbacks.forEach(this::okCallback);

      pendingList.clear();
      callbacks.clear();
   }

   private void okCallback(IOCallback callback) {
      callback.done();
   }

   private void errorCallback(IOCallback callback, int errorCode, String message) {
      callback.onError(errorCode, message);
   }


   protected void flushOne(E element) {
      try {
         doOne(element);
         preparedStatement.addBatch();
      } catch (Exception e) {
         throw new RuntimeException(e.getMessage(), e);
      }
   }

   public ActiveMQBuffer getPersistedBuffer(Persister persister, Object record) {
      int size = persister.getEncodeSize(record);
      ActiveMQBuffer encodedBuffer = ActiveMQBuffers.fixedBuffer(size);
      persister.encode(encodedBuffer, record);
      return encodedBuffer;
   }

   protected static ActiveMQBufferInputStream blobInputStream(ActiveMQBuffer buffer) {
      return new ActiveMQBufferInputStream(buffer);
   }

   protected abstract void doOne(E element) throws Exception;

}
