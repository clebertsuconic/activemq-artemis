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

package org.apache.activemq.artemis.core.persistence.impl.newdatabase.worker;

import java.lang.invoke.MethodHandles;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.core.persistence.impl.newdatabase.NewDatabaseStoreTX;
import org.apache.activemq.artemis.core.persistence.impl.newdatabase.dbdata.DBData;
import org.apache.activemq.artemis.core.persistence.impl.newdatabase.statements.DeleteAddressStatement;
import org.apache.activemq.artemis.core.persistence.impl.newdatabase.statements.DeleteMessageStatement;
import org.apache.activemq.artemis.core.persistence.impl.newdatabase.statements.DeleteReferenceStatement;
import org.apache.activemq.artemis.core.persistence.impl.newdatabase.statements.InsertAddressStatement;
import org.apache.activemq.artemis.core.persistence.impl.newdatabase.statements.InsertMessageStatement;
import org.apache.activemq.artemis.core.persistence.impl.newdatabase.statements.InsertQueueStatement;
import org.apache.activemq.artemis.core.persistence.impl.newdatabase.statements.InsertReferencesStatement;
import org.apache.activemq.artemis.core.persistence.impl.newdatabase.statements.BatchableStatement;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataWorker extends DataAgent {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public DataWorker(Consumer<DataWorker> onDone, JDBCConnectionProvider connectionProvider, DatabaseStorageConfiguration databaseConfiguration, int batchSize, String name) throws SQLException  {
      super(connectionProvider);
      this.onDone = onDone;
      this.databaseConfiguration = databaseConfiguration;
      this.name = name;
      this.batchSize = batchSize;
      connect();
   }

   private final String name;
   public DatabaseStorageConfiguration databaseConfiguration;
   int batchSize;

   public InsertMessageStatement insertMessageStatement;
   public InsertReferencesStatement insertReferencesStatement;
   public DeleteReferenceStatement deleteReferenceStatement;
   public DeleteMessageStatement deleteMessageStatement;
   public DeleteAddressStatement deleteAddressStatement;
   public InsertAddressStatement insertAddressStatement;
   public InsertQueueStatement insertQueueStatement;
   public ArrayList<NewDatabaseStoreTX> pendingTX;
   // To be called when the worker is done
   private final Consumer<DataWorker> onDone;

   private List<BatchableStatement<?>> batchableStatements = new ArrayList<>();

   @Override
   protected void connect() throws SQLException {
      super.connect();
      insertMessageStatement = new InsertMessageStatement(connection, connectionProvider, databaseConfiguration, batchSize);
      insertReferencesStatement = new InsertReferencesStatement(connection, connectionProvider, databaseConfiguration, batchSize);
      deleteReferenceStatement = new DeleteReferenceStatement(connection, connectionProvider, databaseConfiguration, batchSize);
      deleteMessageStatement = new DeleteMessageStatement(connection, connectionProvider, databaseConfiguration, batchSize);
      deleteAddressStatement = new DeleteAddressStatement(connection, connectionProvider, databaseConfiguration, batchSize);
      insertAddressStatement = new InsertAddressStatement(connection, connectionProvider, databaseConfiguration, batchSize);
      insertQueueStatement = new InsertQueueStatement(connection, connectionProvider, databaseConfiguration, batchSize);
      pendingTX = new ArrayList<>();
   }

   List<DBData> dataList;

   public void setTaskList(List<DBData> dataList) {
      this.dataList = dataList;
   }

   @Override
   protected void doCleanup() {
      this.dataList = null;
      insertMessageStatement.clear();
      insertReferencesStatement.clear();
      deleteReferenceStatement.clear();
      deleteMessageStatement.clear();
      deleteAddressStatement.clear();
      insertAddressStatement.clear();
      insertQueueStatement.clear();
      pendingTX.clear();
      onDone.accept(this);
   }

   @Override
   protected void doBeforeCommit() throws SQLException {
      logger.info("Worker {} running with {} tasks", name, dataList.size());
      dataList.forEach(this::doStore);
      insertMessageStatement.flushPending(false);
      insertReferencesStatement.flushPending(false);
      deleteReferenceStatement.flushPending(false);
      deleteMessageStatement.flushPending(false);
      deleteAddressStatement.flushPending(false);
      insertAddressStatement.flushPending(false);
      insertQueueStatement.flushPending(false);
   }

   @Override
   protected void doAfterCommit() {
      insertMessageStatement.confirmData();
      insertReferencesStatement.confirmData();
      deleteReferenceStatement.confirmData();
      deleteMessageStatement.confirmData();
      insertReferencesStatement.confirmData();
      deleteAddressStatement.confirmData();
      insertAddressStatement.confirmData();
      insertQueueStatement.confirmData();
      pendingTX.forEach(NewDatabaseStoreTX::completeIO);
   }

   @Override
   protected void doError(Exception exception) {
      insertMessageStatement.onError(exception);
      insertReferencesStatement.onError(exception);
      deleteReferenceStatement.onError(exception);
      deleteMessageStatement.onError(exception);
      insertReferencesStatement.onError(exception);
      deleteAddressStatement.onError(exception);
      insertAddressStatement.onError(exception);
      insertQueueStatement.onError(exception);
      // TODO: Critical Error
   }

   public void doStore(DBData data) {
      data.store(this);
   }

   public void close() {
      try {
         connection.close();
      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
      }
   }

}
