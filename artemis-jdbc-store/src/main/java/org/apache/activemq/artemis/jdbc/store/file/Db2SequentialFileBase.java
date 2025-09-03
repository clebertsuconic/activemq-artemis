/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.jdbc.store.file;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.activemq.artemis.jdbc.store.drivers.JDBCConnectionProvider;
import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;

@SuppressWarnings("SynchronizeOnNonFinalField")
public final class Db2SequentialFileBase extends JDBCSequentialFileFactoryBase {

   private String replaceLargeObject;

   public Db2SequentialFileBase() {
      super();
   }

   public Db2SequentialFileBase(JDBCConnectionProvider connectionProvider, String tableName) {
      super(connectionProvider, tableName);
   }

   @Override
   protected void prepareStatements() {
      this.deleteFile = sqlProvider.getDeleteFileSQL(tableName);
      this.createFile = sqlProvider.getInsertFileSQL(tableName);
      this.createFileColumnNames = new String[]{"ID"};
      this.selectFileByFileName = sqlProvider.getSelectFileByFileName(tableName);
      this.copyFileRecord = sqlProvider.getCopyFileRecordByIdSQL(tableName);
      this.renameFile = sqlProvider.getUpdateFileNameByIdSQL(tableName);
      this.readLargeObject = sqlProvider.getReadLargeObjectSQL(tableName);
      this.replaceLargeObject = sqlProvider.getReplaceLargeObjectSQL(tableName);
      this.appendToLargeObject = sqlProvider.getAppendToLargeObjectSQL(tableName);
      this.selectFileNamesByExtension = sqlProvider.getSelectFileNamesByExtensionSQL(tableName);
   }

   @Override
   public int writeToFile(JDBCSequentialFile file, byte[] data, boolean append) throws SQLException {
      if (data == null || data.length == 0) {
         return 0;
      }
      try (Connection connection = connectionProvider.getConnection()) {
         try (PreparedStatement largeObjectStatement = connection.prepareStatement(append ? appendToLargeObject : replaceLargeObject)) {
            connection.setAutoCommit(false);
            int bytesWritten;
            largeObjectStatement.setBytes(1, data);
            largeObjectStatement.setLong(2, file.getId());
            final int updatesFiles = largeObjectStatement.executeUpdate();
            assert updatesFiles <= 1;
            connection.commit();
            if (updatesFiles == 0) {
               bytesWritten = 0;
            } else {
               bytesWritten = data.length;
            }
            return bytesWritten;
         } catch (SQLException e) {
            connection.rollback();
            throw e;
         }
      }
   }
}
