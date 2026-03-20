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

package org.apache.artemis.database.statements;

import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.artemis.database.data.PageRefData;
import org.apache.artemis.database.DatabaseProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InsertPageRefStatement extends BatchableStatement<PageRefData> {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public InsertPageRefStatement(DatabaseProvider databaseProvider, Connection connection, int expectedSize) throws SQLException {
      super(databaseProvider, connection, getSQL(databaseProvider), expectedSize);
   }

   private static String getSQL(DatabaseProvider databaseProvider) {
      String tableName = databaseProvider.getSqlProvider().getPageRefs();
      return databaseProvider.getSqlProvider().insertPageReferences(tableName);
   }

   @Override
   protected void doOne(PageRefData task) throws Exception {
      preparedStatement.setLong(1, task.addressID);
      preparedStatement.setLong(2, task.pageID);
      preparedStatement.setLong(3, task.pageNR);
      preparedStatement.setLong(4, task.queueID);
   }
}
