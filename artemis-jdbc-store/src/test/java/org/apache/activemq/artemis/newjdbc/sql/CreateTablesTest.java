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

package org.apache.activemq.artemis.newjdbc.sql;

import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.activemq.artemis.newjdbc.driver.SyncJDBCDriver;
import org.apache.activemq.artemis.tests.util.ArtemisTestCase;
import org.checkerframework.checker.units.qual.A;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class CreateTablesTest extends ArtemisTestCase {

   @AfterEach
   public void shutdownDerby() {
      try {
         DriverManager.getConnection("jdbc:derby:;shutdown=true");
      } catch (Exception ignored) {
      }
   }

   @Test
   public void testCreate()  throws Exception {
      try (Connection conn = DriverManager.getConnection("jdbc:derby:target/data;create=true")) {
         PropertySQLProvider.Factory factory = new PropertySQLProvider.Factory(PropertySQLProvider.Factory.SQLDialect.GENERIC);
         SQLProvider provider = factory.create();
         provider.createTables(conn);
      }
   }


}
