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
package org.apache.activemq.artemis.jdbc.store.sql;

import org.apache.activemq.artemis.tests.util.ArtemisTestCase;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

public class PropertySQLProviderTest extends ArtemisTestCase {

   @Test
   public void testGetProperty() {
      PropertySQLProvider factory = (PropertySQLProvider) new PropertySQLProvider.Factory((PropertySQLProvider.Factory.SQLDialect) null).create();
      factory.sql("create-file-table");
   }

   @Test
   public void testGetMissingProperty() {
      PropertySQLProvider factory = (PropertySQLProvider) new PropertySQLProvider.Factory((PropertySQLProvider.Factory.SQLDialect) null).create();
      try {
         factory.sql("missing-property");
         fail();
      } catch (IllegalStateException e) {
         // expected
      }
   }

   @Test
   public void testGetMissingPropertyNoCheck() {
      PropertySQLProvider factory = (PropertySQLProvider) new PropertySQLProvider.Factory((PropertySQLProvider.Factory.SQLDialect) null).create();
      assertNull(factory.sql("missing-property", false));
   }
}
