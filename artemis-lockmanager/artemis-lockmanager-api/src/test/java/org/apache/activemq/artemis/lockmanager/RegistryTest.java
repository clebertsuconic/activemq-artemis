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

package org.apache.activemq.artemis.lockmanager;

import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RegistryTest {


   @Test
   public void testRegistryUnregister() {
      Registry.getInstance().register(new FakeDistributedLockManagerFactory());
      assertInstanceOf(FakeDistributedLockManagerFactory.class, Registry.getInstance().getFactory("fake"));
      assertInstanceOf(FakeDistributedLockManagerFactory.class, Registry.getInstance().getFactoryWithClassName("Fake"));
      Registry.getInstance().unregisterWithType("fake");
      assertNull(Registry.getInstance().getFactory("fake"));
      assertThrows(IllegalArgumentException.class, () -> Registry.getInstance().getFactoryWithClassName("Fake"));
      Registry.getInstance().register(new FakeDistributedLockManagerFactory());
      assertInstanceOf(FakeDistributedLockManagerFactory.class, Registry.getInstance().getFactory("fake"));
      assertInstanceOf(FakeDistributedLockManagerFactory.class, Registry.getInstance().getFactoryWithClassName("Fake"));
      Registry.getInstance().unregisterWithClassName("Fake");
      assertNull(Registry.getInstance().getFactory("fake"));
      assertNull(Registry.getInstance().getFactory("Fake"));
      assertThrows(IllegalArgumentException.class, () -> Registry.getInstance().getFactoryWithClassName("Fake"));
      assertDoesNotThrow(() -> Registry.getInstance().unregisterWithType("dontExist"));
      assertDoesNotThrow(() -> Registry.getInstance().unregisterWithClassName("dontExist"));
   }

   public static class FakeDistributedLockManagerFactory implements DistributedLockManagerFactory  {

      @Override
      public DistributedLockManager build(Map<String, String> properties) {
         return null;
      }

      @Override
      public String getName() {
         return "fake";
      }

      @Override
      public String getImplName() {
         return "Fake";
      }

      @Override
      public Set<String> getValidParametersList() {
         return Set.of();
      }
   }

}
