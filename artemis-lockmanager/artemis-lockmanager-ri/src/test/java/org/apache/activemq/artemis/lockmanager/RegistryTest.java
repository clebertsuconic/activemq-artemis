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

import java.lang.invoke.MethodHandles;

import org.apache.activemq.artemis.lockmanager.file.FileBasedLockManager;
import org.apache.activemq.artemis.lockmanager.file.FileBasedLockManagerFactory;
import org.apache.activemq.artemis.lockmanager.zookeeper.CuratorDistributedLockManager;
import org.apache.activemq.artemis.lockmanager.zookeeper.CuratorDistributedLockManagerFactory;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RegistryTest {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   @Test
   public void testRegistry() {

      DistributedLockManagerFactory factory;

      factory = Registry.getInstance().getFactory("file");
      assertTrue(factory instanceof FileBasedLockManagerFactory);
      assertEquals(FileBasedLockManager.class.getName(), factory.getImplName());

      factory = Registry.getInstance().getFactoryWithClassName(FileBasedLockManager.class.getName());
      assertTrue(factory instanceof FileBasedLockManagerFactory);
      assertEquals(FileBasedLockManager.class.getName(), factory.getImplName());

      factory = Registry.getInstance().getFactory("ZK");
      assertTrue(factory instanceof CuratorDistributedLockManagerFactory);
      assertEquals(CuratorDistributedLockManager.class.getName(), factory.getImplName());

      factory = Registry.getInstance().getFactoryWithClassName(CuratorDistributedLockManager.class.getName());
      assertTrue(factory instanceof CuratorDistributedLockManagerFactory);
      assertEquals(CuratorDistributedLockManager.class.getName(), factory.getImplName());

   }

}
