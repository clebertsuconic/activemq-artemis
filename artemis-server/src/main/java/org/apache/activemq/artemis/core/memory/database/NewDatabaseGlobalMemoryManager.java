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

package org.apache.activemq.artemis.core.memory.database;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.memory.AddressMemoryManager;
import org.apache.activemq.artemis.core.memory.GlobalMemoryManager;
import org.apache.activemq.artemis.utils.SizeAwareMetric;

public class NewDatabaseGlobalMemoryManager implements GlobalMemoryManager {

   private final ConcurrentMap<SimpleString, NewDatabaseAddressMemoryManager> addressManagers = new ConcurrentHashMap<>();

   private final SizeAwareMetric globalSizeMetric;

   public NewDatabaseGlobalMemoryManager() {
      this.globalSizeMetric = new SizeAwareMetric();
   }

   // TODO: The addressID so we can use on queries
   @Override
   public AddressMemoryManager getMemoryAddressManager(SimpleString address) throws Exception {
      NewDatabaseAddressMemoryManager addressMemoryManager = addressManagers.get(address);

      if (addressMemoryManager == null) {
         addressMemoryManager = new NewDatabaseAddressMemoryManager(address, -1, this);
         addressMemoryManager = addressManagers.putIfAbsent(address, addressMemoryManager);
      }
      return addressMemoryManager;
   }

   @Override
   public long getGlobalSize() {
      return globalSizeMetric.getSize();
   }

   @Override
   public long getGlobalMessages() {
      return globalSizeMetric.getElements();
   }

   @Override
   public void start() throws Exception {

   }

   @Override
   public void stop() throws Exception {

   }

   @Override
   public boolean isStarted() {
      return false;
   }
}
