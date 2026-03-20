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

package org.apache.activemq.artemis.core.server.impl.newDatabase;

import java.util.concurrent.ScheduledExecutorService;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.memory.AddressMemoryManager;
import org.apache.activemq.artemis.core.memory.QueueMemoryManager;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.QueueFactory;
import org.apache.activemq.artemis.core.server.impl.QueueImpl;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;

public class NewDatabaseQueueImpl extends QueueImpl {

   public NewDatabaseQueueImpl(QueueConfiguration queueConfiguration,
                               Filter filter,
                               AddressMemoryManager addressMemoryManager,
                               QueueMemoryManager queueMemoryManager,
                               ScheduledExecutorService scheduledExecutor,
                               PostOffice postOffice,
                               StorageManager storageManager,
                               HierarchicalRepository<AddressSettings> addressSettingsRepository,
                               ArtemisExecutor executor,
                               ActiveMQServer server,
                               QueueFactory factory) {
      super(queueConfiguration, filter, addressMemoryManager, queueMemoryManager, scheduledExecutor, postOffice, storageManager, addressSettingsRepository, executor, server, factory);
   }

   @Override
   public void forceDelivery() {
      deliverAsync();
   }
   protected void checkDepage() {
      if (queueDestroyed) {
         return;
      }

      // prefetching from database will come here...
   }


}
