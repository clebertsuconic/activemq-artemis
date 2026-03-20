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

package org.apache.activemq.artemis.core.persistence.impl.newdatabase.dbdata;


import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.journal.IOCompletion;
import org.apache.activemq.artemis.core.persistence.impl.newdatabase.worker.DataWorker;
import org.apache.activemq.artemis.core.postoffice.QueueInfo;

public class QueueData extends DBData<DataWorker> {


   public QueueData(QueueInfo info, IOCompletion context) {
      super(context, true);
   }

   public QueueData(long addressId, long id,
                    String name,
                    String filter,
                    boolean isMulticast,
                    boolean isAnycast,
                    IOCompletion context) {
      super(context, true);
      this.addressId = addressId;
      this.id = id;
      this.name = name;
      this.filter = filter;
      this.isMulticast = isMulticast;
      this.isAnycast = isAnycast;
   }

   public long id;
   public long addressId;
   public String name;
   public String filter;
   public boolean isMulticast;
   public boolean isAnycast;

   @Override
   public void store(DataWorker worker) {
      worker.insertQueueStatement.addData(this, context);
   }

   public QueueConfiguration toQueueConfiguration() {
      QueueConfiguration queueConfiguration = QueueConfiguration.of(name);
      if (isAnycast) {
         queueConfiguration.setRoutingType(RoutingType.ANYCAST);
      }
      if (isMulticast) {
         queueConfiguration.setRoutingType(RoutingType.MULTICAST);
      }
      if (filter != null) {
         queueConfiguration.setFilterString(filter);
      }
      queueConfiguration.setId(id);
      // TODO: properties
      return queueConfiguration;
   }
}
