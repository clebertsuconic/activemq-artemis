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

import java.util.EnumSet;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.journal.IOCompletion;
import org.apache.activemq.artemis.core.persistence.impl.newdatabase.worker.DataWorker;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;

public class AddressData extends DBData<DataWorker> {


   public AddressData(AddressInfo info, IOCompletion context) {
      super(context, true);
      EnumSet<RoutingType> enumSet = info.getRoutingTypes();
      this.isMulticast = enumSet.contains(RoutingType.MULTICAST);
      this.isAnycast = enumSet.contains(RoutingType.ANYCAST);
      this.id = info.getId();
      this.address = String.valueOf(info.getName());
   }

   public AddressData(long id, String address, boolean isMulticast, boolean isAnycast, IOCompletion context) {
      super(context, true);
      this.id = id;
      this.address = address;
      this.isMulticast = isMulticast;
      this.isAnycast = isAnycast;
   }


   public AddressData(long id, String address, boolean isMulticast, boolean isAnycast) {
      this(id, address, isMulticast, isAnycast, null);
   }


   public AddressInfo toAddressInfo() {
      AddressInfo info = new AddressInfo(address);
      info.setId(id);
      if (isAnycast) {
         info.addRoutingType(RoutingType.ANYCAST);
      }
      if (isMulticast) {
         info.addRoutingType(RoutingType.MULTICAST);
      }
      return info;
   }

   public long id;
   public String address;
   public boolean isMulticast;
   public boolean isAnycast;

   @Override
   public void store(DataWorker worker) {
      worker.insertAddressStatement.addData(this, context);
   }
}
