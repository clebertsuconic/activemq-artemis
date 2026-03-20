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

package org.apache.activemq.artemis.core.persistence.impl.newdatabase;

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.StorageTX;
import org.apache.activemq.artemis.core.persistence.impl.newdatabase.dbdata.DBData;

public class NewDatabaseStoreTX implements StorageTX {

   long txid;
   OperationContext context;

   @Override
   public long getId() {
      return txid;
   }

   @Override
   public void completeIO() {
      if (context != null) {
         context.done();
      }
   }

   @Override
   public void setContext(OperationContext context) {
      this.context = context;
   }

   @Override
   public OperationContext getContext() {
      return this.context;
   }

   public NewDatabaseStoreTX(long txid) {
      this.txid = txid;
   }

   public List<DBData> dataList;

   public void addData(DBData data) {
      if (dataList == null) {
         dataList = new ArrayList<>();
      }
      dataList.add(data);
   }

   @Override
   public boolean isEmpty() {
      return dataList == null || dataList.isEmpty();
   }

}
