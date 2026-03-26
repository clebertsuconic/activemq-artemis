/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.protocol.amqp.broker;

import java.lang.invoke.MethodHandles;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.persistence.CoreMessageObjectPools;
import org.apache.activemq.artemis.utils.DataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.activemq.artemis.core.persistence.PersisterIDs.AMQPMessagePersisterV4_ID;

/**
 * V4 adds a size field to determine persister boundaries, enabling forward-compatible
 * extensions without additional versioning.
 **/
public class AMQPMessagePersisterV4 extends AMQPMessagePersisterV3 {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public static final byte ID = AMQPMessagePersisterV4_ID;

   public static AMQPMessagePersisterV4 theInstance;

   public static AMQPMessagePersisterV4 getInstance() {
      if (theInstance == null) {
         theInstance = new AMQPMessagePersisterV4();
      }
      return theInstance;
   }

   @Override
   public byte getID() {
      return ID;
   }

   public AMQPMessagePersisterV4() {
      super();
   }


   protected static final int PERSISTER_SIZE = DataConstants.SIZE_INT + // memory estimate
      DataConstants.SIZE_BYTE +
      DataConstants.SIZE_BOOLEAN; // message priority

   @Override
   public int getEncodeSize(Message record) {
      int encodeSize = super.getEncodeSize(record) + PERSISTER_SIZE + DataConstants.SIZE_INT; // the size delimiter and whatever is written in encode
      return encodeSize;
   }


   @Override
   public void encode(ActiveMQBuffer buffer, Message record) {
      super.encode(buffer, record);

      writeSizeDelimiter(buffer);
      buffer.writeInt(record.getMemoryEstimate());
      buffer.writeByte(record.getPriority());
      buffer.writeBoolean(record.isDurable());
   }

   protected void writeSizeDelimiter(ActiveMQBuffer buffer) {
      // this is to allow us to determine the boundary of this persister, for future use.
      buffer.writeInt(PERSISTER_SIZE);
   }

   @Override
   public Message decode(ActiveMQBuffer buffer, Message ignore, CoreMessageObjectPools pool) {
      Message record = super.decode(buffer, ignore, pool);

      int sizePersister = buffer.readInt();
      int lastPosition = buffer.readerIndex() + sizePersister;

      {
         AMQPStandardMessage standardMessage = (AMQPStandardMessage) record;
         standardMessage.setMemoryEstimate(buffer.readInt());
         standardMessage.reloadPriority(buffer.readByte());
         standardMessage.reloadSetDurable(buffer.readBoolean());

         assert buffer.readerIndex() <= lastPosition;
      }

      // if a future version of this persister wrote more bytes than what we expected now, this will take care of skipping them
      buffer.readerIndex(lastPosition);

      // note that for v4 and beyond we are not calling scanAfterReload

      return record;
   }

}
