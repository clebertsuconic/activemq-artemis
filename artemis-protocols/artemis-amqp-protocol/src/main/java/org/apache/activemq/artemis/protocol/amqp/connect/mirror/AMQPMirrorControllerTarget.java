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
package org.apache.activemq.artemis.protocol.amqp.connect.mirror;

import java.io.PrintStream;
import java.util.List;
import java.util.function.ToLongFunction;

import org.apache.activemq.artemis.api.core.ActiveMQAddressDoesNotExistException;
import org.apache.activemq.artemis.api.core.ActiveMQNonExistentQueueException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.postoffice.DuplicateIDCache;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.server.impl.AckReason;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.impl.RoutingContextImpl;
import org.apache.activemq.artemis.core.server.mirror.MirrorController;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.TransactionOperationAbstract;
import org.apache.activemq.artemis.core.transaction.impl.TransactionImpl;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessageBrokerAccessor;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback;
import org.apache.activemq.artemis.protocol.amqp.broker.ProtonProtocolManager;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPConnectionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.ProtonAbstractReceiver;
import org.apache.activemq.artemis.utils.ByteUtil;
import org.apache.activemq.artemis.utils.StringPrintStream;
import org.apache.activemq.artemis.utils.pools.MpscPool;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.jboss.logging.Logger;

import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.ADDRESS;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.ADD_ADDRESS;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.CREATE_QUEUE;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.DELETE_ADDRESS;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.DELETE_QUEUE;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.EVENT_TYPE;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.INTERNAL_DESTINATION;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.INTERNAL_ID;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.POST_ACK;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.QUEUE;
import static org.apache.activemq.artemis.protocol.amqp.connect.mirror.AMQPMirrorControllerSource.INTERNAL_ID_EXTRA_PROPERTY;

public class AMQPMirrorControllerTarget extends ProtonAbstractReceiver implements MirrorController {

   private static final Logger logger = Logger.getLogger(AMQPMirrorControllerTarget.class);

   private static ThreadLocal<MirrorController> controllerThreadLocal = new ThreadLocal<>();

   public static void setControllerTarget(MirrorController controller) {
      controllerThreadLocal.set(controller);
   }

   public static MirrorController getControllerTarget() {
      return controllerThreadLocal.get();
   }

   class ACKMessage extends TransactionOperationAbstract implements IOCallback, Runnable {

      Delivery delivery;

      void reset() {
         this.delivery = null;
      }

      ACKMessage setDelivery(Delivery delivery) {
         this.delivery = delivery;
         return this;
      }

      @Override
      public void run() {
         if (logger.isTraceEnabled()) {
            logger.trace("Delivery settling for " + delivery + ", context=" + delivery.getContext());
         }
         delivery.disposition(Accepted.getInstance());
         settle(delivery);
         connection.flush();
         AMQPMirrorControllerTarget.this.ackMessageMpscPool.release(ACKMessage.this);
      }

      @Override
      public void beforeCommit(Transaction tx) throws Exception {
      }

      @Override
      public void afterCommit(Transaction tx) {
         done();
      }

      @Override
      public void done() {
         connection.runNow(this);
      }

      @Override
      public void onError(int errorCode, String errorMessage) {
      }
   }

   // in a regular case we should not have more than amqpCredits on the pool, that's the max we would need
   private final MpscPool<ACKMessage> ackMessageMpscPool = new MpscPool<>(amqpCredits, ACKMessage::reset,  () -> new ACKMessage());

   final RoutingContextImpl routingContext = new RoutingContextImpl(null);

   final BasicMirrorController<Receiver> basicController;

   final ActiveMQServer server;

   final DuplicateIDCache duplicateIDCache;

   private final ToLongFunction<MessageReference> referenceIDSupplier;

   public AMQPMirrorControllerTarget(AMQPSessionCallback sessionSPI,
                                     AMQPConnectionContext connection,
                                     AMQPSessionContext protonSession,
                                     Receiver receiver,
                                     ActiveMQServer server) {
      super(sessionSPI, connection, protonSession, receiver);
      this.basicController = new BasicMirrorController(server, BasicMirrorController.getRemoteMirrorID(receiver));
      this.basicController.setLink(receiver);
      this.server = server;

      // we ise the number of credits for the duplicate detection, as that means the maximum number of elements you can have pending
      if (logger.isTraceEnabled()) {
         logger.trace("Setting up Duplicate detection on " + ProtonProtocolManager.MIRROR_ADDRESS + " wth " + connection.getAmqpCredits() + " as that's the number of credits");
      }
      duplicateIDCache = server.getPostOffice().getDuplicateIDCache(SimpleString.toSimpleString(ProtonProtocolManager.MIRROR_ADDRESS), connection.getAmqpCredits());

      referenceIDSupplier = (source) -> {
         Long id = (Long) source.getMessage().getBrokerProperty(INTERNAL_ID_EXTRA_PROPERTY);
         if (id == null) {
            long mixedID = ByteUtil.mixByteAndLong(basicController.mirrorIDForRouting, source.getMessageID());
            if (logger.isTraceEnabled()) {
               logger.trace("ID provider mixed server = " + basicController.mirrorIDForRouting + " and ID=" + source.getMessageID() + " resulting in " + mixedID + " on " + source.getMessage());
            }
            return mixedID;
         } else {
            if (logger.isTraceEnabled()) {
               logger.trace("ID provider returned " + id + " for " + source.getMessage());
            }
            return id;
         }
      };
   }

   @Override
   public short getLocalMirrorId() {
      return basicController.getLocalMirrorId();
   }

   @Override
   public short getRemoteMirrorId() {
      return basicController.getRemoteMirrorId();
   }

   @Override
   public void flow() {
      creditRunnable.run();
   }

   @Override
   protected void actualDelivery(AMQPMessage message, Delivery delivery, Receiver receiver, Transaction tx) {
      incrementSettle();


      if (logger.isTraceEnabled()) {
         logger.trace(server + "::actualdelivery call for " + message);
      }
      setControllerTarget(this);

      delivery.setContext(message);

      ACKMessage messageCompletionAck = this.ackMessageMpscPool.borrow().setDelivery(delivery);

      try {
         /** We use message annotations, because on the same link we will receive control messages
          *  coming from mirror events,
          *  and the actual messages that need to be replicated.
          *  Using anything from the body would force us to parse the body on regular messages.
          *  The body of the message may still be used on control messages, on cases where a JSON string is sent. */
         Object eventType = AMQPMessageBrokerAccessor.getMessageAnnotationProperty(message, EVENT_TYPE);
         if (eventType != null) {
            if (eventType.equals(ADD_ADDRESS)) {
               AddressInfo addressInfo = parseAddress(message);
               if (logger.isDebugEnabled()) {
                  logger.debug(server + " Adding Address " + addressInfo);
               }
               addAddress(addressInfo);
            } else if (eventType.equals(DELETE_ADDRESS)) {
               AddressInfo addressInfo = parseAddress(message);
               if (logger.isDebugEnabled()) {
                  logger.debug(server + " Removing Address " + addressInfo);
               }
               deleteAddress(addressInfo);
            } else if (eventType.equals(CREATE_QUEUE)) {
               QueueConfiguration queueConfiguration = parseQueue(message);
               if (logger.isDebugEnabled()) {
                  logger.debug(server + " Creating queue " + queueConfiguration);
               }
               createQueue(queueConfiguration);
            } else if (eventType.equals(DELETE_QUEUE)) {

               String address = (String) AMQPMessageBrokerAccessor.getMessageAnnotationProperty(message, ADDRESS);
               String queueName = (String) AMQPMessageBrokerAccessor.getMessageAnnotationProperty(message, QUEUE);
               if (logger.isDebugEnabled()) {
                  logger.debug(server + " Deleting queue " + queueName + " on address " + address);
               }
               deleteQueue(SimpleString.toSimpleString(address), SimpleString.toSimpleString(queueName));
            } else if (eventType.equals(POST_ACK)) {
               String address = (String) AMQPMessageBrokerAccessor.getMessageAnnotationProperty(message, ADDRESS);
               String queueName = (String) AMQPMessageBrokerAccessor.getMessageAnnotationProperty(message, QUEUE);
               AmqpValue value = (AmqpValue) message.getBody();
               Long messageID = (Long) value.getValue();
               if (logger.isDebugEnabled()) {
                  logger.debug(server + " Post ack address=" + address + " queueName = " + queueName + " messageID=" + messageID + "(mirrorID=" + ByteUtil.getFirstByte(messageID) + ", messageID=" + ByteUtil.removeFirstByte(messageID) + ")");
               }
               if (postAcknowledge(address, queueName, messageID, messageCompletionAck)) {
                  messageCompletionAck = null;
               }
            }
         } else {
            if (logger.isDebugEnabled()) {
               logger.debug(server + " Sending message " + message);
            }
            if (sendMessage(message, messageCompletionAck)) {
               messageCompletionAck = null;
            }
         }
      } catch (Throwable e) {
         logger.warn(e.getMessage(), e);
      } finally {
         setControllerTarget(null);
         if (messageCompletionAck != null) {
            server.getStorageManager().afterCompleteOperations(messageCompletionAck);
         }
      }
   }

   @Override
   public void initialize() throws Exception {
      super.initialize();
      org.apache.qpid.proton.amqp.messaging.Target target = (org.apache.qpid.proton.amqp.messaging.Target) receiver.getRemoteTarget();

      // Match the settlement mode of the remote instead of relying on the default of MIXED.
      receiver.setSenderSettleMode(receiver.getRemoteSenderSettleMode());

      // We don't currently support SECOND so enforce that the answer is anlways FIRST
      receiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);
      flow();
   }

   private QueueConfiguration parseQueue(AMQPMessage message) throws Exception {
      AmqpValue bodyvalue = (AmqpValue) message.getBody();
      String body = (String) bodyvalue.getValue();
      QueueConfiguration queueConfiguration = QueueConfiguration.fromJSON(body);
      return queueConfiguration;
   }

   private AddressInfo parseAddress(AMQPMessage message) throws Exception {
      AmqpValue bodyvalue = (AmqpValue) message.getBody();
      String body = (String) bodyvalue.getValue();
      AddressInfo addressInfo = AddressInfo.fromJSON(body);
      return addressInfo;
   }

   @Override
   public void addAddress(AddressInfo addressInfo) throws Exception {
      if (logger.isDebugEnabled()) {
         logger.debug(server + " Adding address " + addressInfo);
      }
      server.addAddressInfo(addressInfo);
   }

   @Override
   public void deleteAddress(AddressInfo addressInfo) throws Exception {
      if (logger.isDebugEnabled()) {
         logger.debug(server + " delete address " + addressInfo);
      }
      try {
         server.removeAddressInfo(addressInfo.getName(), null, true);
      } catch (ActiveMQAddressDoesNotExistException expected) {
         // it was removed from somewhere else, which is fine
         logger.debug(expected.getMessage(), expected);
      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
      }
   }

   @Override
   public void createQueue(QueueConfiguration queueConfiguration) throws Exception {
      if (logger.isDebugEnabled()) {
         logger.debug(server + " Adding queue " + queueConfiguration);
      }
      server.createQueue(queueConfiguration, true);
   }

   @Override
   public void deleteQueue(SimpleString addressName, SimpleString queueName) throws Exception {
      if (logger.isDebugEnabled()) {
         logger.debug(server + " destroy queue " + queueName + " on address = " + addressName + " server " + server.getIdentity());
      }
      try {

         server.destroyQueue(queueName,null, false, true, false, false);
      } catch (ActiveMQNonExistentQueueException expected) {
         logger.debug(server + " queue " + queueName + " was previously removed", expected);
      }
   }

   public boolean postAcknowledge(String address, String queue, long messageID, ACKMessage ackMessage) throws Exception {
      final Queue targetQueue = server.locateQueue(queue);

      if (targetQueue == null) {
         logger.warn("Queue " + queue + " not found on mirror target, ignoring ack for queue=" + queue + ", messageID=" + messageID + " being serverID=" + ByteUtil.getFirstByte(messageID) + " and original id portion = " + ByteUtil.removeFirstByte(messageID));
         return false;
      }

      if (logger.isTraceEnabled()) {
         // we only do the following check if tracing
         if (targetQueue.getConsumerCount() > 0) {
            StringPrintStream stringPrintStream = new StringPrintStream();
            PrintStream out = stringPrintStream.newStream();

            targetQueue.getConsumers().forEach((c) -> out.println(c));

            // shouldn't this be a warn?????
            // Things we could:
            // Kick out the consumer, kick out the connection....
            logger.trace("server " + server.getIdentity() + ", queue " + targetQueue.getName() + " has consumers while delivering ack for " + messageID + "::" + stringPrintStream.toString());
         }
      }

      if (logger.isTraceEnabled()) {
         logger.trace("Server " + server.getIdentity() + " with queue = " + queue + " being acked for " + messageID + " coming from " + messageID + " targetQueue = " + targetQueue);
      }

      Transaction transaction = new MirrorTransaction(server.getStorageManager());

      transaction.addOperation(ackMessage);
      performAck(messageID, targetQueue, transaction, true);
      return true;

   }

   private void performAck(long messageID, Queue targetQueue, Transaction transaction, boolean retry) {
      assert transaction != null;
      assert transaction instanceof MirrorTransaction;

      if (logger.isTraceEnabled()) {
         logger.trace("performAck " + messageID + "(messageID=" + ByteUtil.removeFirstByte(messageID) + "), targetQueue=" + targetQueue.getName());
      }
      MessageReference reference = targetQueue.removeWithSuppliedID(messageID, referenceIDSupplier);
      if (reference == null && retry) {

         if (logger.isDebugEnabled()) {
            logger.debug("Retrying Reference not found on messageID=" + messageID + " not found, representing serverID=" + ByteUtil.getFirstByte(messageID) + " and actual messageID=" + ByteUtil.removeFirstByte(messageID));
         }
         targetQueue.flushOnIntermediate(() -> performAck(messageID, targetQueue, transaction, false));
         return;
      }
      if (reference != null) {
         if (logger.isTraceEnabled()) {
            logger.trace("Post ack Server " + server + " worked well for messageID=" + messageID + " representing serverID=" + ByteUtil.getFirstByte(messageID) + " and actual messageID=" + ByteUtil.removeFirstByte(messageID) + " and message = " + reference);
         }
         try {
            targetQueue.acknowledge(transaction, reference);
         } catch (Exception e) {
            // TODO anything else I can do here?
            // such as close the connection with error?
            logger.warn(e.getMessage(), e);
         }
      } else {
         if (logger.isDebugEnabled()) {
            logger.debug("Post ack Server " + server + " could not find messageID = " + messageID +
                            " representing serverID=" + ByteUtil.getFirstByte(messageID) + " and actual messageID=" +
                            ByteUtil.removeFirstByte(messageID));
         }
      }

      try {
         transaction.commit();
      } catch (Throwable e) {
         logger.warn(e.getMessage(), e);
      }
   }

   byte[] getInternalIDBytes(long internalID) {
      return ByteUtil.longToBytes(internalID);
   }

   byte[] getInternalIDBytes(long queueID, long internalID) {
      byte[] internalIDBytes = new byte[16];
      ByteUtil.longToBytes(queueID, internalIDBytes, 0);
      ByteUtil.longToBytes(internalID, internalIDBytes, 8);
      return internalIDBytes;
   }

   private boolean sendMessage(AMQPMessage message, ACKMessage messageCompletionAck) throws Exception {

      if (message.getMessageID() <= 0) {
         message.setMessageID(server.getStorageManager().generateID());
      }

      Long internalID = (Long) AMQPMessageBrokerAccessor.getDeliveryAnnotationProperty(message, INTERNAL_ID);
      String internalAddress = (String) AMQPMessageBrokerAccessor.getDeliveryAnnotationProperty(message, INTERNAL_DESTINATION);

      if (internalID != null) {
         int internalMirrorID = ByteUtil.getFirstByte(internalID);
         if (internalMirrorID == basicController.localMirrorId) {
            logger.debug("message " + message + " will not be sent towards " + basicController + " mirror as the internal ID already belongs there");
            return false;
         }
      }


      if (logger.isTraceEnabled()) {
         logger.trace("sendMessage on server " + server + " for message " + message +
                         " with internalID = " + internalID + " having salt id " + ByteUtil.getFirstByte(internalID) +
                               " and ID messageID=" + ByteUtil.removeFirstByte(internalID));
      }

      final TransactionImpl transaction = new MirrorTransaction(server.getStorageManager());
      transaction.addOperation(messageCompletionAck);

      routingContext.setDuplicateDetection(false); // we do our own duplicate detection here

      if (internalID != null) {
         byte[] duplicateIDBytes = getInternalIDBytes(internalID);
         if (duplicateIDCache.contains(duplicateIDBytes)) {
            flow();
            return false;
         } else {
            routingContext.setTransaction(transaction);
            duplicateIDCache.addToCache(duplicateIDBytes, transaction);
         }
         message.setBrokerProperty(INTERNAL_ID_EXTRA_PROPERTY, internalID);
      }

      if (internalAddress != null) {
         message.setAddress(internalAddress);
      }

      routingContext.clear().setMirrorSource(this);
      server.getPostOffice().route(message, routingContext, false);
      transaction.commit();
      flow();
      return true;
   }

   /**
    * @param ref
    * @param reason
    */
   @Override
   public void postAcknowledge(MessageReference ref, AckReason reason) {
   }

   @Override
   public void sendMessage(Message message, RoutingContext context, List<MessageReference> refs) {
   }

}
