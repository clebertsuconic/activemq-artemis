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
package org.apache.activemq.artemis.protocol.amqp.proton.handler;

import javax.security.auth.Subject;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPConnectionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.ProtonInitializable;
import org.apache.activemq.artemis.protocol.amqp.sasl.ClientSASL;
import org.apache.activemq.artemis.protocol.amqp.sasl.SASLResult;
import org.apache.activemq.artemis.protocol.amqp.sasl.ServerSASL;
import org.apache.activemq.artemis.spi.core.remoting.ReadyListener;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Collector;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Event;
import org.apache.qpid.proton.engine.Sasl;
import org.apache.qpid.proton.engine.SaslListener;
import org.apache.qpid.proton.engine.Transport;
import org.apache.qpid.proton.engine.impl.TransportInternal;
import org.jboss.logging.Logger;

public class ProtonHandler extends ProtonInitializable implements SaslListener {

   private static final Logger log = Logger.getLogger(ProtonHandler.class);

   private static final byte SASL = 0x03;

   private static final byte BARE = 0x00;

   private final Transport transport = Proton.transport();

   private final Connection connection = Proton.connection();

   private final Collector collector = Proton.collector();

   private List<EventHandler> handlers = new ArrayList<>();

   private ServerSASL chosenMechanism;
   private ClientSASL clientSASLMechanism;

   private final long creationTime;

   private final boolean isServer;

   private SASLResult saslResult;

   protected volatile boolean dataReceived;

   protected boolean receivedFirstPacket = false;

   private final ArtemisExecutor workerExecutor;

   private final ArtemisExecutor poolExecutor;

   protected final ReadyListener readyListener;

   boolean inDispatch = false;

   public ProtonHandler(ArtemisExecutor workerExecutor, ArtemisExecutor poolExecutor, boolean isServer) {
      this.workerExecutor = workerExecutor;
      this.poolExecutor = poolExecutor;
      this.readyListener = () -> runLater(this::flush);
      this.creationTime = System.currentTimeMillis();
      this.isServer = isServer;

      try {
         ((TransportInternal) transport).setUseReadOnlyOutputBuffer(false);
      } catch (NoSuchMethodError nsme) {
         // using a version at runtime where the optimization isn't available, ignore
         log.trace("Proton output buffer optimisation unavailable");
      }

      transport.bind(connection);
      connection.collect(collector);
   }

   public Long tick(boolean firstTick) {
      requireHandler();
      if (!firstTick) {
         try {
            if (connection.getLocalState() != EndpointState.CLOSED) {
               long rescheduleAt = transport.tick(TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));
               if (transport.isClosed()) {
                  throw new IllegalStateException("Channel was inactive for to long");
               }
               return rescheduleAt;
            }
         } catch (Exception e) {
            log.warn(e.getMessage(), e);
            transport.close();
            connection.setCondition(new ErrorCondition());
         } finally {
            flush();
         }
         return 0L;
      }
      return transport.tick(TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));
   }

   /**
    * We cannot flush until the initial handshake was finished.
    * If this happens before the handshake, the connection response will happen without SASL
    * and the client will respond and fail with an invalid code.
    */
   public void scheduledFlush() {
      if (receivedFirstPacket) {
         flush();
      }
   }

   public int capacity() {
      requireHandler();
      return transport.capacity();
   }

   public void requireHandler() {
      if (!workerExecutor.inHandler(Thread.currentThread())) {
         // this should not happen unless there is an obvious programming error
         log.warn("Using inHandler is required", new Exception("trace"));
         throw new IllegalStateException("this method requires to be called within the handler, use the executor");
      }
   }

   public Transport getTransport() {
      return transport;
   }

   public Connection getConnection() {
      return connection;
   }

   public ProtonHandler addEventHandler(EventHandler handler) {
      handlers.add(handler);
      return this;
   }

   public void createServerSASL(String[] mechanisms) {
      requireHandler();
      Sasl sasl = transport.sasl();
      sasl.server();
      sasl.setMechanisms(mechanisms);
      sasl.setListener(this);
   }

   boolean scheduledFlush = false;



   public void flushBytes() {
      requireHandler();

      for (EventHandler handler : handlers) {
         if (!handler.flowControl(readyListener)) {
            return;
         }
      }

      if (!scheduledFlush) {
         scheduledFlush = true;
         workerExecutor.execute(this::actualFlush);
      }
   }

   private void actualFlush() {
      requireHandler();

      try {
         while (true) {
            ByteBuffer head = transport.head();
            int pending = head.remaining();

            if (pending <= 0) {
               break;
            }

            // We allocated a Pooled Direct Buffer, that will be sent down the stream
            ByteBuf buffer = PooledByteBufAllocator.DEFAULT.directBuffer(pending);
            buffer.writeBytes(head);

            for (EventHandler handler : handlers) {
               handler.pushBytes(buffer);
            }

            transport.pop(pending);
         }
      } finally {
         scheduledFlush = false;
      }
   }

   public SASLResult getSASLResult() {
      return saslResult;
   }

   public void inputBuffer(ByteBuf buffer) {
      requireHandler();
      dataReceived = true;
      while (buffer.readableBytes() > 0) {
         int capacity = transport.capacity();

         if (!receivedFirstPacket) {
            handleFirstPacket(buffer);
            // there is a chance that if SASL Handshake has been carried out that the capacity may change.
            capacity = transport.capacity();
         }

         if (capacity > 0) {
            ByteBuffer tail = transport.tail();
            int min = Math.min(capacity, buffer.readableBytes());
            tail.limit(min);
            buffer.readBytes(tail);

            flush();
         } else {
            if (capacity == 0) {
               log.debugf("abandoning: readableBytes=%d", buffer.readableBytes());
            } else {
               log.debugf("transport closed, discarding: readableBytes=%d, capacity=%d", buffer.readableBytes(), transport.capacity());
            }
            break;
         }
      }
   }

   public boolean checkDataReceived() {
      boolean res = dataReceived;

      dataReceived = false;

      return res;
   }

   public long getCreationTime() {
      return creationTime;
   }

   public void runOnPool(Runnable runnable) {
      poolExecutor.execute(runnable);
   }

   public void runNow(Runnable runnable) {
      if (workerExecutor.inHandler(Thread.currentThread())) {
         runnable.run();
      } else {
         workerExecutor.execute(runnable);
      }
   }

   public void runLater(Runnable runnable) {
      workerExecutor.execute(runnable);
   }

   public void flush() {
      if (workerExecutor.inHandler(Thread.currentThread())) {
         transport.process();
         dispatch();
      } else {
         runLater(() -> {
            transport.process();
            dispatch();
         });
      }
   }

   public void close(ErrorCondition errorCondition, AMQPConnectionContext connectionContext) {
      runNow(() -> {
         if (errorCondition != null) {
            connection.setCondition(errorCondition);
         }
         connection.close();
         flush();
         connectionContext.getConnectionCallback().getTransportConnection().close();
      });
   }

   // server side SASL Listener
   @Override
   public void onSaslInit(Sasl sasl, Transport transport) {
      log.debug("onSaslInit: " + sasl);
      dispatchRemoteMechanismChosen(sasl.getRemoteMechanisms()[0]);

      if (chosenMechanism != null) {

         processPending(sasl);

      } else {
         // no auth available, system error
         saslComplete(sasl, Sasl.SaslOutcome.PN_SASL_SYS);
      }
   }

   private void processPending(Sasl sasl) {
      byte[] dataSASL = new byte[sasl.pending()];

      int received = sasl.recv(dataSASL, 0, dataSASL.length);
      if (log.isTraceEnabled()) {
         log.trace("Working on sasl, length:" + received);
      }

      byte[] response = chosenMechanism.processSASL(received != -1 ? dataSASL : null);
      if (response != null) {
         sasl.send(response, 0, response.length);
      }

      saslResult = chosenMechanism.result();
      if (saslResult != null) {
         if (saslResult.isSuccess()) {
            saslComplete(sasl, Sasl.SaslOutcome.PN_SASL_OK);
         } else {
            saslComplete(sasl, Sasl.SaslOutcome.PN_SASL_AUTH);
         }
      }
   }

   @Override
   public void onSaslResponse(Sasl sasl, Transport transport) {
      log.debug("onSaslResponse: " + sasl);
      processPending(sasl);
   }

   // client SASL Listener
   @Override
   public void onSaslMechanisms(Sasl sasl, Transport transport) {

      dispatchMechanismsOffered(sasl.getRemoteMechanisms());

      if (clientSASLMechanism == null) {
         log.infof("Outbound connection failed - unknown mechanism, offered mechanisms: %s", Arrays.asList(sasl.getRemoteMechanisms()));
         dispatchAuthFailed();
      } else {
         sasl.setMechanisms(clientSASLMechanism.getName());
         byte[] initialResponse = clientSASLMechanism.getInitialResponse();
         if (initialResponse != null) {
            sasl.send(initialResponse, 0, initialResponse.length);
         }
      }
   }

   @Override
   public void onSaslChallenge(Sasl sasl, Transport transport) {
      int challengeSize = sasl.pending();
      byte[] challenge = new byte[challengeSize];
      sasl.recv(challenge, 0, challengeSize);
      byte[] response = clientSASLMechanism.getResponse(challenge);
      sasl.send(response, 0, response.length);
   }

   @Override
   public void onSaslOutcome(Sasl sasl, Transport transport) {
      log.debug("onSaslOutcome: " + sasl);
      switch (sasl.getState()) {
         case PN_SASL_FAIL:
            log.info("Outbound connection failed, authentication failure");
            dispatchAuthFailed();
            break;
         case PN_SASL_PASS:
            log.debug("Outbound connection succeeded");

            if (sasl.pending() != 0) {
               byte[] additionalData = new byte[sasl.pending()];
               sasl.recv(additionalData, 0, additionalData.length);
               clientSASLMechanism.getResponse(additionalData);
            }

            saslResult = new SASLResult() {
               @Override
               public String getUser() {
                  return null;
               }

               @Override
               public Subject getSubject() {
                  return null;
               }

               @Override
               public boolean isSuccess() {
                  return true;
               }
            };

            dispatchAuthSuccess();
            break;

         default:
            break;
      }
   }

   private void saslComplete(Sasl sasl, Sasl.SaslOutcome saslOutcome) {
      log.debug("saslComplete: " + sasl);
      sasl.done(saslOutcome);
      if (chosenMechanism != null) {
         chosenMechanism.done();
         chosenMechanism = null;
      }
   }

   private void dispatchAuthFailed() {
      for (EventHandler h : handlers) {
         h.onAuthFailed(this, getConnection());
      }
   }

   private void dispatchAuthSuccess() {
      for (EventHandler h : handlers) {
         h.onAuthSuccess(this, getConnection());
      }
   }

   private void dispatchMechanismsOffered(final String[] mechs) {
      for (EventHandler h : handlers) {
         h.onSaslMechanismsOffered(this, mechs);
      }
   }

   private void dispatchAuth(boolean sasl) {
      for (EventHandler h : handlers) {
         h.onAuthInit(this, getConnection(), sasl);
      }
   }

   private void dispatchRemoteMechanismChosen(final String mech) {
      for (EventHandler h : handlers) {
         h.onSaslRemoteMechanismChosen(this, mech);
      }
   }

   private void dispatch() {
      Event ev;

      if (inDispatch) {
         // Avoid recursion from events
         return;
      }
      try {
         inDispatch = true;
         while ((ev = collector.peek()) != null) {
            for (EventHandler h : handlers) {
               if (log.isTraceEnabled()) {
                  log.trace("Handling " + ev + " towards " + h);
               }
               try {
                  Events.dispatch(ev, h);
               } catch (Exception e) {
                  log.warn(e.getMessage(), e);
                  ErrorCondition error = new ErrorCondition();
                  error.setCondition(AmqpError.INTERNAL_ERROR);
                  error.setDescription("Unrecoverable error: " + (e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage()));
                  connection.setCondition(error);
                  connection.close();
               }
            }

            collector.pop();
         }

      } finally {
         inDispatch = false;
      }

      flushBytes();
   }


   public void handleError(Exception e) {
      if (workerExecutor.inHandler(Thread.currentThread())) {
         internalHandlerError(e);
      } else {
         runLater(() -> internalHandlerError(e));
      }
   }

   private void internalHandlerError(Exception e) {
      log.warn(e.getMessage(), e);
      ErrorCondition error = new ErrorCondition();
      error.setCondition(AmqpError.INTERNAL_ERROR);
      error.setDescription("Unrecoverable error: " + (e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage()));
      connection.setCondition(error);
      connection.close();
      flush();
   }


   public void open(String containerId, Map<Symbol, Object> connectionProperties) {
      this.transport.open();
      this.connection.setContainer(containerId);
      this.connection.setProperties(connectionProperties);
      this.connection.open();
      flush();
   }

   public void setChosenMechanism(ServerSASL chosenMechanism) {
      this.chosenMechanism = chosenMechanism;
   }

   public void setClientMechanism(final ClientSASL saslClientMech) {
      this.clientSASLMechanism = saslClientMech;
   }

   public void createClientSASL() {
      Sasl sasl = transport.sasl();
      sasl.client();
      sasl.setListener(this);
   }

   private void handleFirstPacket(ByteBuf buffer) {
      try {
         byte auth = buffer.getByte(4);
         if (auth == SASL || auth == BARE) {
            if (isServer) {
               dispatchAuth(auth == SASL);
            } else if (auth == BARE && clientSASLMechanism == null) {
               dispatchAuthSuccess();
            }
         }
      } catch (Throwable e) {
         log.warn(e.getMessage(), e);
      }

      receivedFirstPacket = true;
   }
}
