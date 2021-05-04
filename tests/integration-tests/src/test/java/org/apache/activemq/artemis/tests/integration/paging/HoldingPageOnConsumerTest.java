package org.apache.activemq.artemis.tests.integration.paging;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.qpid.jms.provider.amqp.AmqpConsumer;
import org.apache.qpid.jms.provider.amqp.AmqpProducer;
import org.junit.Assert;
import org.junit.Test;

public class HoldingPageOnConsumerTest extends ActiveMQTestBase {

   ActiveMQServer server;

   @Override
   public void setUp() throws Exception {
      super.setUp();
      final Configuration config = createDefaultConfig(0, true);
      final int PAGE_MAX = 20 * 1024;

      final int PAGE_SIZE = 10 * 1024;

      server = createServer(true, config, PAGE_SIZE, PAGE_MAX);
      server.start();
   }

   @Test
   public void testHoldingPage() throws Exception {


      server.addAddressInfo(new AddressInfo("theAddres").addRoutingType(RoutingType.MULTICAST));
      server.createQueue(new QueueConfiguration("q1").setAddress("theAddress").setRoutingType(RoutingType.MULTICAST).setDurable(true));
      server.createQueue(new QueueConfiguration("q2").setAddress("theAddress").setRoutingType(RoutingType.MULTICAST).setDurable(true));

      Queue queue = server.locateQueue("q1");
      Assert.assertNotNull(queue);

      queue.getPagingStore().startPaging();

      AmqpClient client = new AmqpClient(new URI("tcp://localhost:61616"), null, null);
      AmqpConnection connection = client.connect();
      addCloseable(connection::close);
      AmqpSession session = connection.createSession();

      AmqpReceiver holdingConsumer = session.createReceiver("theAddress::q1");
      AmqpSender producer = session.createSender("theAddress");

      holdingConsumer.flow(15);


      Assert.assertTrue(queue.getPagingStore().isPaging());

      for (int i = 0; i < 100; i++) {

         if (i == 5 || i == 10 || i == 15) {
            queue.getPagingStore().forceAnotherPage();
         }

         AmqpMessage message = new AmqpMessage();
         message.setDurable(true);
         message.setText("hello");
         message.setApplicationProperty("i", i);
         producer.send(message);

         if (i < 5) {
            AmqpMessage recMessage = holdingConsumer.receive(5, TimeUnit.SECONDS);
            Assert.assertNotNull(recMessage);
            recMessage.accept();
         }
      }

      AmqpReceiver receiveAll = session.createReceiver("theAddress::q2");
      receiveAll.flow(100);
      for (int i = 0; i < 100; i++) {
         AmqpMessage message = receiveAll.receive(5, TimeUnit.SECONDS);
         message.accept();
      }
      receiveAll.close();


      Thread.sleep(1000);

      AmqpReceiver anotherReceiver = session.createReceiver("theAddress::q1");
      anotherReceiver.flow(85);

      for (int i = 0; i < 85; i++) {
         AmqpMessage message = anotherReceiver.receive(5, TimeUnit.SECONDS);
         Assert.assertNotNull(message);
         message.accept();
      }

      Thread.sleep(1000);
      queue.getPageSubscription().cleanupEntries(true);

      Thread.sleep(1000);

      Assert.assertTrue(queue.getPagingStore().isPaging());

      holdingConsumer.close();

      holdingConsumer = session.createReceiver("theAddress::q1");
      holdingConsumer.flow(10);

      for (int i = 0; i < 10; i++) {

         AmqpMessage message = holdingConsumer.receive(5, TimeUnit.SECONDS);
         Assert.assertNotNull(message);
         message.accept();
      }


   }
}
