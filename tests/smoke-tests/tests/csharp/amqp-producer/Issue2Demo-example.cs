using System;
using System.Collections.Generic;
using System.Threading;
using Amqp.Framing;
using Amqp.Extensions;
using Amqp.Sasl;
using Amqp.Types;

namespace Amqp.Extensions.Examples
{
    class Program
    {
        static void Main(string[] args)
        {
            // create receiver link on broker0
            //Connection connection0 = new Connection(new Address("amqp://localhost:5672"));       
            Connection connection0 = new Connection(new Address("amqp://192.168.2.221:5672"));       
            Session session0 = new Session(connection0);
            //ReceiverLink receiver0 = new ReceiverLink(session0, "test", "orders");
            //ReceiverLink receiver0 = new ReceiverLink(session0, "test", "orders");
            ReceiverLink receiver0 = new ReceiverLink(
                session0, 
                "test|0", 
                new Source(){
                    Address = "orders",
                    Capabilities = new Symbol[]{"topic", "shared", "global"},
                    Durable = 2,
                    ExpiryPolicy = new Symbol("never")
                },  
                null);
            
            // send messages to broker0
            SenderLink sender = new SenderLink(session0, "sender", "orders");
            Message message = new Message("a message!");
            message.Properties = new Properties();
            message.Properties.To = "orders"; 
            
            for (var i = 0; i < 5; i++)
            {
                sender.Send(message);
                Thread.Sleep(100);
            }

            // receive 1 of 5, works as expected...
            Message m = receiver0.Receive(TimeSpan.FromSeconds(1));
            Console.WriteLine(m.Body);
            receiver0.Accept(m);
            session0.Close();
            connection0.Close();

            // create receiver link on broker1
            //Connection connection1 = new Connection(new Address("amqp://localhost:5673"));       
            Connection connection1 = new Connection(new Address("amqp://192.168.2.221:15672"));
            Session session1 = new Session(connection1);
            //ReceiverLink receiver1 = new ReceiverLink(session1, "test", "orders");

            // create a receiverlink emulating a shared durable subscriber by passing the capabilities below, and making it durable, and never expire
            ReceiverLink receiver1 = new ReceiverLink(
                session1, 
                "test|1", 
                new Source(){
                    Address = "orders",
                    Capabilities = new Symbol[]{"topic", "shared", "global"},
                    Durable = 2,
                    ExpiryPolicy = new Symbol("never")
                },  
                null);
            

            // these 4 messages are removed from broker0 (ack'd) but never delivered. NPE seen in logs on broker1
            for (var i = 0; i < 4; i++)
            {   
                m = receiver1.Receive(TimeSpan.FromSeconds(1));
                if (m != null)
                {
                    Console.WriteLine(m.Body);
                    receiver1.Accept(m);
                }
            }

            sender.Close();
            receiver0.Close();
            receiver1.Close();
            session0.Close();
            session1.Close();
            connection0.Close();
            connection1.Close();
        }
    }
}
