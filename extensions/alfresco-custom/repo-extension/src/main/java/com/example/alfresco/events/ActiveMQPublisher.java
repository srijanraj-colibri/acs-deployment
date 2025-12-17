package com.example.alfresco.events;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.alfresco.service.cmr.repository.NodeRef;
import org.apache.activemq.ActiveMQConnectionFactory;

import java.util.HashMap;
import java.util.Map;

public class ActiveMQPublisher {

    private final String brokerUrl;
    private final String topicName;

    public ActiveMQPublisher(String brokerUrl, String topicName) {
        this.brokerUrl = brokerUrl;
        this.topicName = topicName;
    }

    public void publish(String eventType, NodeRef nodeRef) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("eventType", eventType);
        payload.put("nodeRef", nodeRef.toString());

        send(payload.toString());
    }

    private void send(String body) {
        Connection connection = null;
        Session session = null;

        try {
            ActiveMQConnectionFactory factory =
                    new ActiveMQConnectionFactory(brokerUrl);

            connection = factory.createConnection();

            // ⭐ Important for durable topic support
            connection.setClientID("alfresco-repo-publisher");

            connection.start();

            session = connection.createSession(
                    false,
                    Session.AUTO_ACKNOWLEDGE
            );

            // 🔥 Topic (not queue)
            Destination destination = session.createTopic(topicName);

            MessageProducer producer = session.createProducer(destination);

            // 🔒 VERY IMPORTANT: persistent messages
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);

            TextMessage message = session.createTextMessage(body);

            producer.send(message);

        } catch (Exception e) {
            throw new RuntimeException("Failed to publish message to ActiveMQ topic", e);
        } finally {
            try {
                if (session != null) session.close();
                if (connection != null) connection.close();
            } catch (Exception ignored) {}
        }
    }
}
