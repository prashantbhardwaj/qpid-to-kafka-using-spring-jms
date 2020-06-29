package person.prashant.data.qpidkafkaspring;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.listener.SessionAwareMessageListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.stereotype.Component;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class MessageListenerToSendMessageToKafka implements SessionAwareMessageListener, ProducerListener<String, String> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageListenerToSendMessageToKafka.class);
    private Session session;
    private Map<String, Message> pendingAcks = new ConcurrentHashMap();

    @Autowired
    private KafkaTemplate kafkaTemplate;
    @Value("kafka.topic") String topic;

    @Override
    public void onMessage(Message jmsMessage, Session session) throws JMSException {
        LOGGER.info("Received message - {}", jmsMessage);
        String jmsMessageId = jmsMessage.getJMSMessageID();
        try {
            this.session = session;
            this.pendingAcks.put(jmsMessage.getJMSMessageID(), jmsMessage);

            this.kafkaTemplate.sendDefault(jmsMessage.getJMSMessageID(), jmsMessage);
        } catch (Exception exception){
            onError(this.topic, 1, jmsMessageId, jmsMessage.toString(), exception);
            throw exception;
        }
    }

    @Override
    public void onSuccess(String topic, Integer partition, String key, String value, RecordMetadata recordMetadata) {
        LOGGER.info("Sent message - {}", value);
        Message jmsMessage = pendingAcks.remove(key);
        if(pendingAcks.isEmpty()){
            try {
                jmsMessage.acknowledge();
            } catch (JMSException e) {
                LOGGER.error("Exception while acknowledging the message", e);
                onError(topic, partition, key, value, e);
            }
        }
    }

    @Override
    public void onError(String topic, Integer partition, String key, String value, Exception exception) {
        LOGGER.error("Failed message - {}", value, exception);
        this.pendingAcks.clear();
        try {
            this.session.recover();
        } catch (JMSException e) {
            LOGGER.error("Exception while recovering the session", e);
        }
    }


}
