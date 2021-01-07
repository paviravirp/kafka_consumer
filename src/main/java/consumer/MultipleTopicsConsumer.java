package consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class MultipleTopicsConsumer {

    Logger log = LoggerFactory.getLogger(SingleTopicConsumer.class);

    @KafkaListener(topics = "#{'${spring.kafka.topics'}")
    public void listen(@Payload String message,
                       @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                       @Header(KafkaHeaders.OFFSET) int offset,
                       @Header(KafkaHeaders.RECEIVED_TOPIC) String topicName,
                       Acknowledgment acknowledgment) {
        log.info("Received message {0} from topic {1} with partition {2} and offset {3}",
                message, topicName, partition, offset);
        acknowledgment.acknowledge();
    }
}
