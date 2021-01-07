import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


@Service
@RequiredArgsConstructor
public class TopicPublisher {

    @Value("${spring.kafka.timeout}")
    private String timeout;

    private final KafkaTemplate<String, String> kafkaTemplate;

    private final ObjectMapper objectMapper;

    public void send(final Object data, final String topic) throws JsonProcessingException {
        String message = objectMapper.writeValueAsString(data);
        ListenableFuture<SendResult<String, String>> future = this.kafkaTemplate.send(topic, message);
        try {
            future.get(Integer.parseInt(timeout), TimeUnit.SECONDS);
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            e.printStackTrace();
        }

    }
}
