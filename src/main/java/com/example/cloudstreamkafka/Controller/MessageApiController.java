package com.example.cloudstreamkafka.Controller;

import com.example.cloudstreamkafka.models.Message;
import com.example.cloudstreamkafka.services.ConsumerService;
import com.example.cloudstreamkafka.services.KafkaSenderExample;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/message")
public class MessageApiController {
    private final KafkaSenderExample senderExample;

    @Autowired
    public MessageApiController(KafkaSenderExample senderExample) {
        this.senderExample = senderExample;
    }


    @PostMapping
    public ResponseEntity<Message> producingMessage(@RequestBody Message message) {
        senderExample.sendMessageWithCallback("stream-api-topic-kafka", message);
        return new ResponseEntity<>(message, HttpStatus.OK);
    }


    @GetMapping
    public ResponseEntity<Message> getMessage() {
        return new ResponseEntity<>(new Message("Hello", "sad"), HttpStatus.OK);
    }


    @GetMapping("/start-consumer/{offset}")
    public String startConsume(@PathVariable long offset) {
        var consumerService = new ConsumerService<String, Object>();
        var result = consumerService.startConsumer("stream-api-topic-kafka", offset, 1);
        return "Consumer starting.... with result :" + result.value();
    }
}
