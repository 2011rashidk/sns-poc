package com.hm.sqs.demo.controller;

import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.aws.messaging.core.QueueMessagingTemplate;
import org.springframework.cloud.aws.messaging.listener.SqsMessageDeletionPolicy;
import org.springframework.cloud.aws.messaging.listener.annotation.SqsListener;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("api/sqs")
@Slf4j
public class SQSController {
    @Autowired
    private QueueMessagingTemplate queueMessagingTemplate;

    @Autowired
    private AmazonSNSClient amazonSNSClient;

    private String topicARN = "arn:aws:sns:us-west-2:720174271738:MyFirstSNSTopic";

    @GetMapping("addSubscription/{email}")
    public String addSubscription(@PathVariable String email) {
        SubscribeRequest subscribeRequest = new SubscribeRequest(topicARN, "email", email);
        amazonSNSClient.subscribe(subscribeRequest);
        return "Please check your mail";
    }

    @GetMapping("sendNotification")
    public String sendNotification() {
        PublishRequest publishRequest = new PublishRequest(topicARN, "Hello, What's up?", "My SNS subject");
        amazonSNSClient.publish(publishRequest);
        return "Email sent!";
    }

    @Value("${cloud.aws.end-point.uri}")
    private String awsEndPoint;

    @GetMapping("send/{message}")
    public void sendMessage(@PathVariable String message) {
        queueMessagingTemplate.send(awsEndPoint, MessageBuilder.withPayload(message).build());
    }

    @SqsListener(value = "MyFirstQueue")
    public void readMessage(String message) {
        log.info("Message from SQS Queue: {}", message);
    }
}
