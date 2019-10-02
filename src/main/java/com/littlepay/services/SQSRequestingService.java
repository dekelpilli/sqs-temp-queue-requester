package com.littlepay.services;


import com.amazonaws.AmazonClientException;
import com.amazonaws.services.sqs.AmazonSQSRequester;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;

import javax.annotation.PreDestroy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.commons.lang3.RandomUtils.nextBoolean;


@Slf4j
@Controller
@RequiredArgsConstructor
public class SQSRequestingService implements DisposableBean {

    private static final String SERIALISED_NULL = ".rO0ABXA=";

    private final AmazonSQSRequester amazonSQSRequester;

    @Value("${requester.queue-url}")
    private String queueUrl;

    @PostMapping(path = "/send")
    ResponseEntity<Void> begin() {
        sendRequests();
        log.warn("DONE");
        return ResponseEntity.noContent().build();
    }

    public void sendRequests() {
        log.warn("Starting to discover numbers...");
        short count = 0;
        do {
            for (short i = 0; i < 10; i++) {
                sendStringMessage();
                count++;
            }
            log.warn("Count: {}", count);
        } while (count < 10);
        log.warn("Done discovering numbers");
    }

    @SneakyThrows
    private void sendStringMessage() {

        SendMessageRequest sendMessageRequest = new SendMessageRequest();
        String potentialNumber = nextBoolean()
                ? RandomStringUtils.randomAlphanumeric(1)
                : RandomStringUtils.randomNumeric(1);
        sendMessageRequest.setMessageBody(potentialNumber);
        sendMessageRequest.setQueueUrl(queueUrl);
        log.warn("Checking if {} is a number...", potentialNumber);

        try {
            Message responseMessage = amazonSQSRequester.sendMessageAndGetResponse(sendMessageRequest,
                    2, TimeUnit.SECONDS);

            String response = responseMessage.getBody();

            if (response == null || response.equals(SERIALISED_NULL)) {
                log.error("\n\nRECEIVED NULL OR NULL-LIKE: {}\n\n", potentialNumber); // this never happens
            }
            log.warn("{} is {}a number!", potentialNumber,
                    Boolean.parseBoolean(response) ? "" : "not ");
        } catch (TimeoutException | AmazonClientException e) {
            log.warn("Failed to get response for {}", potentialNumber);
        }
    }

    @PreDestroy
    void goodbye() {
        log.warn("Pre-destroy stage");
    }

    @Override
    @SneakyThrows
    public void destroy() {
        log.warn("Destroy stage: shutting down began");
        amazonSQSRequester.shutdown();
        log.warn("Destroy stage: Finished shutting down temporary queue stuff");
    }
}
