package com.littlepay.configurations;


import com.amazonaws.PredefinedClientConfigurations;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.AmazonSQSRequester;
import com.amazonaws.services.sqs.AmazonSQSRequesterClientBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;


@Configuration
public class SQSConfiguration {

    @Bean
    public AWSCredentials awsCredentials(@Value("${keys.access}") String access,
                                         @Value("${keys.secret}") String secret) {
        return new BasicAWSCredentials(access, secret);
    }

    @Bean
    public AWSCredentialsProvider awsCredentialsProvider(AWSCredentials awsCredentials) {
        return new AWSStaticCredentialsProvider(awsCredentials);
    }

    @Bean
    public Map<String, String> temporaryQueueAttributes(@Autowired(required = false) RetryPolicy retryPolicy,
                                                        ObjectMapper objectMapper,
                                                        @Value("${requester.dlq-arn}") String dlqArn)
            throws JsonProcessingException {

        Map<String, String> temporaryQueueAttributes = new HashMap<>();
        temporaryQueueAttributes.put("RedrivePolicy", getRedrivePolicyString(dlqArn, objectMapper, retryPolicy));
        temporaryQueueAttributes.put("MessageRetentionPeriod", Long.toString(TimeUnit.MINUTES.toSeconds(5L)));

        return temporaryQueueAttributes;
    }

    private static String getRedrivePolicyString(String dlqArn, ObjectMapper objectMapper, RetryPolicy retryPolicy)
            throws JsonProcessingException {
        Map<String, String> redrivePolicyAttribute = new HashMap<>();
        if (retryPolicy != null) {
            redrivePolicyAttribute.put("maxReceiveCount", Integer.toString(retryPolicy.getMaxErrorRetry()));
            redrivePolicyAttribute.put("deadLetterTargetArn", dlqArn);
        }
        return objectMapper.writeValueAsString(redrivePolicyAttribute);
    }

    @Bean
    public AmazonSQSRequester amazonSQSRequester(@Value("${requester.queue-prefix}") String prefix,
                                                 AmazonSQS amazonSQS,
                                                 Map<String, String> temporaryQueueAttributes) {
        return AmazonSQSRequesterClientBuilder.standard()
                .withInternalQueuePrefix(prefix)
                .withAmazonSQS(amazonSQS)
                .withQueueAttributes(temporaryQueueAttributes)
                .build();
    }

    @Bean
    @ConditionalOnExpression("${requester.redrive}")
    public RetryPolicy retryPolicy() {
        return PredefinedRetryPolicies.getDefaultRetryPolicy();
    }

    @Bean
    public AmazonSQS amazonSQS(AWSCredentialsProvider awsCredentialsProvider,
                               @Autowired(required = false) RetryPolicy retryPolicy) {
        AmazonSQSClientBuilder amazonSQSClientBuilder = AmazonSQSClientBuilder
                .standard()
                .withRegion(Regions.AP_SOUTHEAST_2)
                .withCredentials(awsCredentialsProvider);

        if (retryPolicy != null) {
            amazonSQSClientBuilder.withClientConfiguration(
                    PredefinedClientConfigurations.defaultConfig()
                            .withRetryPolicy(retryPolicy));
        }

        return amazonSQSClientBuilder.build();
    }
}
