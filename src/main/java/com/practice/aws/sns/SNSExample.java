package com.practice.aws.sns;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.*;

import java.util.Random;

/**
 * This is the SNS example describing some of the SNS functionality.
 * You can also createTopic(..),deleteTopic(..),subscribe(...).
 * Here, for this example, I have created the topic using aws management console.
 */
public class SNSExample {

    private AmazonSNS client;
    private static final String topicArn = "arn:aws:sns:us-east-1:595878661657:PiyushTopic";

    public SNSExample(AWSCredentials awsCredentials) {
        this.client = new AmazonSNSClient(awsCredentials);
    }

    /**
     * This method will publish the message to the topic. And all the subscription which are subscribed to the topic will get the message.
     */
    public void publishMessageToTopic() {
        int i = new Random().nextInt(100);
        PublishRequest publishRequest = new PublishRequest();
        publishRequest.withTopicArn(topicArn).withMessage("This is SNS example ::::" + i).withSubject("SNSsubject ::::" + i);
        PublishResult publishResult = client.publish(publishRequest);
        System.out.println("the publish result message id is ::::" + publishResult.getMessageId());
    }

    public void listSubscriptionsToMyTopic() {
        ListSubscriptionsByTopicRequest listSubscriptionsByTopicRequest = new ListSubscriptionsByTopicRequest();
        listSubscriptionsByTopicRequest.withTopicArn(topicArn);
        ListSubscriptionsByTopicResult listSubscriptionsByTopicResult = client.listSubscriptionsByTopic(listSubscriptionsByTopicRequest);
        for (Subscription subscription : listSubscriptionsByTopicResult.getSubscriptions()) {
            System.out.println("The subscripition is ::::" + subscription);
        }
    }
}
