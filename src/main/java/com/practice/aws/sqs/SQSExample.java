package com.practice.aws.sqs;


import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;

/**
 * Operations provided by AmazonSQSClient : -
 * createQueue(..),deleteQueue(..),sendMessage(..),receiveMessage(..),deleteMessage(..) etc
 * Following I have shown some of the functions of the SQS. Here, I have already created the queue using aws console.
 */
public class SQSExample {

    private static final String QUEUEURL = "https://sqs.us-east-1.amazonaws.com/595878661657/PiyushQueue";
    private AmazonSQSClient sqsClient;

    public SQSExample(AWSCredentials awsCredentials) {
        sqsClient = new AmazonSQSClient(awsCredentials);
    }

    /**
     * we can send the batch messages also.
     */
    public void sendMessage() {
        SendMessageRequest sendMessageRequest = new SendMessageRequest()
                .withQueueUrl(QUEUEURL)
                .withMessageBody("This is the java sqs example");
        SendMessageResult sendMessageResult = sqsClient.sendMessage(sendMessageRequest);
        System.out.println("The sqs message id is " + sendMessageResult.getMessageId());
    }

    /**
     * The aws receive message can only receive one message at a time.
     */
    public void receiveMessage() {

        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest();
        receiveMessageRequest.setQueueUrl(QUEUEURL);
        //This wait time seconds is used for long polling. It will wait for 20 seconds till it doesn't get any message.
        receiveMessageRequest.setWaitTimeSeconds(20);
        ReceiveMessageResult receiveMessageResult = sqsClient.receiveMessage(receiveMessageRequest);
        System.out.println("The recieved message result is ::" + receiveMessageResult);
        for (Message message : receiveMessageResult.getMessages()) {
            System.out.println("message is " + message);
        }
        deleteReceivedMessage(receiveMessageResult.getMessages().get(0).getReceiptHandle());
    }

    /**
     * we can delete the batch messages also.
     *
     * @param receiptHandle
     */
    public void deleteReceivedMessage(String receiptHandle) {
        DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
        deleteMessageRequest.setQueueUrl(QUEUEURL);
        deleteMessageRequest.setReceiptHandle(receiptHandle);

        try {
            sqsClient.deleteMessage(deleteMessageRequest);
            System.out.println("The received message is deleted successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
