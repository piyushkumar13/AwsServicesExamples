package com.practice.aws.sqs;

import com.practice.aws.AwsCredentialProvider;

public class SQSExecutionClass {

    public static void main(String[] args) {
        AwsCredentialProvider awsCredentialProvider = new AwsCredentialProvider();
        SQSExample sqsExample = new SQSExample(awsCredentialProvider.getAWSCredentials());
        sqsExample.receiveMessage();
        //sqsExample.sendMessage();
    }
}
