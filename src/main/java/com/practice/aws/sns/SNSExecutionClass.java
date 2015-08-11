package com.practice.aws.sns;

import com.practice.aws.AwsCredentialProvider;

public class SNSExecutionClass {

    public static void main(String[] args) {
        SNSExample snsExample = new SNSExample(new AwsCredentialProvider().getAWSCredentials());
        snsExample.publishMessageToTopic();
        snsExample.listSubscriptionsToMyTopic();
    }
}
