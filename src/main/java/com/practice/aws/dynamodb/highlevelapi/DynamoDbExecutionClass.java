package com.practice.aws.dynamodb.highlevelapi;

import com.practice.aws.AwsCredentialProvider;

public class DynamoDbExecutionClass {

    public static void main(String[] args) {

        DynamoDbExample dynamoDbExample = new DynamoDbExample(new AwsCredentialProvider().getAWSCredentials());
//        dynamoDbExample.saveIntoDynamoDb();
//        dynamoDbExample.loadFromDynamoDb();
//        dynamoDbExample.deleteFromDynamoDb();
//        dynamoDbExample.batchSaveInDynamoDb();
//        dynamoDbExample.queryDynamoDb();
//        dynamoDbExample.scanDynamoDb();
    }
}
