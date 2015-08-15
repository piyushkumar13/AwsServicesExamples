package com.practice.aws.dynamodb.lowlevelapi.usingamazondynamodbclient;

import com.practice.aws.AwsCredentialProvider;

public class DynamoDbExecutionClass {

    public static void main(String[] args) {
        DynamoDbExample dynamoDbExample = new DynamoDbExample(new AwsCredentialProvider().getAWSCredentials());
//        uncomment following to execute methods.
//        dynamoDbExample.putItemInDynamoDb();
//        dynamoDbExample.getItemFromDynamoDb();
//        dynamoDbExample.deleteItemFromDynamoDb();
//        dynamoDbExample.updateItemInDynamoDb();
//        dynamoDbExample.scanDynamoDb();
//        dynamoDbExample.queryDynamoDb();
    }
}
