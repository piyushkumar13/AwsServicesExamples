package com.practice.aws.dynamodb.lowlevelapi.dynamodbclass;

import com.practice.aws.AwsCredentialProvider;

public class DynamoDbExecutionClass {
    public static void main(String[] args) {
        DynamoDbExample dynamoDbExample = new DynamoDbExample(new AwsCredentialProvider().getAWSCredentials());
//        dynamoDbExample.putItemInDynamoDb();
//        dynamoDbExample.putItemWithConditionInDynamoDb();
//        dynamoDbExample.updateItemInDynamoDb();
//        dynamoDbExample.updateItemWithConditionInDynamoDb();
//        dynamoDbExample.deleteItemFromDynamoDb();
//        dynamoDbExample.deleteItemWithConditionInDynamoDb();
//        dynamoDbExample.queryDynamoDb();
//        dynamoDbExample.scanDynamoDb();
    }

}
