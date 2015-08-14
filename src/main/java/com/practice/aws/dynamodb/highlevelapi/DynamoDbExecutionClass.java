package com.practice.aws.dynamodb.highlevelapi;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.practice.aws.AwsCredentialProvider;

public class DynamoDbExecutionClass {

    public static void main(String[] args) {
        AmazonDynamoDBClient amazonDynamoDBClient = new AmazonDynamoDBClient(new AwsCredentialProvider().getAWSCredentials());
        DynamoDBMapper dynamoDBMapper = new DynamoDBMapper(amazonDynamoDBClient);
    }
}
