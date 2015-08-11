package com.practice.aws.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;
import com.practice.aws.AwsCredentialProvider;

import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;

public class DynamoDbExample {

    private AmazonDynamoDBClient amazonDynamoDBClient;
    private static final String TABLENAME = "PiyushTable";

    public DynamoDbExample() {
        this.amazonDynamoDBClient = new AmazonDynamoDBClient(new AwsCredentialProvider().getAWSCredentials());
    }

    /**
     * This method will put the items in the dynamodb table. While putting in the dyamodb table, dynamodb client first
     * looks for the primary key and if it does not exist, then, it will put the item(row) in the table and if it exists,
     * it will replace the whole row with the same primary key but with the new values. It looks like we are just updating
     * some attributes value but it actually replaces the whole item(row) in the dynamodb.
     */
    public void putItemInDynamoDb() {

        PutItemRequest putItemRequest = new PutItemRequest();
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("emailId", new AttributeValue("piyush.k9013@gmail.com"));
        item.put("name", new AttributeValue("piyush kumar"));
        item.put("location", new AttributeValue("Bangalore"));
        //item.put("company",new AttributeValue("thermo"));
        putItemRequest.withTableName(TABLENAME).withItem(item);
        try {
            amazonDynamoDBClient.putItem(putItemRequest);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Put operation is successfully completed.");
    }

    /**
     * This method will get the attribute values for the given primary key.Here, primary key is the combination of the
     * Hash key and the Range key.
     */
    public void getItemFromDynamoDb() {

        GetItemRequest getItemRequest = new GetItemRequest();
        Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
        key.put("emailId", new AttributeValue("abc@gamil.com"));
        key.put("name", new AttributeValue("xy@gmail.com"));
        getItemRequest.withTableName(TABLENAME).withKey(key);
        GetItemResult getItemResult = new GetItemResult();
        try {
            getItemResult = amazonDynamoDBClient.getItem(getItemRequest);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (getItemResult.getItem() == null) {
            System.out.println("no key is found");
            System.exit(0);
        }
        System.out.println("" + getItemResult.getItem());
    }

    /**
     * This method will delete the item for the given primary key.
     */
    public void deleteItemFromDynamoDb() {
        DeleteItemRequest deleteItemRequest = new DeleteItemRequest();
        Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
        key.put("emailId", new AttributeValue("piyush.k9013@gmail.com"));
        key.put("name", new AttributeValue("piyush kumar"));
        // Here we require to set the returnedValues as ALL_OLD because the values which is getting deleted, we want
        // them to be returned in the DeleteItemResult.
        deleteItemRequest.withTableName(TABLENAME).withKey(key).withReturnValues(ReturnValue.ALL_OLD);
        DeleteItemResult deleteItemResult = new DeleteItemResult();
        try {
            deleteItemResult = amazonDynamoDBClient.deleteItem(deleteItemRequest);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (deleteItemResult == null) {
            System.out.println("No key found to delete");
            System.exit(0);
        }
        System.out.println("Deleted item is :::: " + deleteItemResult.getAttributes());
    }

    /**
     * This method will update the non-key attributes. If we try to update the key attribute, it will throw the
     * validation exception.
     * This method actually looks for the primary key, if key exists it will update the non-key field of the key attribute
     * of the item. If not, it will create a new item.
     * If the primary key exists but attribute which we want to create doesn't, then, it will create a new attribute with
     * the provided value.
     */
    public void updateItemInDynamoDb() {
        UpdateItemRequest updateItemRequest = new UpdateItemRequest();
        Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
        key.put("emailId", new AttributeValue("piyush.k9013@gmail.com"));
        key.put("name", new AttributeValue("piyush kumar"));

        Map<String, AttributeValueUpdate> updateAttributeValue = new HashMap<String, AttributeValueUpdate>();
        updateAttributeValue.put("location", new AttributeValueUpdate().withValue(new AttributeValue("Bangalore")));

        updateItemRequest.withTableName(TABLENAME).withKey(key).withAttributeUpdates(updateAttributeValue).withReturnValues(ReturnValue.UPDATED_OLD);
        UpdateItemResult updateItemResult = new UpdateItemResult();
        try {
            updateItemResult = amazonDynamoDBClient.updateItem(updateItemRequest);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (updateItemResult.getAttributes() == null) {
            System.out.println("Either no key is found to be updated or key is found but no attribute is found to be updated.");
            System.exit(0);
        }
        System.out.println("The value is updated successfully. The old values which are updated are :::" + updateItemResult.getAttributes());
    }

    /**
     * This method will can the whole dynamo db table. Either you want to get the all the columns or specific column that we
     * define in the ScanRequest.
     */
    public void scanDynamoDb() {
        ScanRequest scanRequest = new ScanRequest();
        scanRequest.withTableName(TABLENAME);

        //to get specific columns we will use the following command.
        // scanRequest.withTableName(TABLENAME).withSelect(Select.SPECIFIC_ATTRIBUTES).withAttributesToGet(asList("emailId"));
        ScanResult scanResult = amazonDynamoDBClient.scan(scanRequest);
        System.out.println("The scanned result is :::" + scanResult.getItems());
    }

    /**
     * This method is used for querying the dynamodb. We can query dynamodb using only hash key and the range key. Hashkey is
     * mandatory to give while quering the dynamodb and range key is optional.But we can query the dynamodb using only hashkey
     * and dynamodb other attributes are not allowed for querying dynamodb. If we want to query dynamodb using attributes other
     * than hashkey and range key, we need to make local secondary index or global secondary index.
     */
    public void queryDynamoDb() {
        QueryRequest queryRequest = new QueryRequest();
        Condition condition = new Condition();
        condition.withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(asList(new AttributeValue("piyush.k9013@gmail.com")));
        Map<String, Condition> queryMap = new HashMap<String, Condition>();
        queryMap.put("emailId", condition);
        queryRequest.withTableName(TABLENAME).withKeyConditions(queryMap).withSelect(Select.SPECIFIC_ATTRIBUTES).withAttributesToGet(asList("name"));
        QueryResult queryResult = amazonDynamoDBClient.query(queryRequest);
        if (queryResult.getItems() != null) {
            System.out.println("the query result is :::" + queryResult.getItems());
        }
    }

}
