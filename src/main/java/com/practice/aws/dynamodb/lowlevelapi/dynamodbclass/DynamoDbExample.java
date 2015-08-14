package com.practice.aws.dynamodb.lowlevelapi.dynamodbclass;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.*;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;

import java.util.Iterator;

/**
 * Here we have used Class DynamoDB as a client which is wrapper over AmazonDynamoDBClient.
 * DynamoDB client is threadsafe.
 */
public class DynamoDbExample {
    private static final String TABLENAME = "PiyushTable";
    private DynamoDB dynamoDB;

    public DynamoDbExample(AWSCredentials awsCredentials) {
        dynamoDB = new DynamoDB(new AmazonDynamoDBClient(awsCredentials));
    }

    /**
     * This method will put an item in the dynamo db. The call table.putItem returns empty PutItemOutcome object
     */
    public void putItemInDynamoDb() {

        Table table = dynamoDB.getTable(TABLENAME);
        Item item = new Item().withPrimaryKey("emailId", "piyush.k3@gmail.com", "name", "piyush").withString("location", "Del").withString("company", "thermo");
        table.putItem(item);
        System.out.println("Item put successfully");

    }

    /**
     * This method will put an item in the dynamo db based on the defined condition.
     */
    public void putItemWithConditionInDynamoDb() {
        Table table = dynamoDB.getTable(TABLENAME);
        Item item = new Item().withPrimaryKey("emailId", "piyr2@ch.com", "name", "piyush kumar").withString("location", "Dl").withString("company", "therm");
        PutItemSpec putItemSpec = new PutItemSpec().withItem(item).withConditionExpression("attribute_not_exists(#lc) and attribute_not_exists(#cm)").withNameMap(new NameMap().with("#lc", "location").with("#cm", "company"));

        try {
            table.putItem(putItemSpec);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
        System.out.println("Item put successfully");
    }

    /**
     * This method will update the item for the given primary key.
     * If the attribute which we want to update does not exist for the given primary key, then, updateItem call will
     * create the attribute and then, put the set value in that attribute as provided in the update expression.
     * Here, in this method we have used #loc because the actual attribute name was location, which was conflicting with
     * the aws reserved word.Hence, we have provided our variable with the placeholder # and then, we are providing its
     * name in the nameMap. We can also achieve this thing using following:- <br/><br/>
     * ================================================================================<br/>
     * Map<String,String> expressionsName = new HashMap<String, String>();<br/>
     * expressionsName.put("#loc","location");<br/>
     * <br/>
     * Map<String,Object> expressionsValue = new HashMap<String, Object>();<br/>
     * expressionsValue.put(":lr","location");<br/>
     * table.updateItem("emailId", "piyush.k3@gmail.com", "name", "piyush kumar","set #loc=:lr",expressionsName,expressionsValue);
     * <br/>
     * ==================================================================================<br/>
     * On successful call of the table.updateItem(updateItemSpec); returns the null UpdateItemOutcome.
     */
    public void updateItemInDynamoDb() {

        Table table = dynamoDB.getTable(TABLENAME);
        UpdateItemSpec updateItemSpec = new UpdateItemSpec();
        PrimaryKey primaryKey = new PrimaryKey("emailId", "piyush.k3@gmail.com", "name", "piyush kumar");
        //refer above java doc for using #loc
        updateItemSpec.withPrimaryKey(primaryKey).withUpdateExpression("set #loc=:lr")
                .withNameMap(new NameMap().with("#loc", "location")).withValueMap(new ValueMap()
                .withString(":lr", "thermo")).withReturnValues(ReturnValue.UPDATED_NEW);

        UpdateItemOutcome updateItemOutcome = table.updateItem(updateItemSpec);
        System.out.println("the udpated value is ::: " + updateItemOutcome.getItem());
        System.out.println("The updateItemResult is::::: " + updateItemOutcome.getUpdateItemResult());
        System.out.println("update succeeded on the item(row) :::" + table.getItem(primaryKey));

    }

    /**
     * This method will update the column(or attribute) value iff satisfies the given condition, otherwise, throws the
     * CheckFailedException.
     */
    public void updateItemWithConditionInDynamoDb() {
        Table table = dynamoDB.getTable(TABLENAME);
        UpdateItemSpec updateItemSpec = new UpdateItemSpec();
        PrimaryKey primaryKey = new PrimaryKey("emailId", "piyr2@ch.com", "name", "piyush kumar");

        updateItemSpec.withPrimaryKey(primaryKey).withUpdateExpression("set #com=:c")
                .withConditionExpression("#com=:th")
                .withNameMap(new NameMap().with("#com", "company"))
                .withValueMap(new ValueMap().withString(":c", "thermo").withString(":th", "therm"))
                .withReturnValues(ReturnValue.UPDATED_NEW);

        UpdateItemOutcome updateItemOutcome = table.updateItem(updateItemSpec);
        System.out.println("the udpated value is ::: " + updateItemOutcome.getItem());
        System.out.println("The updateItemResult is::::: " + updateItemOutcome.getUpdateItemResult());
        System.out.println("update succeeded on the item(row) :::" + table.getItem(primaryKey));
    }

    /**
     * This method will delete the item for the given primary key.
     * deleteItem(primaryKey) returns the empty DeleteItemOutcome in success deletion.
     * if call dynamoDb.deleteItem(primaryKey) does not find any matching primaryKey as given, it wont throw any exception
     * and returned DeleteItemOutcome will again be empty in this case too.
     */
    public void deleteItemFromDynamoDb() {

        Table table = dynamoDB.getTable(TABLENAME);
        PrimaryKey primaryKey = new PrimaryKey("emailId", "ac@gamil.com", "name", "xyz@gmail.com");
        DeleteItemOutcome deleteItemOutcome = table.deleteItem(primaryKey);
        System.out.println("deletion of an item is completed" + deleteItemOutcome);
    }

    /**
     * This method deletes the item conditionally, otherwise, will throw exception.
     */
    public void deleteItemWithConditionInDynamoDb() {

        Table table = dynamoDB.getTable(TABLENAME);
        PrimaryKey primaryKey = new PrimaryKey("emailId", "afc@gamil.com", "name", "xrz@gmail.com");
        DeleteItemSpec deleteItemSpec = new DeleteItemSpec().withPrimaryKey(primaryKey).withConditionExpression("#com=:c")
                .withNameMap(new NameMap().with("#com", "company"))
                .withValueMap(new ValueMap().with(":c", "th"))
                .withReturnValues(ReturnValue.ALL_OLD);
        DeleteItemOutcome deleteItemOutcome = table.deleteItem(deleteItemSpec);
        System.out.println("deletion of an item is completed" + deleteItemOutcome.getItem());
    }

    /**
     * This method query the dynamodb with the given hash key. Note that we can only query the dynamo db with only
     * hash key(mandatory) and range key(optional). If we want to query table using other fields than we have to define
     * local secondary index(created using table's primary key and any other attribute which act as alternate key) and
     * global secondary index(created with attributes of the table does not mandate the presence of table's primary key).
     * <p/>
     * Here filterExpression filters out the results of the query before giving result.
     * ProjectionExpression will only return the specified attributes.
     * There are other options for refining the result and putting conditions like withQueryFilter.
     */
    public void queryDynamoDb() {
        Table table = dynamoDB.getTable(TABLENAME);

        QuerySpec querySpec = new QuerySpec().withHashKey("emailId", "piyush.k3@gmail.com")
                .withProjectionExpression("emailId,company")
                .withFilterExpression("#loc=:th").withNameMap(new NameMap().with("#loc", "location"))
                .withValueMap(new ValueMap().withString(":th", "thermo1"));
        ItemCollection<QueryOutcome> itemCollection = table.query(querySpec);

        Iterator<Item> iterator = itemCollection.iterator();
        while (iterator.hasNext()) {
            System.out.println("the item is :::::" + iterator.next());
        }
    }

    /**
     * This method will scan all the rows in the table.
     * We can filter the returned rows using FilterExpression and project the specified attributes using ProjectionExpression.
     */
    public void scanDynamoDb() {

        Table table = dynamoDB.getTable(TABLENAME);
        ScanSpec scanSpec = new ScanSpec().withProjectionExpression("emailId,company");
        ItemCollection<ScanOutcome> itemCollection = table.scan(scanSpec);
        Iterator<Item> iterator = itemCollection.iterator();
        while (iterator.hasNext()) {
            System.out.println("The item is :::::" + iterator.next());
        }
    }
}
