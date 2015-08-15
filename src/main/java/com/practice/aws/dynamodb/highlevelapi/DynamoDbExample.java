package com.practice.aws.dynamodb.highlevelapi;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;

import java.util.List;

import static java.util.Arrays.asList;

/**
 * This class uses the dynamo db high level apis.
 * High level interfaces for dynamo db let you define the relationships between the objects in your program with the
 * database tables that store those objects data. This high level api actually does the object persistence mapping. This
 * allows us to write the object centric code rather than the database centric code. Using high level api, we can avoid
 * extra code we need to write using low level api for putting, getting, updating item etc.
 * Using high level api provide us operations like saving updating and deleting(DML operations) but using this we cant
 * create table, delete table or alter it(DDL operations).
 */
public class DynamoDbExample {

    private DynamoDBMapper dynamoDBMapper;

    public DynamoDbExample(AWSCredentials awsCredentials) {
        this.dynamoDBMapper = new DynamoDBMapper(new AmazonDynamoDBClient(awsCredentials));
    }

    /**
     * This method will save the object(or item) into the dynamo db. For the updation of the item, same method will be
     * used.
     */
    public void saveIntoDynamoDb() {

        EmployeeDetails employeeDetails = getEmployeeDetails("vjbest@gmail.com", "vj", "My company", "Bangalore");
        dynamoDBMapper.save(employeeDetails);
        System.out.println("Employee detail is save successfully.");
    }

    /**
     * This method is used for getting the item from the dynamo db. If you provide only hashKey and thre are more than
     * one item with that hashkey, then load will throw exception.
     * If hashKey and rangeKey provided in the call, does not exist in the dynamodb, then, it will return the null object.
     */
    public void loadFromDynamoDb() {
        EmployeeDetails employeeDetails = dynamoDBMapper.load(EmployeeDetails.class, "piyush.k9013@gmail.com", "piyush kumar");
        System.out.println(employeeDetails);
    }

    /**
     * This method will delete the item with the given hashKey and rangeKey provided in the EmployeeDetails object.
     */
    public void deleteFromDynamoDb() {

        EmployeeDetails employeeDetails = getEmployeeDetails("abc@gamil.com", "xy@gmail.com", null, null);

        dynamoDBMapper.delete(employeeDetails);
        System.out.println("Item deleted successfully.");
    }

    /**
     * This method will save the items in the batch. We need to provide the list of object which we want to save as an
     * item in the dynamo db.
     */
    public void batchSaveInDynamoDb() {
        EmployeeDetails employeeDetails1 = getEmployeeDetails("ptr@gmail.com", "Puneet Rao", "XZ", "Delhi");
        EmployeeDetails employeeDetails2 = getEmployeeDetails("ag@yahoo.com", "Ashutosh Gov", null, null);
        dynamoDBMapper.batchSave(asList(employeeDetails1, employeeDetails2));
        System.out.println("Batch saved successfully.");
    }

    /**
     * This method will delete the items from the dynamo db corresponding to the hashKeys and rangeKeys provided in the
     * list of objects passed in the batchDelete call.
     */
    public void batchDeleteFromDynamoDb() {
        EmployeeDetails employeeDetails1 = getEmployeeDetails("ptr@gmail.com", "Puneet Rao", "XZ", "Delhi");
        EmployeeDetails employeeDetails2 = getEmployeeDetails("ag@yahoo.com", "Ashutosh Gov", null, null);
        dynamoDBMapper.batchDelete(asList(employeeDetails1, employeeDetails2));
        System.out.println("Batch deleted successfully.");
    }

    /**
     * This method will query the dynamo db.
     * If dynamo db primary key is made up of only hashKey, we can not use the query method to query dynamo db.
     * If dynamo db primary key is made up of hashKey and rangeKey, we can use the query method to query dynamo db using
     * either hashKey or hashKey + rangeKey.
     * =======================================================================
     * using queryFilter
     * Condition rangeKeyCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(new AttributeValue().withS("thermo"));
     * Map<String,Condition> queryFilter = new HashMap<String, Condition>();
     * queryFilter.put("company",rangeKeyCondition);
     * DynamoDBQueryExpression<EmployeeDetails> queryExpression = new DynamoDBQueryExpression<EmployeeDetails>()
     * .withHashKeyValues(employeeDetails).withQueryFilter(queryFilter);
     * ========================================================================
     * we can apply the queryFilter on the non-primary key attribute. If we apply to the primary key attribute i.e hashkey
     * or rangeKey, it will throw the exception.
     * note : any filter, we can apply only on the non-primary key attribute, otherwise, it will throw exception.
     */
    public void queryDynamoDb() {
        EmployeeDetails employeeDetails = getEmployeeDetails("piyush.k9013@gmail.com", null, null, null);
        Condition rangeKeyCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(new AttributeValue().withS("thermo"));
        DynamoDBQueryExpression<EmployeeDetails> queryExpression = new DynamoDBQueryExpression<EmployeeDetails>()
                .withHashKeyValues(employeeDetails).withRangeKeyCondition("name", rangeKeyCondition);
        List<EmployeeDetails> details = dynamoDBMapper.query(EmployeeDetails.class, queryExpression);
        for (EmployeeDetails detail : details) {
            System.out.println(detail);
        }
    }

    /**
     * This method will scan the whole dynamo db.
     * We can apply the filters on the result returned by the scan using DynamoDbScanExpression. In this
     * DynamoDBScanExpression, we can either set the withScanFilter or addScanFilter. Only difference between them is,
     * in addScanFilter we can add multiple filters whereas with withScanFilter we can have only one filter.
     * =====================================================================
     * withScanFilter :-
     * Map<String,Condition> scanFilter1 = new HashMap<String, Condition>();
     * scanFilter1.put("company",new Condition().withComparisonOperator(ComparisonOperator.EQ)
     * .withAttributeValueList(new AttributeValue().withS("thermo")));
     * DynamoDBScanExpression scanExpression = new DynamoDBScanExpression().withScanFilter(scanFilter1);
     * =====================================================================
     */
    public void scanDynamoDb() {

        DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();
        scanExpression.addFilterCondition("emailId", new Condition().withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue().withS("piyush.k9013@gmail.com")));
        scanExpression.addFilterCondition("name", new Condition().withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue().withS("piyush")));
        List<EmployeeDetails> details = dynamoDBMapper.scan(EmployeeDetails.class, scanExpression);
        for (EmployeeDetails detail : details) {
            System.out.println(detail);
        }
    }

    private EmployeeDetails getEmployeeDetails(String emailId, String empName, String company, String compLocation) {
        EmployeeDetails employeeDetails = new EmployeeDetails();
        employeeDetails.setEmpName(empName);
        employeeDetails.setEmailId(emailId);
        employeeDetails.setCompany(company);
        employeeDetails.setCompanyLocation(compLocation);
        return employeeDetails;
    }
}
