package com.practice.aws;

import com.amazonaws.auth.AWSCredentials;

public class AwsCredentialProvider {

    private AWSCredentials myCredentials() {
        return new AWSCredentials() {
            @Override
            public String getAWSAccessKeyId() {

                return "enter your access key";
            }

            @Override
            public String getAWSSecretKey() {
                return "enter your secret key";
            }
        };
    }

    public AWSCredentials getAWSCredentials() {
        return myCredentials();
    }
}
