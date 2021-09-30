package org.example.basicapp;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;

import java.nio.file.Path;
import java.nio.file.Paths;


public class App {

    public static void main(String[] args) {
        final String USAGE = "\n" +
                "Usage:\n" +
                "put <action> <endPoint> <bucketName> <objectPath>\n\n" +
                "list <action> <endPoint> <bucketName> \n\n" +
                "Where:\n" +
                "  action - the action to run action can be put or list.\n" +
                "  endPoint - the url of the S3 server. \n" +
                "  bucketName - the Amazon S3 bucket to upload an object into.\n" +
                "  objectPath - the path where the file is located (for example, C:/AWS/book2.pdf). \n\n";

        if (args.length < 2) {
            System.out.println(USAGE);
            System.exit(1);
        }
        String action = args[0];
        if (!action.equals("put") && !action.equals("list")) {
            System.out.println("action can be put or list");
            return;
        }
        String endPoint = args[1];
        if (endPoint.isEmpty()) {
            System.out.println("endPoint cannot be empty");
            return;
        }
        if (action.equals("put")) {
            String bucketName = args[2];
            String objectPath = args[3];
            Path p = Paths.get(objectPath);
            try {
                PutObjectResult result = s3(endPoint).putObject(new PutObjectRequest(bucketName, String.valueOf(p.getFileName()), p.toFile().getAbsoluteFile()));
                System.out.println("put object : " + p.getFileName() + " was saved successful to bucket : " + bucketName);
            } catch (Exception e) {
                System.err.println(e.getMessage());
                System.exit(1);
            }
        }
        if (action.equals("list")) {
            String bucketName = args[2];
            ObjectListing objectListing = s3(endPoint).listObjects(new ListObjectsRequest().withBucketName(bucketName));
            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                System.out.println(" - " + objectSummary.getKey() + "  " + "(size = " + objectSummary.getSize() + ")");
            }
        } else {
            System.out.println("invalid action");
            System.exit(1);
        }
        System.exit(0);
    }

    static AmazonS3 s3(String endPoint) {
        return AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(getCredentials()))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endPoint, "us-east-1"))
                .build();
    }

    static AWSCredentials getCredentials() {
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            System.out.println("Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (~/.aws/credentials), and is in valid format.");
        }
        return credentials;
    }
}


