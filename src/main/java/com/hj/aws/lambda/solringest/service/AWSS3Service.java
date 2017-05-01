package com.hj.aws.lambda.solringest.service;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.json.Jackson;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AWSS3Service {

  private static final ObjectMapper mapper = new ObjectMapper();
  private static AmazonS3 s3Client = new AmazonS3Client();
  private static final String CONTENT_TYPE = "application/json";

  public static <T> T getObject(String bucketName, String key, Class<T> clazz)
      throws Exception {
    try {
      S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, key));
      InputStream objectData = s3Object.getObjectContent();
      return mapper.readValue(objectData, clazz);
    } catch (AmazonServiceException e) {
      return null;

    }
  }

  public static boolean putObject(String bucketName, String key, Object object)
      throws AmazonServiceException {
    try {
      s3Client.putObject(toPutobjectRequest(bucketName, key, Jackson.toJsonString(object)));
      return true;
    } catch (AmazonServiceException e) {
      throw e;
    }
  }

  public static List<S3ObjectSummary> listObjects(String bucketName) {
    return listObjects(bucketName, null);
  }

  public static List<S3ObjectSummary> listObjects(String bucketName, String prefix) {
    List<S3ObjectSummary> s3ObjectSummaries = new ArrayList<S3ObjectSummary>();
    ObjectListing objectListing;
    do {
      ListObjectsRequest listObjectsRequest = toListObjectRequest(bucketName, prefix);
      objectListing = s3Client.listObjects(listObjectsRequest);
      s3ObjectSummaries.addAll(objectListing.getObjectSummaries());
      listObjectsRequest.setMarker(objectListing.getNextMarker());
    } while (objectListing.isTruncated());
    return s3ObjectSummaries;
  }

  public static void deleteObjects(String bucketName, String key) {
    s3Client.deleteObject(new DeleteObjectRequest(bucketName, key));
  }



  private static PutObjectRequest toPutobjectRequest(String bucketName, String key,
      String content) {
    byte[] fileContentBytes = content.getBytes(StandardCharsets.UTF_8);
    InputStream fileInputStream = new ByteArrayInputStream(fileContentBytes);
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentType(CONTENT_TYPE);
    metadata.setContentLength(fileContentBytes.length);
    return new PutObjectRequest(bucketName, key, fileInputStream, metadata);
  }

  private static ListObjectsRequest toListObjectRequest(String bucketName, String prefix) {
    ListObjectsRequest objectsRequest = new ListObjectsRequest().withBucketName(bucketName);
    if (prefix == null) {
      return objectsRequest;
    }
    return objectsRequest.withPrefix(prefix);
  }
}
