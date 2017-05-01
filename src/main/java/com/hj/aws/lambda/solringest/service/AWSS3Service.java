package com.hj.aws.lambda.solringest.service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.json.Jackson;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AWSS3Service {

  private static final ObjectMapper mapper = new ObjectMapper();
  private static AmazonS3 s3Client = new AmazonS3Client();
  private static final String CONTENT_TYPE = "application/json";

  public static <T> T getObject(String bucketName, String key, Class<T> clazz)
      throws IOException, JsonParseException, JsonMappingException {
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



  private static PutObjectRequest toPutobjectRequest(String bucketName, String key,
      String content) {
    byte[] fileContentBytes = content.getBytes(StandardCharsets.UTF_8);
    InputStream fileInputStream = new ByteArrayInputStream(fileContentBytes);
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentType(CONTENT_TYPE);
    metadata.setContentLength(fileContentBytes.length);
    return new PutObjectRequest(bucketName, key, fileInputStream, metadata);
  }
}
