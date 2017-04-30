package com.hj.aws.lambda.solringest;

import java.io.InputStream;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hj.aws.lambda.solringest.model.Rules;

public class DownloadMetricEventLambdaFunctionHandler implements RequestHandler<S3Event , Object> {

  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public Object handleRequest(S3Event input, Context context) {
    try {
      context.getLogger()
          .log("Input: " + input);
      

      input.getRecords()
          .stream()
          .flatMap(record -> {
            try {
              return s3ObjectToRule(record, context).getRules()
                  .stream();
            } catch (final Exception e) {
              throw new RuntimeException(e);
            }
          })
          .forEach(p -> {
            context.getLogger()
                .log("Received rule for term: " + p.getTerm() + " and for skuid : " + p.getSkuid()
                    + " with score: " + p.getScore());
          });
    } catch (final Exception e) {
      context.getLogger()
          .log("Failed to parse input: " + input);
      throw new RuntimeException(e);
    }

    // TODO: implement your handler
    return "OK";
  }

  private Rules s3ObjectToRule(S3EventNotificationRecord record, Context context) throws Exception {
    String srcBucket = record.getS3()
        .getBucket()
        .getName();
    // Object key may have spaces or unicode non-ASCII characters.
    String srcKey = record.getS3()
        .getObject()
        .getKey()
        .replace('+', ' ');

    context.getLogger()
        .log("Going to download s3 object with key: " + srcKey + "from Bucket :" + srcBucket);

    AmazonS3 s3Client = new AmazonS3Client();
    S3Object s3Object = s3Client.getObject(new GetObjectRequest(srcBucket, srcKey));
    InputStream objectData = s3Object.getObjectContent();
    return mapper.readValue(objectData, Rules.class);
  }

}
