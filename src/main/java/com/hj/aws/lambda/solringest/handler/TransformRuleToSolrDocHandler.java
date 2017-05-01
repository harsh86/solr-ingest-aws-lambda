package com.hj.aws.lambda.solringest.handler;

import java.util.List;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.hj.aws.lambda.solringest.model.S3BucketEnum;
import com.hj.aws.lambda.solringest.service.AWSS3Service;

public class TransformRuleToSolrDocHandler implements RequestHandler<S3Event, Object> {

  @Override
  public Object handleRequest(S3Event input, Context context) {

    List<S3ObjectSummary> updatedEvents =
        AWSS3Service.listObjects(S3BucketEnum.RULE_STAGING.bucketName, "updated_");
    updatedEvents.stream()
                 .forEach(object -> {
                   context.getLogger()
                          .log("Event with key: " + object.getKey() + "is up for publish");
                 });
    return "Ok";
  }

}
