package com.hj.aws.lambda.solringest.handler;

import java.io.IOException;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.hj.aws.lambda.solringest.model.Rule;
import com.hj.aws.lambda.solringest.model.RuleEvent;
import com.hj.aws.lambda.solringest.model.RuleType;
import com.hj.aws.lambda.solringest.model.Rules;
import com.hj.aws.lambda.solringest.model.S3BucketEnum;
import com.hj.aws.lambda.solringest.model.SearchMetricRuleEvent;
import com.hj.aws.lambda.solringest.service.AWSS3Service;

public class DownloadMetricEventLambdaFunctionHandler implements RequestHandler<S3Event, Object> {



  @Override
  public Object handleRequest(S3Event input, Context context) {
    try {
      context.getLogger()
             .log("Input: " + input);
      saveSearchMetricEvents(input, context);
    } catch (final Exception e) {
      context.getLogger()
             .log("Failed to parse input: " + input);
      throw new RuntimeException(e);
    }
    return "OK";
  }

  /**
   * Function which transforms and saves S3Event as RuleEvent
   */
  private void saveSearchMetricEvents(S3Event input, Context context) {
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
         .forEach(rule -> {
           try {
             context.getLogger()
                    .log("Saving rule for term: " + rule.getTerm() + " and for skuid : "
                        + rule.getSkuid() + " with score: " + rule.getScore());
             saveMetricRule(rule);
           } catch (Exception e) {
             throw new RuntimeException(e);
           }
         });
  }

  /**
   * Transformer function which converts S3Event to RuleEvent
   */
  private Rules s3ObjectToRule(S3EventNotificationRecord record, Context context) throws Exception {
    String bucketName = record.getS3()
                              .getBucket()
                              .getName();

    String key = record.getS3()
                       .getObject()
                       .getKey()
                       .replace('+', ' ');
    context.getLogger()
           .log("Going to download s3 object with key: " + key + "from Bucket :" + bucketName);
    return AWSS3Service.getObject(bucketName, key, Rules.class);
  }

  /**
   * Persists incoming searchMetric Events
   */

  private boolean saveMetricRule(Rule incomingEvent) throws Exception {
    String bucketName = S3BucketEnum.RULE_STAGING.bucketName;
    String ruleBucketKey =
        RuleEvent.toRuleBucketKey(incomingEvent.getTerm(), RuleType.SEARCH_METRIC);
    SearchMetricRuleEvent mergedRule = mergeRule(bucketName, ruleBucketKey, incomingEvent);
    ruleBucketKey = mergedRule.isUpdated() ? "updated_" + ruleBucketKey : ruleBucketKey;
    return AWSS3Service.putObject(bucketName, ruleBucketKey, mergedRule);
  }

  /**
   * Method merges skuid-score info onto existing metric rule for same term. TODO:// Need
   * refactoring and re-thinking to solve conflict resolution if concurrent updates exists.
   * 
   * @throws IOException
   * @throws JsonMappingException
   * @throws JsonParseException
   */
  private static SearchMetricRuleEvent mergeRule(String bucketName, String key, Rule incomingRule)
      throws Exception {
    // If updated in this batch need to merge with updated
    SearchMetricRuleEvent mergedRule =
        AWSS3Service.getObject(bucketName, "updated_" + key, SearchMetricRuleEvent.class);
    if (mergedRule != null) {
      mergedRule.addSkuInfo(incomingRule);
      mergedRule.updated(true);
      AWSS3Service.deleteObjects(bucketName, "updated_" + key);
      return mergedRule;
    }
    // Need to update aldready exisiting termRule
    mergedRule = AWSS3Service.getObject(bucketName, key, SearchMetricRuleEvent.class);
    if (mergedRule != null) {
      mergedRule.addSkuInfo(incomingRule);
      mergedRule.updated(true);
      AWSS3Service.deleteObjects(bucketName, key);
      return mergedRule;
    }
    // New termRule.
    mergedRule = new SearchMetricRuleEvent().forTerm(incomingRule.getTerm())
                                            .withSkuInfo(incomingRule)
                                            .updated(false);
    return mergedRule;
  }

}
