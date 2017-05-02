package com.hj.aws.lambda.solringest.handler;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

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
      //TODO:: Check if ML can send a batchId
      String batchId = UUID.randomUUID()
                           .toString();
      return saveSearchMetricEvents(input, context, batchId);
    } catch (final Exception e) {
      context.getLogger()
             .log("Failed to parse input: " + input);
      throw new RuntimeException(e);
    }
  }

  /**
   * Function which transforms and saves S3Event as RuleEvent
   * 
   * @throws Exception
   */
  private String saveSearchMetricEvents(S3Event input, Context context, String batchId)
      throws Exception {
    Set<String> changeSet = new HashSet<String>();
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
             changeSet.add(saveMetricRule(rule));
           } catch (Exception e) {
             throw new RuntimeException(e);
           }
         });
    return saveChangeSet(changeSet, batchId);
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

  private String saveMetricRule(Rule incomingEvent) throws Exception {
    String bucketName = S3BucketEnum.RULE_STAGING.bucketName;
    String ruleBucketKey =
        RuleEvent.toRuleBucketKey(incomingEvent.getTerm(), RuleType.SEARCH_METRIC);
    SearchMetricRuleEvent mergedRule = mergeRule(bucketName, ruleBucketKey, incomingEvent);
    AWSS3Service.putObject(bucketName, ruleBucketKey, mergedRule);
    return ruleBucketKey;
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

    // Need to update aldready exisiting termRule
    SearchMetricRuleEvent mergedRule =
        AWSS3Service.getObject(bucketName, key, SearchMetricRuleEvent.class);
    if (mergedRule != null) {
      mergedRule.addSkuInfo(incomingRule);
      return mergedRule;
    }
    // New termRule.
    mergedRule = new SearchMetricRuleEvent().forTerm(incomingRule.getTerm())
                                            .withSkuInfo(incomingRule);
    return mergedRule;
  }

  /**
   * Persists changeSet: terms modified or added as part of this batch.
   */

  private String saveChangeSet(Set<String> changeSet, String batchId) throws Exception {
    String bucketName = S3BucketEnum.RULE_STAGING.bucketName;
    String ruleBucketKey = new StringBuilder("changeset_").append(RuleType.SEARCH_METRIC.name())
                                                          .append("_")
                                                          .append(batchId)
                                                          .toString();
    AWSS3Service.putObject(bucketName, ruleBucketKey, changeSet);
    return ruleBucketKey;
  }

}
