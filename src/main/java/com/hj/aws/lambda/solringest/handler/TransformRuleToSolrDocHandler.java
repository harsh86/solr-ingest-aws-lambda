package com.hj.aws.lambda.solringest.handler;

import java.util.List;
import java.util.Optional;
import java.util.PrimitiveIterator.OfDouble;

import javax.management.RuntimeErrorException;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.hj.aws.lambda.solringest.model.Rule;
import com.hj.aws.lambda.solringest.model.RuleEvent;
import com.hj.aws.lambda.solringest.model.RuleType;
import com.hj.aws.lambda.solringest.model.S3BucketEnum;
import com.hj.aws.lambda.solringest.model.SearchMetricRuleEvent;
import com.hj.aws.lambda.solringest.service.AWSS3Service;

public class TransformRuleToSolrDocHandler implements RequestHandler<S3Event, Object> {

  @Override
  public Object handleRequest(S3Event input, Context context) {

    List<S3ObjectSummary> updatedEvents =
        AWSS3Service.listObjects(S3BucketEnum.RULE_STAGING.bucketName, "updated_");

    updatedEvents.stream()
                 .map(object -> {
                   try {
                     context.getLogger()
                            .log("Event with key: " + object.getKey() + "is up for publish");
                     return Optional.<SearchMetricRuleEvent>of(
                         AWSS3Service.getObject(S3BucketEnum.RULE_STAGING.bucketName,
                             object.getKey(), SearchMetricRuleEvent.class));
                   } catch (Exception e) {
                     context.getLogger()
                            .log("Failed to transform updated events");
                     return Optional.<SearchMetricRuleEvent>empty();
                   }
                 })
                 .filter(Optional::isPresent)
                 .map(Optional::get)
                 .map(this::toSolrDoc)
                 .forEach(solrDoc -> {
                   try {
                     saveMetricRule(solrDoc);
                   } catch (Exception e) {
                     context.getLogger()
                            .log("Failed to save solr doc with id " + solrDoc.get("id")
                                                                             .getValue()
                                                                             .toString()
                                + "is up for publish");
                     throw new RuntimeException(e);
                   }
                 });
    return "Ok";
  }

  private SolrInputDocument toSolrDoc(SearchMetricRuleEvent ruleEvent) {
    SolrInputDocument solrInputDocument = new SolrInputDocument();
    solrInputDocument.addField("id", ruleEvent.getTerm()
                                              .concat("_")
                                              .concat(RuleType.SEARCH_METRIC.name()));

    solrInputDocument.setField("term", ruleEvent.getTerm());
    ruleEvent.getSkuInfo()
             .forEach(p -> {
               solrInputDocument.addField("queryParams", p.getSkuid()
                                                          .concat("^")
                                                          .concat(p.getScore()));
             });
    return solrInputDocument;
  }

  private void saveMetricRule(SolrInputDocument solrInputDocument) throws Exception {

    String bucketName = S3BucketEnum.RULE_SOLR_DOC.bucketName;
    String ruleBucketKey = RuleEvent.toRuleBucketKey(solrInputDocument.getField("term")
                                                                      .getValue()
                                                                      .toString(),
        RuleType.SEARCH_METRIC);
    AWSS3Service.putObject(bucketName, ruleBucketKey, solrInputDocument);

  }
}
