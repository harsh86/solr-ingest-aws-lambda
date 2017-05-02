package com.hj.aws.lambda.solringest.handler;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.bestbuy.hj.aws.lambda.solringest.util.SolrUtils;
import com.hj.aws.lambda.solringest.model.RuleEvent;
import com.hj.aws.lambda.solringest.model.RuleType;
import com.hj.aws.lambda.solringest.model.S3BucketEnum;
import com.hj.aws.lambda.solringest.model.SearchMetricRuleEvent;
import com.hj.aws.lambda.solringest.service.AWSS3Service;

public class TransformRuleToSolrDocHandler implements RequestHandler<String, Object> {
  @Override
  public String handleRequest(String changeSetId, Context context) {
    context.getLogger()
           .log("Input: " + changeSetId);

    if (StringUtils.isEmpty(changeSetId)) {
      new RuntimeException("Invalid changeSetId cannot Process event.");
    }
    try {
      Set<String> updatedEvents =
          AWSS3Service.getObject(S3BucketEnum.RULE_STAGING.bucketName, changeSetId, HashSet.class);
      processChangeSet(context, updatedEvents);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return "Ok";
  }

  private void processChangeSet(Context context, Set<String> updatedEvents) {
    updatedEvents.stream()
                 .map(key -> {
                   try {
                     context.getLogger()
                            .log("Event with key: " + key + "is up for publish");
                     return Optional.<SearchMetricRuleEvent>of(AWSS3Service.getObject(
                         S3BucketEnum.RULE_STAGING.bucketName, key, SearchMetricRuleEvent.class));
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
                     saveSolrDoc(solrDoc);
                   } catch (Exception e) {
                     context.getLogger()
                            .log("Failed to save solr doc with id " + solrDoc.get("id")
                                                                             .getValue()
                                                                             .toString()
                                + "is up for publish");
                     throw new RuntimeException(e);
                   }
                 });
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

  private void saveSolrDoc(SolrInputDocument solrInputDocument) throws Exception {

    String bucketName = S3BucketEnum.RULE_SOLR_DOC.bucketName;
    String ruleBucketKey = RuleEvent.toRuleBucketKey(solrInputDocument.getField("term")
                                                                      .getValue()
                                                                      .toString(),
        RuleType.SEARCH_METRIC);
    ClientUtils.toXML(solrInputDocument);
    AWSS3Service.putObject(bucketName, ruleBucketKey,  SolrUtils.toJSON(solrInputDocument));

  }
}
