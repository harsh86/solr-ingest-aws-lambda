package com.hj.aws.lambda.solringest.handler;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.bestbuy.hj.aws.lambda.solringest.util.SolrUtils;
import com.hj.aws.lambda.solringest.service.AWSS3Service;

public class SolrIndexerLambdaHandler implements RequestHandler<S3Event, String> {

  @Override
  public String handleRequest(S3Event input, Context context) {
    try {
      context.getLogger()
             .log("Recived solrDoc for publish: " + input.getRecords()
                                                         .get(0)
                                                         .getS3()
                                                         .getObject()
                                                         .getKey());

      SolrClient solrClient = new HttpSolrClient.Builder("http://blabla").build();

      List<SolrInputDocument> solrInputDocuments = toSolrInputDocuments(input, context);

      solrClient.add("rules", solrInputDocuments);
    } catch (SolrServerException e) {
      context.getLogger()
             .log("Failed ot index document to solr with exception " + e.getMessage());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }



    return "Ok";
  }

  private List<SolrInputDocument> toSolrInputDocuments(S3Event input, Context context) {
    return input.getRecords()
                .stream()
                .map(record -> {
                  try {
                    return Optional.<SolrInputDocument>of(s3ObjectToSolrInputDoc(record, context));
                  } catch (Exception e) {
                    context.getLogger()
                           .log("Failed to fetch solrDoc with key:" + record.getS3()
                                                                            .getObject()
                                                                            .getKey());
                    return Optional.<SolrInputDocument>empty();
                  }
                })
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.<SolrInputDocument>toList());
  }

  /**
   * Transformer function which converts S3Event to RuleEvent
   */
  private SolrInputDocument s3ObjectToSolrInputDoc(S3EventNotificationRecord record,
      Context context) throws Exception {
    String bucketName = record.getS3()
                              .getBucket()
                              .getName();

    String key = record.getS3()
                       .getObject()
                       .getKey()
                       .replace('+', ' ');
    context.getLogger()
           .log("Going to download s3 object with key: " + key + "from Bucket :" + bucketName);
    return SolrUtils.fromJson(AWSS3Service.getObject(bucketName, key, String.class));
  }
}
