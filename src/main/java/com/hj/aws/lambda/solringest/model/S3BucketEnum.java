package com.hj.aws.lambda.solringest.model;

public enum S3BucketEnum {

  RULE_STAGING("hj-aws-s3-rule-staging"),
  SEARCH_METRIC("hj-aws-s3-search-metrics");
  
  public String bucketName;

  S3BucketEnum(String bucketName){
    this.bucketName = bucketName;
  }
  
}
