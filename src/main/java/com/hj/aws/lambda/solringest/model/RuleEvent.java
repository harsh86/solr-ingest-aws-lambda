package com.hj.aws.lambda.solringest.model;

public abstract class RuleEvent {

  public static String toRuleBucketKey(String term, RuleType ruleType) {
    return new StringBuffer(ruleType.name()).append("_")
                                            .append(term)
                                            .toString();
  }

}
