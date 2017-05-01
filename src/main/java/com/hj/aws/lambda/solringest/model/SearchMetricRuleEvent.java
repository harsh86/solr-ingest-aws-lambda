package com.hj.aws.lambda.solringest.model;

import java.util.HashSet;
import java.util.Set;

public class SearchMetricRuleEvent extends RuleEvent {
  private String term;
  private Set<Rule> skuInfo = new HashSet<Rule>();


  public SearchMetricRuleEvent forTerm(String term) {
    this.term = term;
    return this;
  }

  public SearchMetricRuleEvent withSkuInfo(Rule rule) {
    this.skuInfo.add(rule);
    return this;
  }

  public SearchMetricRuleEvent withSkuInfo(Set<Rule> rule) {
    this.skuInfo.addAll(rule);
    return this;
  }

  public String getTerm() {
    return term;
  }

  public Set<Rule> getSkuInfo() {
    return skuInfo;
  }

  public void addSkuInfo(Rule rule) {
    this.skuInfo.add(rule);
  }

  public void addSkuInfo(Set<Rule> rule) {
    this.skuInfo.addAll(rule);
  }


}
