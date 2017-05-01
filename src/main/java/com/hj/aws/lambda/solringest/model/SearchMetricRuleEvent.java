package com.hj.aws.lambda.solringest.model;

import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SearchMetricRuleEvent extends RuleEvent {
  private String term;
  private Set<Rule> skuInfo = new HashSet<Rule>();
  @JsonInclude(Include.NON_EMPTY)
  private boolean isUpdated;


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
  

  public boolean isUpdated() {
    return isUpdated;
  }

  public SearchMetricRuleEvent updated(boolean isUpdated) {
    this.isUpdated = isUpdated;
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
