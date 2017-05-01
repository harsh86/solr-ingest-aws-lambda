package com.hj.aws.lambda.solringest.model;

public class Rule {
  private String term;
  private String skuid;
  private String score;

  public Rule() {
    super();
  }

  public String getTerm() {
    return term;
  }

  public String getSkuid() {
    return skuid;
  }

  public String getScore() {
    return score;
  }

  public Rule withTerm(String term) {
    this.term = term;
    return this;
  }

  public Rule withSkuid(String skuid) {
    this.skuid = skuid;
    return this;
  }

  public Rule withScore(String score) {
    this.score = score;
    return this;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((score == null) ? 0 : score.hashCode());
    result = prime * result + ((skuid == null) ? 0 : skuid.hashCode());
    result = prime * result + ((term == null) ? 0 : term.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Rule other = (Rule) obj;
    if (score == null) {
      if (other.score != null)
        return false;
    } else if (!score.equals(other.score))
      return false;
    if (skuid == null) {
      if (other.skuid != null)
        return false;
    } else if (!skuid.equals(other.skuid))
      return false;
    if (term == null) {
      if (other.term != null)
        return false;
    } else if (!term.equals(other.term))
      return false;
    return true;
  }
  

}
