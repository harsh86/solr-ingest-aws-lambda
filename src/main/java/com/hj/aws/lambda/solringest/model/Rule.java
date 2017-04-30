package com.hj.aws.lambda.solringest.model;

public class Rule {
	private String term;
	private String skuid;
	private String score;

	public Rule() {
    super();
  }

  public Rule(String term, String skuId, String score) {
		super();
		this.term = term;
		this.skuid = skuId;
		this.score = score;
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

}
