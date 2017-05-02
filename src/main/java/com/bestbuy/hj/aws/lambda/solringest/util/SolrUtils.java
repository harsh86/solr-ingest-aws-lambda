package com.bestbuy.hj.aws.lambda.solringest.util;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class SolrUtils {


  public static SolrInputDocument fromJson(String jsonText) {
    SolrInputDocument solrInputDocument = new SolrInputDocument();
    if (StringUtils.isBlank(jsonText)) {
      return solrInputDocument;
    }
    JSONObject jsonObject = new JSONObject(jsonText);
    if (jsonObject.length() < 1) {
      return solrInputDocument;
    }
    for (String key : JSONObject.getNames(jsonObject)) {
      Object value = jsonObject.opt(key);
      if (value instanceof JSONArray) {
        JSONArray array = (JSONArray) value;
        for (int i = 0; i < array.length(); i++) {
          solrInputDocument.addField(key, array.get(i));
        }
      } else if (key.equalsIgnoreCase("doc_boost")) {
        solrInputDocument.setDocumentBoost(Float.parseFloat(value.toString()));
      } else {
        solrInputDocument.addField(key, value);
      }
    }
    return solrInputDocument;
  }

  public static String toJSON(SolrInputDocument solrInputDoc) throws IOException, JSONException {
    JSONObject jsonObject = toJsonObject(solrInputDoc);
    return jsonObject.toString();
  }

  private static JSONObject toJsonObject(SolrInputDocument solrInputDoc) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    if (solrInputDoc != null) {
      jsonObject.put("doc_boost", solrInputDoc.getDocumentBoost());
      solrInputDoc.entrySet()
                  .forEach(entry -> {
                    SolrInputField solrInputField = entry.getValue();
                    int valueCount = solrInputField.getValueCount();
                    if (valueCount == 1) {
                      jsonObject.put(solrInputField.getName(), solrInputField.getValue());
                    } else if (valueCount > 1) {
                      jsonObject.put(solrInputField.getName(),
                          new JSONArray(solrInputField.getValues()));
                    }
                  });
    }
    return jsonObject;
  }

}
