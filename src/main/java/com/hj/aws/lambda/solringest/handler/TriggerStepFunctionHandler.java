package com.hj.aws.lambda.solringest.handler;

import java.util.UUID;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsAsyncClientBuilder;
import com.amazonaws.services.stepfunctions.model.StartExecutionRequest;
import com.amazonaws.services.stepfunctions.model.StartExecutionResult;
import com.amazonaws.util.json.Jackson;

public class TriggerStepFunctionHandler implements RequestHandler<S3Event, String> {

  @Override
  public String handleRequest(S3Event input, Context context) {
    context.getLogger()
           .log("Input: " + input);

    if (input.getRecords()
             .isEmpty()) {
      return "Empty Record . Will not trigger Step function";
    }

    AWSStepFunctions stepFunctionClient = AWSStepFunctionsAsyncClientBuilder.defaultClient();

    StartExecutionRequest startExecReq = new StartExecutionRequest().withInput("")
                                                                    .withName(UUID.randomUUID()
                                                                                  .toString())
                                                                    .withInput(
                                                                        Jackson.toJsonString(input))
                                                                    .withStateMachineArn(
                                                                        "arn:aws:states:us-west-2:328763813367:stateMachine:ProcessMetricEvent");
    StartExecutionResult stepFunctionResponse = stepFunctionClient.startExecution(startExecReq);

    return "Initialised Step Function and received status Code:"
        + stepFunctionResponse.getSdkHttpMetadata()
                              .getHttpStatusCode();
  }

}
