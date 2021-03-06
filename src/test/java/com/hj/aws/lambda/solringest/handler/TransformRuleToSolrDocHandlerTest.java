package com.hj.aws.lambda.solringest.handler;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.services.lambda.runtime.Context;

import com.amazonaws.services.lambda.runtime.events.S3Event;

/**
 * A simple test harness for locally invoking your Lambda function handler.
 */
public class TransformRuleToSolrDocHandlerTest {

    private static String input;

    @BeforeClass
    public static void createInput() throws IOException {
        input = "changeset_SEARCH_METRIC_9c6a8289-2177-4224-8845-4c6a6a611c42";
    }

    private Context createContext() {
        TestContext ctx = new TestContext();

        // TODO: customize your context here if needed.
        ctx.setFunctionName("Your Function Name");

        return ctx;
    }

    @Test
    public void testTransformRuleToSolrDocHandler() {
        TransformRuleToSolrDocHandler handler = new TransformRuleToSolrDocHandler();
        Context ctx = createContext();

        Object output = handler.handleRequest(input, ctx);

        // TODO: validate output here if needed.
        if (output != null) {
            System.out.println(output.toString());
        }
    }
}
