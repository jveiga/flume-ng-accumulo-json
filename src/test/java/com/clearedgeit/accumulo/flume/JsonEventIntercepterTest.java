package com.clearedgeit.accumulo.flume;

import com.google.common.base.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Created by jveiga on 19-12-2015.
 */
public class JsonEventIntercepterTest {

    @Test
    public void testMessage1() {
        final String jobid = "jobid";
        final String clientid = "clientid";
        final String resultType = "result_type";
        String body = newBody(jobid, clientid, resultType);
        Context context = new Context();

        Interceptor.Builder builder = new JsonEventIntercepter.Builder();
        builder.configure(context);
        Interceptor interceptor = builder.build();

        Event event = EventBuilder.withBody(body, Charsets.UTF_8);

        event = interceptor.intercept(event);
        Map<String,String> headers = event.getHeaders();
        Assert.assertEquals(headers.get(JsonEventIntercepter.Constants.JOBID), jobid);
        Assert.assertEquals(headers.get(JsonEventIntercepter.Constants.CLIENTID), clientid );
        Assert.assertEquals(headers.get(JsonEventIntercepter.Constants.RESULTYPE), resultType);

    }

    @Test
    public void testExceptionOnWrongFormat(){

    }

    private String newBody(final String jobid, final String clientid, final String resultType) {
        return "{\"result_type\":\"" +resultType + "\"," +
                "\"provider\":\"min-29-9051-usnj-dev\"," +
                "\"origin\":\"portscan\"," +
                "\"src\":{" +
                "\"ip\":\"23.58.195.116\"," +
                "\"port\":80," +
                "\"protocol\":\"tcp\"" +
                "}," +
                "\"job_id\":\""+ jobid +"\","+
                "\"client_id\":\""+ clientid + "\","+
                "\"ts\":1446504620000," +
                "\"client_id\":\""+ clientid + "\" "+
                "}";
    }
}
