package com.clearedgeit.accumulo.flume;

import org.apache.flume.Context;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.json.simple.parser.ParseException;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import javax.annotation.Nullable;

/**
 * Created by jveiga on 19-12-2015.
 */


public class JsonEventIntercepter implements Interceptor {


    private JSONParser jsonParser = new JSONParser();
    private static final Logger LOG = Logger.getLogger(JsonEventIntercepter.class);

    public JsonEventIntercepter(){

    }

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(final Event event) {
        final Map<String, String> headers = event.getHeaders();
        JSONObject parsedObject = null;
        try {
            parsedObject = (JSONObject) jsonParser.parse(new String(event.getBody()));
        } catch (ParseException e) {
            e.printStackTrace();
            LOG.info("Received this log message that is not formatted in json: " + new String(event.getBody()) + "\n");
            return null;
        }
        try {
            headers.put(Constants.JOBID, getParameter(Constants.JOBID, parsedObject));
            headers.put(Constants.CLIENTID, getParameter(Constants.CLIENTID, parsedObject));
            headers.put(Constants.RESULTYPE, getParameter(Constants.RESULTYPE, parsedObject));
        }catch (FailedToParseObjectException e){
            LOG.info("Failed to get parameter from json object" + e.getMessage());

            return event;
        }
        return event;
    }

    private @Nullable String getParameter(final String parameter, final JSONObject object) throws FailedToParseObjectException {
        String value;

        try {
            value = (String) object.get(parameter);
            if (null == value){
                value = Constants.DEFAULT;
            }
        } catch (ClassCastException e) {
              throw new FailedToParseObjectException(parameter);
//            return "";
        }
        return value;

    }

    @Override
    public List<Event> intercept(final List<Event> list) {
        return null;
    }

    @Override
    public void close() {

    }

    private class FailedToParseObjectException extends Exception {
        public FailedToParseObjectException(final String parameter) {
            super(parameter);
        }
    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new JsonEventIntercepter();
        }

        @Override
        public void configure(final Context context) {

        }
    }

    public static class Constants {
        static final String JOBID = "job_id";
        static final String CLIENTID = "client_id";
        static final String RESULTYPE = "result_type";
        static final String DEFAULT = "default";
    }
}
