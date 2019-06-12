package org.vanbart.flinkjobs;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.vanbart.functions.WebSocketSourceFunction;

import java.util.Map;

@Slf4j
public class Main {

    public static final int CHECKPOINTING_INTERVAL_MS = 5000;
    private static final String JOB_NAME = "Flink Streaming Java API Skeleton";

    /**
     * Main Flink job.
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ObjectMapper objectMapper = new ObjectMapper();

        env.setParallelism(4);

        ParameterTool paramTool = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(paramTool);

        DataStreamSource<String> mySocketStream = env.addSource(new WebSocketSourceFunction("ws://stream.meetup.com/2/rsvps"));
        mySocketStream.map(new RsvpIdExtractor()).print();
//        mySocketStream.print();

        env.enableCheckpointing(CHECKPOINTING_INTERVAL_MS);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.execute(JOB_NAME);
    }

    public static class RsvpIdExtractor extends RichMapFunction<String, String> {

        final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public String map(String value) throws Exception {
            Map<String, Object> mapped = objectMapper.readValue(value, Map.class);
            Object rsvp = mapped.get("rsvp_id");
            return rsvp != null ? rsvp.toString() : "null" ;
        }
    }
}

