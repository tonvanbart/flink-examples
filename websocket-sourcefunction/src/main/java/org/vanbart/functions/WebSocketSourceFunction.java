package org.vanbart.functions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.ws.WebSocket;
import org.asynchttpclient.ws.WebSocketListener;
import org.asynchttpclient.ws.WebSocketUpgradeHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class WebSocketSourceFunction extends RichSourceFunction<String> {

    private boolean running = true;
    transient AsyncHttpClient client;
    transient BoundRequestBuilder boundRequestBuilder;
    transient WebSocketUpgradeHandler.Builder webSocketListener;
    private BlockingQueue<String> messages = new ArrayBlockingQueue<>(100);
    private final String websocketUrl;

    private static final Logger log = LoggerFactory.getLogger(WebSocketSourceFunction.class);

    public WebSocketSourceFunction(String websocketUrl) {
        this.websocketUrl = websocketUrl;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        WebSocketUpgradeHandler webSocketUpgradeHandler = webSocketListener.addWebSocketListener(
                new WebSocketListener() {

                    @Override
                    public void onOpen(WebSocket webSocket) {
                    }

                    @Override
                    public void onClose(WebSocket webSocket, int i, String s) {
                    }

                    @Override
                    public void onError(Throwable throwable) {
                    }

                    @Override
                    public void onTextFrame(String payload, boolean finalFragment, int rsv) {
                        log.debug("onTextFrame({})", hash(payload));
                        if (payload != null) {
                            try {
                                messages.put(payload);
                            } catch (InterruptedException e) {
                                log.error("Interrupted!", e);
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                }).build();
        boundRequestBuilder.execute(webSocketUpgradeHandler).get();

        while (running) {
            ctx.collect(messages.take());
        }
        running = false;
    }

    @Override
    public void cancel() {
        log.info("cancel function called");
        running = false;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        log.info("open function called");
        super.open(parameters);
        client = Dsl.asyncHttpClient();
        boundRequestBuilder = client.prepareGet(websocketUrl);
        webSocketListener = new WebSocketUpgradeHandler.Builder();
    }

    /**
     * Hashes the given payload for logging (for quick inspection for duplicates).
     * If debug is not enabled, return a fixed string.
     * @param input
     * @return
     */
    private String hash(String input) {
        if (input == null || !log.isDebugEnabled()) {
            return "-- NULL --";
        }

        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(input.getBytes());
            byte[] digest = md.digest();
            return DatatypeConverter.printHexBinary(digest).toUpperCase();
        } catch (NoSuchAlgorithmException e) {
            log.error("Cound not instantiate MD5", e);
            return "--NOT CALCULATED--";
        }
    }
}
