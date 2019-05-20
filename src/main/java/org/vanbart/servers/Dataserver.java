package org.vanbart.servers;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.util.Random;

/**
 * A minimal server which sends random data on a socket.
 */
public class Dataserver {

    private static Random random = new Random();

    private static final Logger log = LoggerFactory.getLogger(Dataserver.class);

    public static void main(String[] args) throws Exception {
        URL resource = Dataserver.class.getClassLoader().getResource("default-log4j.properties");
//        System.out.println("resource = " + resource);
        PropertyConfigurator.configure(resource);

        ServerSocket serverSocket = new ServerSocket(7777);
        log.info("Starting server.");
        while (true) {
            Socket socket = serverSocket.accept();
            log.info("Accepted connection. {}", socket);
            new Thread(() -> sendData(socket) ).start();
        }
    }

    public static void sendData(Socket socket) {
        boolean running = true;
        while (running) {
            try {
                String value = (1 + random.nextInt(6)) + " ";
                log.debug("sending data: {}", value);
                socket.getOutputStream().write(value.getBytes());
                socket.getOutputStream().flush();
                Thread.sleep(1000);
            } catch (IOException e) {
                log.warn("Got IO exception", e);
                running = false;
            } catch (InterruptedException e) {
                running = false;
            }
        }
    }

}
