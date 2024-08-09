package org.apache.doris.flink.sink.util;

import java.net.HttpURLConnection;
import java.net.URL;

public class Test2 {

    public static void main(String[] args) {
        String host = "10.16.10.6:8021";
        tryHttpConnection(host);
    }

    public static boolean tryHttpConnection(String host) {
        try {
            System.out.println("try to connect host " + host);
            host = "http://" + host;
            URL url = new URL(host);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            int defaultConnectTimeout = connection.getConnectTimeout();
            int defaultReadTimeout = connection.getReadTimeout();
            System.out.println(defaultConnectTimeout);
            System.out.println(defaultReadTimeout);
            int responseCode = connection.getResponseCode();
            System.out.println(responseCode);
            System.out.println(connection.getResponseMessage());
            System.out.println("connect sucessful true");
            return true;
        } catch (Exception ex) {
            System.out.println("Failed to connect to host:" + host + ex);
            return false;
        }
    }
}
