package org.apache.doris.flink.sink.util;

import org.apache.commons.net.telnet.TelnetClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class TelnetExample {
    private TelnetClient telnet;
    private InputStream in;
    private OutputStream out;

    public TelnetExample(String server, int port) {
        telnet = new TelnetClient();
        try {
            telnet.connect(server, port);
            in = telnet.getInputStream();
            out = telnet.getOutputStream();
            //            System.out.println(telnet.isAvailable());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void sendCommand(String command) {
        try {
            out.write((command + "\n").getBytes());
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String readResponse() {
        StringBuilder response = new StringBuilder();
        try {
            int len;
            byte[] buffer = new byte[1024];
            while ((len = in.read(buffer)) != -1) {
                response.append(new String(buffer, 0, len));
                if (len < 1024) {
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response.toString();
    }

    public void disconnect() {
        try {
            telnet.disconnect();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        TelnetExample telnetExample = new TelnetExample("10.16.10.6", 8021);
        telnetExample.sendCommand("aaa");
        System.out.println(telnetExample.readResponse());
        telnetExample.disconnect();
    }
}
