package org.apache.doris.flink.sink.util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

public class PortScanner {
    private static final int TIMEOUT = 2000; // 超时时间为 2000 毫秒

    public static boolean isPortOpen(String ip, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(ip, port), TIMEOUT);
            return true; // 端口开放
        } catch (IOException e) {
            return false; // 端口未开放或无法连接
        }
    }

    public static void main(String[] args) {
        String ip = "10.16.10.6"; // 替换为实际的服务器 IP 地址
        int port = 8001; // 替换为你想要检查的端口号

        if (isPortOpen(ip, port)) {
            System.out.println("端口 " + port + " 在 " + ip + " 上是开放的。");
        } else {
            System.out.println("端口 " + port + " 在 " + ip + " 上是关闭的或无法连接。");
        }
    }
}
