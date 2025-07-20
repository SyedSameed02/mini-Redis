
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;

class RESPUtils
{
    public static final Map<String,String> dict = new ConcurrentHashMap<>();
    public static final Map<String,Long> expiryTime = new ConcurrentHashMap<>();
    public static final Map<String, BlockingDeque<String>> listMap = new ConcurrentHashMap<>();public static List<String> parseArray(InputStream in) throws IOException {
        List<String> result = new ArrayList<>();

        int prefix = in.read();
        if (prefix != '*') {
            throw new IOException("Invalid RESP array: missing *");
        }

        int arrayLength = readNumber(in);
        for (int i = 0; i < arrayLength; i++) {
            int dollar = in.read();
            if (dollar != '$') {
                throw new IOException("Expected '$' for bulk string but got: " + (char) dollar);
            }

            int len = readNumber(in);
            if (len == -1) {
                result.add(null);
                continue;
            }

            byte[] buf = new byte[len];
            int totalRead = 0;
            while (totalRead < len) {
                int read = in.read(buf, totalRead, len - totalRead);
                if (read == -1) {
                    throw new IOException("Unexpected end of stream while reading bulk string");
                }
                totalRead += read;
            }

            if (in.read() != '\r' || in.read() != '\n') {
                throw new IOException("Malformed RESP: expected CRLF after bulk string");
            }

            result.add(new String(buf, StandardCharsets.UTF_8));
        }

        return result;
    }

    private static int readNumber(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\r') {
                int next = in.read();
                if (next != '\n') {
                    throw new IOException("Malformed number: expected LF after CR");
                }
                break;
            }
            sb.append((char) b);
        }

        try {
            return Integer.parseInt(sb.toString());
        } catch (NumberFormatException e) {
            throw new IOException("Invalid number format: " + sb.toString());
        }
    }
    public static void sendBulkString(OutputStream out, String value) throws IOException {
        if(value == null)
        {
            out.write("$-1\r\n".getBytes());
        }else{
            out.write(("$"+value.length()+"\r\n"+value+"\r\n").getBytes());
        }
    }

    public static  void sendList(OutputStream out, List<String> list) throws IOException {
        out.write(('*' + String.valueOf(list.size()) + "\r\n").getBytes());
        for (String element : list) {
            sendBulkString(out, element);
        }
    }

    public static void sendArray(OutputStream out, List<String> items) throws IOException {
        if (items == null || items.isEmpty()) {
            out.write("*0\r\n".getBytes());
            return;
        }

        out.write(("*" + items.size() + "\r\n").getBytes());
        for (String item : items) {
            if (item == null) {
                out.write("$-1\r\n".getBytes()); // RESP Null Bulk String
            } else {
                out.write(("$" + item.length() + "\r\n" + item + "\r\n").getBytes());
            }
        }
    }

}