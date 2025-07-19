import java.io.IOException;
import java.io.OutputStream;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class RedisCommands {
    public static void handleSet(List<String> command, OutputStream out, Map<String, String> map, Map<String, Long> expiryMap) throws IOException {

        String key = command.get(1);
        String value = command.get(2);

        map.put(key, value);

        if (command.size() > 3 && command.get(3).equalsIgnoreCase("PX")) {
            long expiryTime = System.currentTimeMillis() + Long.parseLong(command.get(4));
            expiryMap.put(key, expiryTime);
        }
        out.write("+OK\r\n".getBytes());
    }

    public static void handleGet(List<String> command, OutputStream out, Map<String, String> map, Map<String, Long> expiryMap) throws IOException {

        String key = command.get(0);
        if(expiryMap.containsKey(key) && expiryMap.get(key) < System.currentTimeMillis())
        {
            map.remove(key);
            expiryMap.remove(key);
            out.write("$-1\r\n".getBytes());
        }else{
            String value = map.get(key);
            RESPUtils.sendBulkString(out,value);
        }

    }



    public static void handleRPUSH(List<String> command, OutputStream out, Map<String, Deque<String>> listMap) {

        String key = command.get(1);

        Deque<String> deque = listMap.computeIfAbsent(key, k->new LinkedList<>());


        for( int i = 2 ;i< command.size();i++)
        {
            String value = command.get(i);
            deque.addLast(value);
        }
    }
    public static void handleLPUSH(List<String> command, OutputStream out, Map<String, Deque<String>> listMap) {

        String key = command.get(1);

        Deque<String> deque = listMap.computeIfAbsent(key, k->new LinkedList<>());


        for( int i = 2 ;i< command.size();i++)
        {
            String value = command.get(i);
            deque.addFirst(value);
        }
    }

    public static void handleLLen(List<String> command,OutputStream out,Map<String,Deque<String>> listMap ) throws IOException
    {
        String key = command.get(1);
        Deque<String> deque = listMap.get(key);
        int size = (deque == null) ? 0 :deque.size();

        out.write((":"+size + "\r\n").getBytes());
    }
}
