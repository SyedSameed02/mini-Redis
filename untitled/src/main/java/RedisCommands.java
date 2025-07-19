import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

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

        String key = command.get(1);
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



    public static void handleRPUSH(List<String> command, OutputStream out, Map<String, Deque<String>> listMap) throws IOException {

        String key = command.get(1);

        Deque<String> deque = listMap.computeIfAbsent(key, k->new LinkedList<>());

        synchronized (deque) {
            for (int i = 2; i < command.size(); i++) {
                String value = command.get(i);
                deque.addLast(value);
            }
            deque.notifyAll();
        }
        out.write((":" + deque.size() + "\r\n").getBytes());

    }
    public static void handleLPUSH(List<String> command, OutputStream out, Map<String, Deque<String>> listMap) throws IOException {

        String key = command.get(1);

        Deque<String> deque = listMap.computeIfAbsent(key, k->new LinkedList<>());

        synchronized (deque) {
            for (int i = 2; i < command.size(); i++) {
                String value = command.get(i);
                deque.addFirst(value);
            }
            deque.notifyAll();
        }
        out.write((":" + deque.size() + "\r\n").getBytes());

    }

    public static void handleLLen(List<String> command,OutputStream out,Map<String,Deque<String>> listMap ) throws IOException
    {
        String key = command.get(1);
        Deque<String> deque = listMap.get(key);
        int size = (deque == null) ? 0 :deque.size();

        out.write((":"+size + "\r\n").getBytes());
    }

    public static void handleLange(List<String> command, OutputStream out, Map<String, Deque<String>> listMap) throws IOException {

        String key = command.get(1);
        int start = Integer.parseInt(command.get(2));
        int end = Integer.parseInt(command.get(3));

        Deque<String> deque =  listMap.getOrDefault(key,new LinkedList<>());

        List<String> list = new ArrayList<>(deque);

        int size = list.size();

        if (start < 0) start+=size;
        if(end < 0) end += size;

        start = Math.max(0,start);
        end = Math.min(end,size-1);

        List<String> subList = (start > end) ?new ArrayList<>() : list.subList(start,end+1);

        StringBuilder sb=  new StringBuilder("*" +subList.size()+"\r\n");
        for(String s : subList)
        {
            sb.append("$").append(s.length()).append("\r\n").append(s).append("\r\n");
        }

        out.write(sb.toString().getBytes());

    }


    public static void handleLPOP(List<String> command, OutputStream out, Map<String, Deque<String>> listMap) throws IOException {

        String key = command.get(1);
        int count = (command.size() > 2)?Integer.parseInt(command.get(2)) : 1;

        Deque<String> deque = listMap.get(key);
        if(deque == null || deque.isEmpty())
        {
            RESPUtils.sendBulkString(out,"");return;
        }

        List<String> popped = new ArrayList<>();

        for(int i =0;i<count && !deque.isEmpty();i++)
        {
            popped.add(deque.pollFirst());
        }
        RESPUtils.sendArray(out,popped);

    }


    public static void handleBLPOP(List<String> command, OutputStream out, Map<String, Deque<String>> listMap) throws IOException {

        String key = command.get(1);

        String value = null;

        Deque<String> deque = listMap.computeIfAbsent(key,k->new LinkedList<>());

        double timePeriod = Double.parseDouble(command.get(2));

        long waitTime = (long)(timePeriod*1000);

        synchronized (deque)
        {
            if(waitTime == 0 && deque.isEmpty())
            {
               try {
                   deque.wait();

               } catch (InterruptedException e) {
                   throw new RuntimeException(e);
               }
            }else
            {
                long endTime = System.currentTimeMillis() + waitTime;

                while (deque.isEmpty())
                {
                    long sleepTime = endTime - System.currentTimeMillis();
                    if(sleepTime <= 0) {
                        RESPUtils.sendBulkString(out, value);
                        return;
                    }else{
                        try
                        {
                            deque.wait(sleepTime);
                            if(deque.isEmpty())
                            {
                                RESPUtils.sendBulkString(out,value);return;
                            }
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }


            }
            value = deque.pollFirst();

        }


        StringBuilder sb = new StringBuilder("*2\r\n");
        sb.append("$").append(key.length()).append("\r\n").append(key).append("\r\n")
                .append("$").append(value.length()).append("\r\n").append(value);


        out.write(sb.toString().getBytes());
    }
}
