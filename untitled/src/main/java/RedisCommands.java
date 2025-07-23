import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.BlockingDeque;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

class RedisCommands
{
    public static void handleSET(OutputStream out, List<String> cmd) throws IOException
    {
            int length = cmd.size();

            String key = cmd.get(1);
            if(length > 3)
            {
                long px = Long.parseLong(cmd.get(4));
                RESPUtils.expiryTime.put(key, px + System.currentTimeMillis());
                RESPUtils.dict.put(key,cmd.get(2));
            }else {
                RESPUtils.dict.put(key,cmd.get(2));
            }

            out.write("OK\r\n".getBytes());

    }

    public static void handleGET(OutputStream out, List<String> cmd) throws IOException {
        String key = cmd.get(1);


        if(!RESPUtils.dict.containsKey(key))
        {
            RESPUtils.sendBulkString(out,null);return;
        }
        if(RESPUtils.expiryTime.containsKey(key))
        {
            long expire = RESPUtils.expiryTime.get(key);
            long currentTime = System.currentTimeMillis();
            if(expire - currentTime <= 0)
            {
                RESPUtils.sendBulkString(out,null);
                RESPUtils.expiryTime.remove(key);
                RESPUtils.dict.remove(key);
                return;
            }
        }
        String value = RESPUtils.dict.getOrDefault(key,null);
        RESPUtils.sendBulkString(out,value);
    }

    public static void handlePush(OutputStream out,List<String> cmd) throws IOException {
        boolean side = cmd.get(0).equals("RPUSH");

        String key = cmd.get(1);
        BlockingDeque<String> deque = RESPUtils.listMap.computeIfAbsent(key, k -> new LinkedBlockingDeque<>());

        for(int i = 2 ; i < cmd.size();i++)
        {
            if(side)
            deque.addLast(cmd.get(i));
            else {
                deque.addFirst(cmd.get(i));
            }
        }

        String msg = ":"+deque.size()+"\r\n";
        out.write(msg.getBytes());

    }

    public  static  void handleLRANGE(OutputStream out, List<String> cmd) throws IOException {
        String key = cmd.get(1);

        if(cmd.size() < 4)
        {
            out.write("-Err in command\r\n".getBytes());
        }

        BlockingDeque<String> deque = RESPUtils.listMap.computeIfAbsent(key, k -> new LinkedBlockingDeque<>());

        int startIndex = Integer.parseInt(cmd.get(2));
        int lastIndex = Integer.parseInt(cmd.get(3));

        if(startIndex < 0) startIndex += deque.size();
        if(lastIndex < 0) lastIndex += deque.size();

        startIndex = Math.max(startIndex, 0);
        lastIndex = Math.min(lastIndex, deque.size() - 1);

        List<String> list =  new ArrayList<>(deque);

        List<String> result = list.subList(startIndex,lastIndex+1);


        RESPUtils.sendList(out,result);

    }

    public static void handleLPOP(OutputStream out, List<String> cmd) throws IOException {
        String key = cmd.get(1);
        int count = 1;

        if (cmd.size() > 2) {
            count = Integer.parseInt(cmd.get(2));
        }

        BlockingDeque<String> deque = RESPUtils.listMap.computeIfAbsent(key, k -> new LinkedBlockingDeque<>());


        if (deque == null || deque.isEmpty()) {
            RESPUtils.sendBulkString(out, null);
            return;
        }

        List<String> result = new ArrayList<>();

        for (int i = 0; i < count && !deque.isEmpty(); i++) {
            String value = deque.pollFirst();
            if (value != null) {
                result.add(value);
            }
        }

        // Send RESP Array
        RESPUtils.sendArray(out, result);
    }
    public static void handleBLPOP(OutputStream out, List<String> cmd) throws IOException {
        String key = cmd.get(1);
        double timeoutSeconds = Double.parseDouble(cmd.get(2));

        BlockingDeque<String> deque = RESPUtils.listMap.computeIfAbsent(key, k -> new LinkedBlockingDeque<>());
        String result = null;

        try {
            if (timeoutSeconds == 0.0) {
                // Wait indefinitely
                result = deque.take();
            } else {
                long timeoutMillis = (long) (timeoutSeconds * 1000);
                result = deque.poll(timeoutMillis, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (result == null) {
            RESPUtils.sendBulkString(out, null); // timeout case
        } else {
            RESPUtils.sendArray(out, List.of(key, result)); // success case
        }
    }

    public static void handleType(OutputStream out,List<String> cmd)
    {
        //need to implement
    }
    public static void handleLMOVE(OutputStream out,List<String> cmd) throws IOException {

            if(cmd.size() <5){RESPUtils.sendBulkString(out,null);return;}
            String key = cmd.get(1);
            String target = cmd.get(2);
            BlockingDeque<String> deque = RESPUtils.listMap.get(key);
            BlockingDeque<String> destin = RESPUtils.listMap.computeIfAbsent(key, k -> new LinkedBlockingDeque<>());
            if(deque == null || deque.isEmpty())
            {
                RESPUtils.sendBulkString(out,null);return;
            }


            String pullDir = cmd.get(3);
            String pushDir = cmd.get(4);

            String val1 = (pullDir.equals("LEFT")) ? deque.pollFirst() : deque.poll();

        assert val1 != null;
        if ((pushDir.equals("RIGHT"))) {

            destin.offerLast(val1);
        } else {
            destin.offerFirst(val1);
        }

        RESPUtils.sendBulkString(out,val1);


    }





}