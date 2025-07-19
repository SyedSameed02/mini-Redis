import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;



public class ClientHandler  extends Thread {

    private final Socket socket;

    private static final Map<String,String> map = new ConcurrentHashMap<>();
    private static final Map<String,Long> expiryMap =  new ConcurrentHashMap<>();
    private Map<String, Deque<String>> listMap = new ConcurrentHashMap<>();

    public ClientHandler(Socket clientSocket) {
        this.socket = clientSocket;
    }


    public void run() {

        try(
                InputStream in =socket.getInputStream();
                OutputStream out =socket.getOutputStream();
        ) {

            while(true)
            {
                char  ch = (char)in.read();
                if(ch != '*')
                {
                    out.write("-ERR Protocol erorr\r\n".getBytes());
                    continue;
                }
                List<String> command = readCommand(in);
                String cmd = command.get(0).toUpperCase();


                switch (cmd)
                {
                    case "ECHO":
                        RESPUtils.sendBulkString(out,(command.size() >1)?command.get(1) : "");
                        break;
                    case "PING":
                        RESPUtils.sendBulkString(out,"PONG");
                        break;
                    case "SET":
                        RedisCommands.handleSet(command,out,map,expiryMap);
                        break;
                    case "GET":
                        RedisCommands.handleGet(command,out,map,expiryMap);
                        break;
                    case "RPUSH":
                        RedisCommands.handleRPUSH(command,out,listMap);
                        break;
                    case "LPUSH":
                        RedisCommands.handleLPUSH(command,out,listMap);
                        break;
                    case "LLEN":
                        RedisCommands.handleLLen(command,out,listMap);
                        break;
                    case "LRANGE":
                        RedisCommands.handleLange(command,out,listMap);
                        break;
                    case "LPOP":
                        RedisCommands.handleLPOP(command,out,listMap);
                        break;
                    case "BLPOP":
                        RedisCommands.handleBLPOP(command,out,listMap);
                        break;
                    default:
                        out.write("-ERR unknowm command\r\n".getBytes());
                        break;
                }
            }


        } catch (IOException e) {
            System.err.println("Client thread error: " + e.getMessage());
        }

    }

    private List<String> readCommand(InputStream in) throws IOException {
        List<String> command = new ArrayList<>();
        int count = readInt(in);

        for(int i =0;i<count;i++)
        {
            in.read();
            int len = readInt(in);
            byte[] buf = new byte[len];
            in.read(buf);
            command.add(new String(buf));
            in.read();in.read();
        }
        return command;
    }

    private int readInt(InputStream in) throws IOException {

        int result =0;
        char ch;
        while((ch = (char) in.read()) != '\r')
        {
            result = result *10+ (ch-'0');
        }
        in.read();
        return  result;
    }


}
