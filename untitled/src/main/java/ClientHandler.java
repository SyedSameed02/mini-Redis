import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import java.util.List;

import java.util.Scanner;


public  class ClientHandler implements Runnable
{
        Socket socket;



        public ClientHandler(Socket socket)
        {
            this.socket = socket;

        }

        public void run()
        {





            try(
                    OutputStream out =  socket.getOutputStream();
                    InputStream in = socket.getInputStream();
                )
            {



                List<String> commands = RESPUtils.parseArray(in);
                String cmd = commands.get(0);

                switch (cmd)
                {
                    case "PING":
                        out.write("PONG\r\n".getBytes());
                        break;

                    case "SET":
                        RedisCommands.handleSET(out,commands);
                        break;

                    case "GET":
                        RedisCommands.handleGET(out,commands);
                        break;
                    case "RPUSH":
                    case "LPUSH":
                        RedisCommands.handlePush(out,commands);
                        break;
                    case "LRANGE":
                        RedisCommands.handleLRANGE(out,commands);
                        break;
                    case "LPOP":
                        RedisCommands.handleLPOP(out,commands);
                        break;
                    case "BLPOP":
                        RedisCommands.handleBLPOP(out,commands);
                        break;





                    default:
                        out.write("-ERR in command\r\n".getBytes());
                }




            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
}