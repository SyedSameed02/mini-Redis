import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

public class Main
{
    public static void main(String[] args) {

        int port = 6379;//this port is listened by redis

        try(ServerSocket serverSocket =  new ServerSocket(port))
        {
            serverSocket.setReuseAddress(true);

            while (true)
            {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Connection established with "+clientSocket.getInetAddress());

                new ClientHandler(clientSocket).start();

            }


        } catch (IOException e) {
            System.out.println("Server exception: "+e.getMessage());
        }
    }
}
