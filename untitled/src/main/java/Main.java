    import java.io.IOException;

    import java.net.ServerSocket;
    import java.net.Socket;


    public class Main
    {
        public static void main(String[] args) {

            int port = 6379;//this port is listened by redis


            System.out.println("Your logs will appear here");

            try(ServerSocket socket = new ServerSocket(port)){

                socket.setReuseAddress(true);

                while(true) {

                    Socket clientSocket = socket.accept();

                    System.out.println("Connected to :"+clientSocket.getInetAddress());
                    Thread clientThread =  new Thread(  new ClientHandler(clientSocket ));
                    clientThread.start();


                }
            } catch (IOException e) {
                System.out.println("Server exception: "+e.getMessage());
            }
        }


    }
