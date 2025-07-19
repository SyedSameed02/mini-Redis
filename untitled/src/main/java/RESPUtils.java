import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class RESPUtils {


    public static void sendBulkString(OutputStream out,String value) throws IOException {
        if(value == null)
        {
           out.write("$-1\r\n".getBytes());
        }else{
            out.write(("$"+value.length()+"\r\n"+value+"\r\n").getBytes());
        }
    }

    public static void sendArray(OutputStream out, List<String> values) throws IOException {
        out.write(("*"+values.size()+"\r\n").getBytes());

        for(String val : values)
        {
            sendBulkString(out,val);
        }
    }
    public static void sendNullArray(OutputStream out) throws IOException {
        out.write("*-1\r\n".getBytes());
    }

}
