import com.datayes.heterDataTransfer.scanner.InsertUpdateScanner;

/**
 * Created by lyhdk7 on 2018/6/18.
 */
public class TestIDMainScanner {
    public static void main(String [] args){
        try {
            InsertUpdateScanner myScanner = new InsertUpdateScanner("test1");

            myScanner.start();
        } catch(Exception e) {
            System.out.println("bad");
        }

    }
}
