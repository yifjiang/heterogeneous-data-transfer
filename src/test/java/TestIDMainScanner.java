import com.datayes.heterDataTransfer.insertDeleteThread.MainScanner;

/**
 * Created by lyhdk7 on 2018/6/18.
 */
public class TestIDMainScanner {
    public static void main(String [] args){
        try {
            MainScanner myScanner = new MainScanner("test1");

            myScanner.start();
        } catch(Exception e) {
            System.out.println("Bad");
        }

    }
}
