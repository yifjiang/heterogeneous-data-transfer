import com.datayes.heterDataTransfer.scanner.LogScanner;

public class TestLogScanner {
    public static void main(String [] args){
        try {
            LogScanner myScanner = new LogScanner();

            myScanner.run();
        } catch(Exception e) {
            e.printStackTrace();
            System.out.println("Bad");
        }

    }
}