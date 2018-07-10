import com.datayes.heterDataTransfer.scanner.MainScanner;

public class TestMainScanner {
    public static void main(String [] args){
        try {
            MainScanner myScanner = new MainScanner();

            myScanner.start();
        } catch(Exception e) {
            System.out.println(e.getMessage());
        }

    }
}
