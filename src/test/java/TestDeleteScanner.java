import com.datayes.heterDataTransfer.scanner.DeleteScanner;

public class TestDeleteScanner {
    public static void main(String [] args){
        try {
            DeleteScanner myScanner = new DeleteScanner();

            myScanner.start();
        } catch(Exception e) {
            e.printStackTrace();
            System.out.println("Bad");
        }

    }
}
