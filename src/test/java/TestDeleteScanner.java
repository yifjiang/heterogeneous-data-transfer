import com.datayes.heterDataTransfer.insertDeleteThread.DeleteScanner;

public class TestDeleteScanner {
    public static void main(String [] args){
        try {
            DeleteScanner myScanner = new DeleteScanner("test1");

            myScanner.start();
        } catch(Exception e) {
            e.printStackTrace();
            System.out.println("Bad");
        }

    }
}
