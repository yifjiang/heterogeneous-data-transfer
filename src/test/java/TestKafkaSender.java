import com.datayes.heterDataTransfer.insertDeleteThread.KafkaSender;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by lyhdk7 on 2018/6/24.
 */
public class TestKafkaSender {
    public static void main(String [] args){
        KafkaSender sd = new KafkaSender();
        Map<String, String> testMap = new HashMap<>();
        testMap.put("Operation", "Insert");
        testMap.put("Find","Junlin");

        sd.send(testMap.toString());
    }
}
