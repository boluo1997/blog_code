import boluo.basics.Note02_Enum;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;

public class boluoTest {

    @Test
    public void func1(){
        int areaType = 3;
        if (Note02_Enum.PREFECTURE.eq(areaType)) {
            System.out.println(Note02_Enum.PREFECTURE.getName());
        }
    }
}
