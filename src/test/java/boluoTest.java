import boluo.basics.Note02_Enum;
import boluo.basics.Note03_ThreadPoolManager;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class boluoTest {

    private static final Logger logger = LoggerFactory.getLogger(boluoTest.class);

    @Test
    public void func1(){
        int areaType = 3;
        if (Note02_Enum.PREFECTURE.eq(areaType)) {

            logger.info(Note02_Enum.PREFECTURE.getName());
            Note03_ThreadPoolManager.run(() -> {
                // do something
            });

        }
    }
}
