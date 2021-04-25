package dataStructure;

public class SqList implements ListIntf {

    final int maxlen = 1000;            // maxlen表示线性表可能的最大数据元素数目
    String v[] = new String[maxlen];    // 存放线性表元素的数组
    int len = 0;                        // 表示线性表的长度

    int getMaxlen() {
        return maxlen;
    }

    public SqList() {

    }

    @Override
    public int size() {
        return len;
    }

    @Override
    public void clear() {

    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public Object get(int i) {
        return null;
    }

    @Override
    public int indexOf(Object obj) {
        int i;
        for (i = 0; i < len; i++) {
            if (obj.equals(v[i])) {
                break;
            }
        }
        if (i < len) {
            return i + 1;
        } else {
            return 0;
        }
    }

    @Override
    public Object getPre(Object obj) {
        return null;
    }

    @Override
    public Object getNext(Object obj) {
        return null;
    }

    @Override
    public void insertElementAt(Object obj, int i) {
        if (len == maxlen) {    // 判断线性表的存储空间是否已满
            System.out.println("溢出!!!");
            return;
        } else {
            if ((i < 1 || i > len + 1)) {   // 检查i值是否超出所允许的范围
                System.out.println("插入位置不正确!!!");
                return;
            } else {
                for (int j = len - 1; j > i - 1; j--) {
                    v[j + 1] = v[j];    // 将第i个元素和他后面的所有元素均后移一位
                }
                v[i - 1] = (String) obj;
                len++;
                return;
            }
        }
    }

    @Override
    public Object remove(int i) {

        if ((i < 1) || (i > len)) {   // 判断i值是否超过所允许的范围
            System.out.println("删除位置不正确!!!");
            return null;
        } else {
            Object obj = v[i - 1];      // 把要删除位置的元素返回
            for (int j = i; j < len; j++) {
                v[j - 1] = v[j];        // 后面的往前移一位
            }
            len--;
            return obj;
        }
    }

    @Override
    public Object remove(Object obj) {
        return null;
    }
}
