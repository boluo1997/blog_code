package boluo.datastructure.seqlist;

public interface ListIntf {

    int size();

    void clear();

    boolean isEmpty();

    Object get(int i);

    int indexOf(Object obj);    // 第一个与obj满足关系equals()的数据元素的位序, 若这样的数据不存在, 返回-1

    Object getPre(Object obj);  // 若obj是表中的元素, 返回他的前驱

    Object getNext(Object obj); // 若obj是表中的元素, 返回他的后继

    void insertElementAt(Object obj, int i);    // 在第i个位置之前插入新的数据元素obj, 表长度+1

    Object remove(int i);   // 删除第i个数据元素, 并返回其值, 表长度-1

    Object remove(Object obj);  // 删除数据obj, 并返回其值, 表长度-1

}
