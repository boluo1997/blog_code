package boluo.datastructure.stack;

public interface StackIntf {

    void push(Object obj);       // 入栈

    Object pop();                // 出栈

    int size();     // 返回栈中的元素个数

    void clear();   // 清空栈

    boolean isEmpty(); // 判断栈是否为空

    Object top();       // 读取栈顶元素

}
