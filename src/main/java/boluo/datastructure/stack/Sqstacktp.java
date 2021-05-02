package dataStructure.stack;

public class Sqstacktp implements StackIntf {

    final int maxSize = 100;
    int elem[] = new int[maxSize];
    int top;

    public Sqstacktp() {
        // 初始化栈, 将顺序栈s置为空
        top = 0;
    }

    @Override
    public void push(Object obj) {

    }

    // 入栈操作
    public void push(int i) {
        if (top == maxSize) throw new UnsupportedOperationException("栈溢出!!!");
        elem[top++] = i;
    }

    @Override
    public Object pop() {
        if (top == 0) return 0;
        return elem[--top];
    }

    @Override
    public int size() {
        return top;
    }

    @Override
    public void clear() {

    }

    @Override
    public boolean isEmpty() {
        if (top < 0) throw new UnsupportedOperationException("bug");
        if (top > 0) return false;
        return true;
    }

    @Override
    public Object top() {

        if (top == 0) return 0;
        return elem[top-1];
    }

}
