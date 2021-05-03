package boluo.datastructure.queue;

public class Squeuetp implements QueueIntf {

    public final int maxSize = 100;
    public int elem[] = new int[maxSize];

    // 队头指针, 队尾指针
    public int front, rear;

    @Override
    public void enQueue(int x) {

    }

    @Override
    public Object delQueue() {
        return null;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public void clear() {

    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public Object head() {
        return null;
    }
}
