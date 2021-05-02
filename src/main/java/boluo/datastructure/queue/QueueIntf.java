package boluo.datastructure.queue;

public interface QueueIntf {

    void enQueue(Object obj);   //入队列

    Object delQueue();      // 出队列

    int size();

    void clear();

    boolean isEmpty();

    Object head();
}
