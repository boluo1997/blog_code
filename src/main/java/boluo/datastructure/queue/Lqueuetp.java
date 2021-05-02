package boluo.datastructure.queue;

/**
 * 队列的链式存储
 */
public class Lqueuetp implements QueueIntf {

    public Nodetype front;
    public Nodetype rear;

    public Lqueuetp() {
        front = new Nodetype();
        front.next = null;
        rear = null;
    }

    @Override
    public void enQueue(int x) {
        Nodetype s = new Nodetype();
        s.data = x;
        s.next = null;
        rear.next = s;
        rear = s;
    }

    // 出队列操作
    @Override
    public Object delQueue() {

        int x;
        Nodetype p;
        if (front == rear) {
            return 0;
        } else {
            p = front.next;
            front.next = p.next;
            if (p.next == null) rear = front;
            x = p.data;
            p = null;
            return x;
        }
    }

    @Override
    public int size() {
        int i = 0;
        Nodetype p = front.next;
        while (p != null) {
            i++;
            p = p.next;
        }
        return i;
    }

    @Override
    public void clear() {

    }

    @Override
    public boolean isEmpty() {
        if (front == rear) return true;
        return false;
    }

    @Override
    public Object head() {
        if (front == rear) return 0;
        return front.next.data;
    }

    class Nodetype {
        int data;
        Nodetype next;
    }
}
