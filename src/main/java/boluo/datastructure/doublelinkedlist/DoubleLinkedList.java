package boluo.datastructure.doublelinkedlist;

public class DoubleLinkedList {

    int size = 0;   // 节点数
    Node firstNode;     // 头结点
    Node lastNode;      // 尾节点

    // 下标越界问题
    public void isOut(int index) {
        if (index < 0 || index >= size) {
            throw new UnsupportedOperationException("下标已经超出链表的长度!!!");
        }
    }

    // 根据下标找到指定的节点
    public Node getNode(int index) {
        Node no = this.firstNode;
        for (int i = 0; i < index; i++) {
            no = no.next;
        }
        return no;
    }

    // 添加
    public void addNode(String str) {
        // 新建节点
        Node node = new Node(str, null, null);

        // 判断是头部添加还是尾部添加
        if (size == 0) {
            this.firstNode = node;          // 新节点指向头节点
        } else {
            this.lastNode.next = node;      // 新节点指向原先尾节点的下一个节点
            node.prev = this.lastNode;      // 尾节点指向新节点的下一个节点
        }
        this.lastNode = node;
        size++;
    }

    public void addNode(int index, String str) {
        // 判断下标越界
        isOut(index);

        // 新建节点
        Node node = new Node(str, null, null);

        // 判断在哪个位置进行插入
        if (index == 0) {   // 头节点
            this.firstNode.prev = node;     // 新节点指向原头节点的上一个节点
            node.next = this.firstNode;     // 原节点指向新节点的下一个节点
            this.firstNode = node;          // 新节点变为头节点
        } else if (index == size) {         // 尾节点
            // 调用添加方法
            addNode(str);
            size--;
        } else {        // 中间插入
            Node no = getNode(index);       // 查找到index下标对应的节点
            no.prev.next = node;            // 新节点指向node节点的上一个节点的下一个节点
            node.prev = node;               // 新节点指向no上一个节点
            node.next = no;                 // no指向新节点的下一个节点
        }
        size++;
    }

    // 根据下标进行删除
    public void remove(int index) {
        // 判断下标越界
        isOut(index);

        // 判断删除的位置
        if (index == 0) {   // 删除头节点
            this.firstNode.next.prev = null;        // 原头节点的下一个节点的上一个节点为null
            this.firstNode = this.firstNode.next;   // 原头节点的下一个节点指向头节点
        } else if (index == size - 1) {      // 删除尾节点
            this.lastNode.prev.next = null;         // 原尾节点的上一个节点的下一个节点为null
            this.lastNode = this.lastNode.prev;     // 原尾节点的上一个节点指向尾节点
        } else {        // 删除中间元素
            // 找到index下标对应的节点
            Node no = getNode(index);

            no.next.prev = no.prev;     // no节点的上一个节点指向no节点下一个节点的上一个节点
            no.prev.next = no.next;     // no节点的下一个节点指向no上一个节点的下一个
        }
        size--;
    }

    // 删除指定元素
    public void remove(String str) {
        int index = indexOf(str);
        if (index != -1) {
            remove(index);
        }
    }

    // 元素第一次出现的下标
    public int indexOf(String str) {
        // 遍历节点
        Node no = this.firstNode;
        for (int i = 0; i < size; i++) {
            // 判断item值和str是否相等
            if (str == no.item || str != null && str.equals(no.item)) {
                return i;
            }
            no = no.next;
        }

        return -1;
    }

    // 重写toString方法
    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder("[");

        Node no = this.firstNode;
        for (int i = 0; i < size; i++) {
            sb.append(no.item).append(", ");
            no = no.next;
        }

        String str = sb.toString();
        if (size > 0) {
            str = str.substring(0, str.length() - 2);
        }

        return str += "]";
    }
}












