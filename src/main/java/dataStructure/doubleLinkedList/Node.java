package dataStructure.doubleLinkedList;

public class Node {

    // 属性--存储数据
    String item;
    Node prev;
    Node next;

    public Node(String item, Node prev, Node next){
        this.item = item;
        this.prev = prev;
        this.next = next;
    }
}
