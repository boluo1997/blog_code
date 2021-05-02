package boluo.datastructure.singlelinkedlist;

import boluo.datastructure.seqlist.ListIntf;

import java.util.Scanner;

public class LinkedList implements ListIntf {

    LNode head = null;

    public void setHead(LNode node) {
        this.head = node;
    }

    /**
     * 头插法建立单链表
     * 1): 首先建立一个头节点 head
     * 2): 读入字符串 str
     * 3): 建立一个新节点 p
     * 4): 将字符串的值赋给新节点 p的数据域
     * 5): 分别改变新节点 p的指针域和头节点 h的指针域, 使 p成为 h的直接后继
     * 6): 重复 2~5 直到不满足循环条件为止
     */
    public LinkedList() {
        char ch;
        LNode p;
        head = new LNode();     // 建立头节点
        head.next = null;       // 使头节点的指针域为空
        Scanner scanner = new Scanner(System.in);
        String str = scanner.nextLine();
        int i = 0;
        while (i < str.length()) {
            ch = str.charAt(i);
            p = new LNode();    // 建立一个新节点
            p.data = ch;
            p.next = head.next;
            head.next = p;
            i++;
        }
    }

    @Override
    public int size() {
        LNode p;
        int i = 0;
        p = head.next;
        while (p.next != null) {
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
        return false;
    }

    @Override
    public Object get(int i) {
        int j;
        LNode p;
        p = head.next;
        j = 1;
        while (p != null && j < i) {
            p = p.next;
            j++;
        }
        if (i == j) {
            return p;
        } else {
            return null;
        }
    }

    @Override
    public int indexOf(Object obj) {
        return 0;
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

    }

    // 在单链表中某节点之后插入值新节点算法
    public void insertElementAfter(LNode p, char x) {
        LNode s = new LNode();
        s.data = x;
        s.next = p.next;
        p.next = s;
    }

    // 在单链表中第i个元素之前插入一个元素的算法
    public int insertElementAt(int i, char x) {
        LNode p, s;
        int j = 0;
        p = head;
        while (p != null && j < i - 1) {    // 寻找第i-1号节点
            p = p.next;
            j++;
        }
        if (p != null) {
            s = new LNode();
            s.data = x;
            s.next = p.next;
            p.next = s;
            return 1;
        } else {
            return 0;
        }
    }

    public void remove(LNode p) {      // 删除单链表中某个节点的后继节点
        LNode q;
        if (p.next != null) {
            q = p.next;
            p.next = q.next;
            q = null;
        }
    }

    public LNode search(char x) {       // 单链表的按值查找
        LNode p;
        p = head.next;
        while (p != null && p.data != x) {
            p = p.next;
        }
        return p;
    }

    @Override
    public Object remove(int i) {
        return null;
    }

    @Override
    public Object remove(Object obj) {
        return null;
    }

    class LNode {

        public char data;
        public LNode next;

    }
}
