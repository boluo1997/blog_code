package dataStructure.singleLinkedList;

import dataStructure.seqList.ListIntf;

import java.io.IOException;
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
        return null;
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
    public int insertElementAt() {
        return 0;
    }

    @Override
    public Object remove(int i) {
        return null;
    }

    @Override
    public Object remove(Object obj) {
        return null;
    }
}
