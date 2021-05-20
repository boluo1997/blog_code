package boluo.datastructure.tree;

import java.util.LinkedList;
import java.util.Queue;

public class LinkedBinaryTreeTest {

    public static void main(String[] args) {

        Node node5 = new Node(5, null, null);
        Node node4 = new Node(4, null, node5);

        Node node3 = new Node(3, null, null);
        Node node7 = new Node(7, null, null);
        Node node6 = new Node(6, null, node7);

        Node node2 = new Node(2, node3, node6);
        Node node1 = new Node(1, node4, node2);

        LinkedBinaryTree btree = new LinkedBinaryTree(node1);

        System.out.println("二叉树节点个数" + btree.size(node1));
        System.out.println("二叉树高度" + btree.height(node1));
        System.out.println("寻找值" + btree.findKey(3, node1));
        //btree.preOrderTraverse();
        System.out.println("先序遍历");
        btree.preOrderTraverse(node1);
        System.out.println("中序遍历");
        //btree.inOrderTraverse(node1);
        System.out.println("后序遍历");
        //btree.postOrderTraverse(node1);
        System.out.println("层遍历");
        btree.levelOrderByStack();

    }

    static class LinkedBinaryTree {
        Node root;

        public LinkedBinaryTree(Node root) {
            this.root = root;
        }

        //尺寸、二叉树节点个数
        public int size(Node root) {
            if (root == null) {
                return 0;
            } else {
                int nl = size(root.leftChild);
                int nr = size(root.rightChild);
                return nl + nr + 1;
            }
        }

        //二叉树高度
        public int height(Node root) {
            if (root == null) {
                return 0;
            } else {
                int nl = height(root.leftChild);
                int nr = height(root.rightChild);
                return nl > nr ? nl + 1 : nr + 1;
            }
        }

        //在二叉树中寻找某个值
        public Node findKey(Object value, Node root) {
            if (root == null) {
                return null;
            } else if (root.value == root) {
                return root;
            } else {
                Node nl = findKey(value, root.leftChild);
                Node nr = findKey(value, root.rightChild);

                if (nl != null && nl.value == value) {
                    return nl;
                }

                if (nr != null && nr.value == value) {
                    return nr;
                }
                return null;
            }
        }

        //二叉树先序遍历
        public void preOrderTraverse(Node root) {
            if (root != null) {
                System.out.println(root.value + " ");
                preOrderTraverse(root.leftChild);
                preOrderTraverse(root.rightChild);
            }
        }

        //按照层次遍历--借助栈
        public void levelOrderByStack() {
            if (root == null) {
                return;
            }

            Queue<Node> queue = new LinkedList<>();
            queue.add(root);

            while (queue.size() != 0) {   //只要栈里面元素的个数不为0
                //遍历栈中的元素
                for (int i = 0; i < queue.size(); i++) {
                    Node temp = queue.poll();   //把栈顶的元素取出
                    System.out.println(temp.value + " ");

                    //然后再遍历栈顶元素的左子树、右子树
                    if (temp.leftChild != null) {
                        queue.add(temp.leftChild);
                    }

                    if (temp.rightChild != null) {
                        queue.add(temp.rightChild);
                    }
                }
            }
        }
    }

    static class Node {

        Object value;
        Node leftChild;
        Node rightChild;

        public Node(Object value) {
            this.value = value;
        }

        public Node(Object value, Node leftChild, Node rightChild) {
            this.value = value;
            this.leftChild = leftChild;
            this.rightChild = rightChild;
        }

        @Override
        public String toString() {
            return "Node{" +
                    "value=" + value +
                    ", leftChild=" + leftChild +
                    ", rightChild=" + rightChild +
                    '}';
        }
    }
}
