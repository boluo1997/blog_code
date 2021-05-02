package boluo.datastructure.stack;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * 地图四染色问题
 */
public class StackTest2 {

    class Cell {

        private int row;
        private int col;
        private Cell from;

        public Cell(int row, int col, Cell from) {
            this.row = row;
            this.col = col;
            this.from = from;
        }
    }

    char[][] maze =

                    {{'#', '#', '#', '#', 'B', '#', '#', '#', '#', '#', '#', '#'},

                    {'#', '#', '#', '#', '.', '.', '.', '.', '#', '#', '#', '#'},

                    {'#', '#', '#', '#', '.', '#', '#', '#', '#', '.', '.', '#'},

                    {'#', '.', '.', '.', '.', '#', '#', '#', '#', '#', '.', '#'},

                    {'#', '.', '#', '#', '#', '#', '#', '.', '#', '#', '.', '#'},

                    {'#', '.', '#', '#', '#', '#', '#', '.', '#', '#', '.', '#'},

                    {'#', '.', '#', '#', '.', '.', '.', '.', '.', '.', '.', '#'},

                    {'#', '.', '#', '#', '.', '#', '#', '#', '.', '#', '.', '#'},

                    {'#', '.', '.', '.', '.', '#', '#', '#', '.', '#', '.', '#'},

                    {'#', '#', '.', '#', '.', '#', '#', '#', '.', '#', '.', 'A'},

                    {'#', '#', '.', '#', '#', '#', '.', '.', '.', '#', '#', '#'},

                    {'#', '#', '#', '#', '#', '#', '#', '#', '#', '#', '#', '#'}};

    public void show() {

        for (int i = 0; i < maze.length; i++) {

            for (int j = 0; j < maze[i].length; j++) {
                System.out.print(" " + maze[i][j]);
            }
            System.out.println();
        }
    }

    //把与from集合中相邻的可染色节点染色，被染色节点记入 dest
    //一旦发现出口将被染色，则返回当前的“传播源”节点
    public Cell colorCell(Set from, Set dest) {
        Iterator it = from.iterator();

        while (it.hasNext()) {
            Cell a = (Cell) it.next();

            Cell[] c = new Cell[4];
            c[0] = new Cell(a.row - 1, a.col, a);
            c[1] = new Cell(a.row, a.col - 1, a);
            c[2] = new Cell(a.row + 1, a.col, a);
            c[3] = new Cell(a.row, a.col + 1, a);
            //填空

            for (int i = 0; i < 4; i++) {
            //越出地图就跳过

                if (c[i].row < 0 || c[i].row >= maze.length) continue;
                if (c[i].col < 0 || c[i].col >= maze[0].length) continue;
                char x = maze[c[i].row][c[i].col];

                if (x == 'B') return a;
                if (x == '.') {
                    maze[c[i].row][c[i].col] = '?';
                    dest.add(c[i]);
                }
            }
        }
        return null;
    }

    public void resolve() {
        Set set = new HashSet();
        set.add(new Cell(9, 11, null));

        for (; ; ) {
            Set set1 = new HashSet();
            Cell a = colorCell(set, set1);
            if (a != null) {
                System.out.println("找到解！");

                while (a != null) {
                    maze[a.row][a.col] = '+';
                    a = a.from;
                }
                break;

            }

            if (set1.isEmpty()) {
                System.out.println("无解！");
                break;
            }
            set = set1;
        }
    }

    public static void main(String[] args) {
        StackTest2 m = new StackTest2();
        m.show();
        m.resolve();
        m.show();
    }
}
