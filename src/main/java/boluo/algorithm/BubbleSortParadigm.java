package boluo.algorithm;

import java.util.Comparator;

public class BubbleSortParadigm<T> {

    public Comparator<T> comp;

    public BubbleSortParadigm(Comparator<T> comp) {
        super();
        this.comp = comp;
    }

    @SuppressWarnings("unchecked")
    public int compare(T t1, T t2) {
        if (comp == null) {
            Comparable<T> t = (Comparable<T>) t1;
            return t.compareTo(t2);
        } else {
            return comp.compare(t1, t2);
        }
    }

    public void swap(T[] array, int i, int j) {
        T t = array[i];
        array[i] = array[j];
        array[j] = t;
    }

    public void sort(T[] array) {
        int n = array.length;
        for (int i = 0; i < n - 1; i++) {
            boolean flag = true;
            for (int j = 0; j < n - 1 - i; j++) {
                if (compare(array[j], array[j + 1]) > 0) {
                    flag = false;
                    swap(array, j, j + 1);
                }
            }
            if (flag) {
                break;
            }
        }
    }
}
