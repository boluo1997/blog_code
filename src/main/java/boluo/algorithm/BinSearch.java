package boluo.algorithm;

public class BinSearch {

    public static void main(String[] args) {

    }

    // 递归写法
    private static int binSearch(int nums[], int low, int high, int key) {

        while (low <= high) {

            int mid = (low + high) / 2;
            if (key == nums[mid]) {
                return mid;
            } else if (key < nums[mid]) {
                return binSearch(nums, low, mid - 1, key);
            } else {    // if(key > nums[mid])
                return binSearch(nums, mid + 1, high, key);
            }
        }
        return -1;
    }

    // 非递归写法
    private static int binSearch1(int nums[], int low, int high, int key) {

        int nums1[] = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9};
        int key1 = 8;
        int min = 0;
        int max = nums.length - 1;
        int mid = (min + max) / 2;

        while (key != mid) {
            if (key < mid) {
                max = mid - 1;
            } else {
                min = mid + 1;
            }

            if (min >= max) {
                mid = -1;
                break;
            }

            mid = (min + max) / 2;
        }

        return mid;
    }
}


