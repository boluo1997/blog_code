package algorithm;

public class binSearch {

    public static void main(String[] args) {


    }

    private static int binSearch(int nums[], int low, int high, int key){

        while(low <= high){

            int mid = (low + high)/2;
            if(key == nums[mid]){
                return mid;
            }else if(key < nums[mid]){
                return binSearch(nums,low,mid-1,key);
            }else{	// if(key > nums[mid])
                return binSearch(nums,mid+1,high,key);
            }
        }
        return -1;
    }
}


