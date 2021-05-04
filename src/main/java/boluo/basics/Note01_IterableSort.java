package boluo.basics;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * 集合排序
 */
public class Note01_IterableSort {


    /**
     * 第一种: 使用比较器 Comparator, 重写 compare方法
     * 通过集合调用 sort方法
     */
    @Test
    public void func1() {
        Student s1 = new Student("dingc", 20, 80);
        Student s2 = new Student("boluo", 40, 70);
        Student s3 = new Student("qidai", 30, 90);

        List<Student> studentList = Lists.newArrayList();
        studentList.add(s1);
        studentList.add(s2);
        studentList.add(s3);

        System.out.println("排序之前: " + studentList);

        studentList.sort(new Comparator<Student>() {
            @Override
            public int compare(Student o1, Student o2) {
                return o1.score - o2.score;
            }
        });

        System.out.println("排序之后: " + studentList);
    }

    class Student {

        String name;
        int age;
        int score;

        public Student(String name, int age, int score) {
            this.name = name;
            this.age = age;
            this.score = score;
        }

        @Override
        public String toString() {
            return "Student{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    ", score=" + score +
                    '}';
        }
    }


    /**
     * 第二种: 实现 Comparable接口, 重写 compareTo方法, 指定排序规则
     */
    @Test
    public void func2() {

        Teacher t1 = new Teacher("dingc", 30, 80);
        Teacher t2 = new Teacher("boluo", 20, 70);
        Teacher t3 = new Teacher("qidai", 40, 90);

        List<Teacher> teacherList = Lists.newArrayList();
        teacherList.add(t1);
        teacherList.add(t2);
        teacherList.add(t3);

        Collections.sort(teacherList);
        System.out.println(teacherList);
    }

    class Teacher implements Comparable<Teacher> {

        String name;
        int age;
        int score;

        public Teacher(String name, int age, int score) {
            this.name = name;
            this.age = age;
            this.score = score;
        }

        @Override
        public String toString() {
            return "Teacher{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    ", score=" + score +
                    '}';
        }

        @Override
        public int compareTo(Teacher teacher) {
            return this.score - teacher.score;
        }
    }
}
