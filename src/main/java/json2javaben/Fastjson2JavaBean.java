package json2javaben;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;

public class Fastjson2JavaBean {
    public static void main(String[] args) {
        Course math = new Course(1,"数学");
        Course english = new Course(2,"英语");
        List<Course> course1List = new ArrayList<Course>();
        course1List.add(math);
        course1List.add(english);
        
        Student student = new Student("zz", course1List);
        String json = JSONObject.toJSONString(student, SerializerFeature.WriteMapNullValue);
        System.out.println(json);
        
        
        Student s = JSON.parseObject(json, Student.class);
        System.out.println(s.getName());
        List<Course> list = s.getCourse();
        for(int i = 0; i < list.size(); i++){
            Course c = list.get(i);
            System.out.println(c.getId() + "--" + c.getName());
        }
        
    }
}

class Student{
    private String name;
    private List<Course> course1;
    public Student(){
        //必须实现无参的构造方法
    }
    public Student(String name, List<Course> course1){
        this.name = name;
        this.course1 = course1;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public List<Course> getCourse() {
        return course1;
    }
    public void setCourse(List<Course> course1) {
        this.course1 = course1;
    }
}
class Course{
    private int id;
    private String name;
    public Course(){
        //必须实现无参的构造方法        
    }
    public Course(int id, String name){
        this.id = id;
        this.name = name;
    }
    public int getId() {
        return id;
    }
    public void setId(int id) {
        this.id = id;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
}