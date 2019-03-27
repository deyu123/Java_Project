package com.deyu.example.new_project;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class NewProjectApplicationTests {

    @Test
    public void contextLoads() {
        System.out.println("aa");
    }

    @Test
    public void jvmTest(){

//        启动类加载器
        System.out.println("启动类加载器");

    }

}
