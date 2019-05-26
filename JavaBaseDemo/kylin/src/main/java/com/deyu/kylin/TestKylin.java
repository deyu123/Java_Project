package com.deyu.kylin;

import java.sql.*;

public class TestKylin {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        String driverName = "org.apache.kylin.jdbc.Driver";
        String url = "jdbc:kylin://hadoop202:7070/Model_Deyu";
        String userName = "ADMIN";
        String passwd = "KYLIN";
        Class.forName(driverName);
        Connection connection = DriverManager.getConnection(url, userName, passwd);
        String sql = "select dept.dname,sum(emp.sal) from emp join dept on emp.deptno = dept.deptno group by dept.dname";
        PreparedStatement ps = connection.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();
        while(resultSet.next()){
            System.out.println(resultSet.getString(1) + "-" + resultSet.getDouble(2));
        }

    }
}
