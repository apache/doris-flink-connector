package org.apache.doris.flink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class JDCBTest {
    static int count=1;
    public static void main(String[] args) throws Exception {

        String url = "jdbc:mysql://52.83.29.166:3306/test";
        Connection conn = DriverManager.getConnection(url,"root","suijimima123456");
        Statement st = conn.createStatement();

        while(true){
            st.execute(String.format("insert into test(name,age,date1,datetime1,timestamp1) value('%s',1,'2021-01-01',now(),now())","zhangsan"+count));
            //st.execute(String.format("update test set age=%s where name='%s'",count,"zhangsan"+count));

            Thread.sleep(5000);
            count++;
            System.out.println("=====" + count);
        }
//        st.close();
//        conn.close();
    }
}
