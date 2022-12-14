import com.mysql.cj.jdbc.MysqlDataSource;

import java.io.File;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Scanner;

class Toy2 {
    public static void main(String arg[]) {
        Connection connection = null;
        try {
            // make connection
            MysqlDataSource dataSource = new MysqlDataSource();
            dataSource.setUser("austin");
            dataSource.setURL("jdbc:mysql://localhost:3306/stockUS");
            dataSource.setPassword("password");
            dataSource.setDatabaseName("stockUS");
            Connection c = dataSource.getConnection();
            c.setAutoCommit(false);
            Statement s = c.createStatement();

            int[] batchsizes = {1, 100, 200, 400, 800, 1200, 1600, 2000, 3000, 4000, 5000};
            //int[] batchsizes = {2000};

            for (int batchsize : batchsizes) {
                long[] rec = new long[5];
                // try each batch size 5 times and get average time eclasped
                for (int i = 0; i < 5; i++) {
                    // create table
                    String table_name = "A_Java";
                    s.executeUpdate("Drop TABLE " + table_name);
                    String createTable = "CREATE TABLE " + table_name + "(date date, open DECIMAL(10 , 2 ), high DECIMAL(10 , 2 ),low DECIMAL(10 , 2 ),close DECIMAL(10 , 2 ),trade DECIMAL(10 , 2 ), amount decimal(20,2))";
                    s.executeUpdate(createTable);

                    long startTime = System.currentTimeMillis();
                    // read line one by one and commit to db
                    Scanner sc = new Scanner(new File("/home/phantomoflamancha/Desktop/stockProject/US/daily-normal/A.csv"));
                    sc.useDelimiter("\r\n");
                    boolean firstline = true;
                    int count = 0;
                    //int batchsize = 100;
                    while (sc.hasNext()) {
                        String st = sc.next();
                        //skip first line
                        if (firstline) {
                            firstline = false;
                            continue;
                        }

                        String[] row = st.split(",");

                        //System.out.println(row[0]);System.out.println(row[1]);System.out.println(row[2]);System.out.println(row[3]);System.out.println(row[4]);System.out.println(row[5]);
                        String insert = "insert into " + table_name + "(date,open,high,low,close,trade,amount) values ('"
                                + row[0] + "'," + row[1] + "," + row[2] + "," + row[3] + "," + row[4] + "," + row[5] + "," + row[6] + ")";
                        s.addBatch(insert);
                        count++;
                        if (count % batchsize == 0) {
                            s.executeBatch();
                            count = 0;
                        }
                    }
                    if (count != 0) {
                        s.executeBatch();
                    }
                    c.commit();
                    sc.close();
                    long endTime = System.currentTimeMillis();
                    rec[i] = endTime - startTime;
                    ResultSet resultSet = s.executeQuery(
                            "select count(*) as c from "+table_name);
                    resultSet.next();
                    int checkRowNum = resultSet.getInt("c");
                    if(checkRowNum!=5299){
                        System.out.println("error, got "+i+" rows");
                    }

                }

                System.out.println("batch size: " + batchsize + ", time eclasped: " + (rec[0] + rec[1] + rec[2] + rec[3] + rec[4]) / 5);

            }
            s.close();
            c.close();
        } catch (Exception exception) {
            System.out.println(exception);
            System.out.println("hi");
        }
    } // function ends
} // class ends