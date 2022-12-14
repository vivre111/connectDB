import com.mysql.cj.jdbc.MysqlDataSource;
import org.json.JSONObject;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Scanner;

class Toy2 {
    Toy2(String _filename) {
        this.filename = _filename;
    }

    private String filename;
    private MysqlDataSource mysqlDataSource;
    private Connection connection;
    private Statement statement;

    public void close() {
        try {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public void connectToDB() throws Exception {
        mysqlDataSource = new MysqlDataSource();
        String fileText = new String(Files.readAllBytes(Paths.get("/home/phantomoflamancha/IdeaProjects/connectDb/src/connectionConfig.json")));
        JSONObject obj = new JSONObject(fileText);
        String user = obj.getString("user");
        String password = obj.getString("password");
        String url = obj.getString("url");
        String database = obj.getString("database");
        mysqlDataSource.setURL(url);
        mysqlDataSource.setUser(user);
        mysqlDataSource.setPassword(password);
        mysqlDataSource.setDatabaseName(database);
        connection = mysqlDataSource.getConnection();
        connection.setAutoCommit(false);
        statement = connection.createStatement();
    }

    // returns time taken
    public long uploadCSV(int batchsize) throws Exception {
        // TODO: add check whether table exist
        try{
            statement.executeUpdate("Drop TABLE " + filename + "_Java");
        }catch (Exception e){
            System.out.println(filename+"_Java probably does not exisit, we can continue");
        }
        String createTable = "CREATE TABLE " + filename + "_Java " + "(date date, open DECIMAL(10 , 2 ), high DECIMAL(10 , 2 ),low DECIMAL(10 , 2 ),close DECIMAL(10 , 2 ),trade DECIMAL(10 , 2 ), amount decimal(20,2))";
        statement.executeUpdate(createTable);

        long startTime = System.currentTimeMillis();
        Scanner sc = new Scanner(new File("/home/phantomoflamancha/Desktop/stockProject/US/daily-normal/" + filename + ".csv"));
        sc.useDelimiter("\r\n");
        boolean firstline = true;
        int count = 0;
        while (sc.hasNext()) {
            String st = sc.next();
            //skip first line
            if (firstline) {
                firstline = false;
                continue;
            }
            String[] row = st.split(",");

            //System.out.println(row[0]);System.out.println(row[1]);System.out.println(row[2]);System.out.println(row[3]);System.out.println(row[4]);System.out.println(row[5]);
            String insert = "insert into " + filename + "_Java" + "(date,open,high,low,close,trade,amount) values ('"
                    + row[0] + "'," + row[1] + "," + row[2] + "," + row[3] + "," + row[4] + "," + row[5] + "," + row[6] + ")";
            statement.addBatch(insert);
            count++;
            if (count % batchsize == 0) {
                statement.executeBatch();
                count = 0;
            }
        }
        // execute the last batch
        if (count != 0) {
            statement.executeBatch();
        }
        connection.commit();
        sc.close();
        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }

    public void sanity_test() throws SQLException {
        // hardcoded sanity check
        ResultSet resultSet = statement.executeQuery(
                "select count(*) as c from " + filename + "_Java");
        resultSet.next();
        int checkRowNum = resultSet.getInt("c");
        if (checkRowNum != 5299) {
            System.out.println("error, got " + checkRowNum + " rows");
        }
    }

    public static void main(String arg[]) {
        Toy2 toy = new Toy2("A");
        try {
            toy.connectToDB();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        int[] batchsizes = {1, 100, 200, 400, 800, 1200, 1600, 2000, 3000, 4000, 5000};
        try {
            for (int batchsize : batchsizes) {
                long[] rec = new long[5];
                // try each batch size 5 times and get average time eclasped
                for (int i = 0; i < 5; i++) {
                    // create table
                    toy.sanity_test();
                    rec[i] = toy.uploadCSV(batchsize);
                }

                System.out.println("batch size: " + batchsize + ", time eclasped: " + (rec[0] + rec[1] + rec[2] + rec[3] + rec[4]) / 5);

            }
        } catch (Exception exception) {
            System.out.println(exception);
        } finally {
            toy.close();
        }
    } // function ends
} // class ends