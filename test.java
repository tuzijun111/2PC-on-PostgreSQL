
import java.io.*;
import java.sql.*;
import java.math.BigInteger;


public class test {
        public static void main(String args[]) throws SQLException, IOException {
            //TranProcessing1();
            String tranlog = "/Users/binbingu/Documents/tranlog.txt";
            FileInputStream inputStream = new FileInputStream(tranlog);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

            String str = null;
            while((str = bufferedReader.readLine()) != null)
            {
                System.out.println(str);
            }

//        Connection c = null;
//        Statement stmt = null;
//		try {
//		    String name1 = "node3";
//            Class.forName("org.postgresql.Driver");
//            String url = "jdbc:postgresql://localhost:5432/"+name1;
//            c = DriverManager.getConnection(
//                    url, "postgres",
//                    "016111");
//            c.setAutoCommit(true);
//
//            System.out.println("连接数据库成功！");
//            stmt = c.createStatement();
//
//            int b = Hashvalue("2017-11-08 00:00:00, 30cced27_6cd1_4d82_9894_bddbb71a4402");
//
//            System.out.println(b);
//
////            String sql = "CREATE TABLE messages " +
////                    "(ID INT PRIMARY KEY     NOT NULL," +
////                    " NAME           TEXT    NOT NULL, " +
////                    " AGE            INT     NOT NULL, " +
////                    " ADDRESS        CHAR(50), " +
////                    " SALARY         REAL)";
////            stmt.executeUpdate(sql);
//
//
////            try {
////                String sql = "INSERT INTO messages (id,name,age,address,salary) VALUES (1, 'a', 3, 'aa', 5 );";
////                stmt.executeUpdate(sql);
////            }catch (Exception e) {
////                e.printStackTrace();
////                System.err.println(e.getClass().getName() + ":++++ " + e.getMessage());
////                System.exit(0);
////            }
////
//
//
////            ResultSet rs = stmt.executeQuery("select * from messages");
////            while(rs.next()){
////                int id = rs.getInt("id");
////                String name = rs.getString("name");
////                int age = rs.getInt("age");
////                String address = rs.getString("address");
////                float salary = rs.getFloat("salary");
////                System.out.println(id + "," + name + "," + age + "," + address.trim() + "," + salary);
////            }
//
////            rs.close();
//            stmt.close();
//
//            c.close();
//
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//            System.exit(0);
//        }
//		System.out.println("查询数据成功！");
    }

    public static int Hashvalue(String s) {
        // 数组大小一般取质数
        int h = s.hashCode();
        return Math.abs(h%3)+1; //make the value in {1,2,3}
    }

    public static int[] TranProcessing1() throws SQLException, IOException {
            String sensor = "3141_clwa_1422";
            int tran_num =5;
            String tran_log_path = "/Users/binbingu/Documents/tranlog.txt";
            int[] hash={0,0,0,0,0};
        File filename = new File(tran_log_path);
        String path = "/Users/binbingu/Documents/test.sql";
        BufferedReader reader;
        Connection conn = null;
        Statement pst = null;

        try {
            File writename = new File(tran_log_path);
            writename.createNewFile();
            BufferedWriter out = new BufferedWriter(new FileWriter(writename));
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
            String line;
            int i = 0;

            while ((line = reader.readLine()) != null) {
                String[] insert = line.split(",");
                String[] sensorid111 = insert[insert.length-1].split("\\)");
                String[] sensorid = sensorid111[0].split("\\'");   //sensor_id
                String[] timestamp111 = insert[insert.length-2].split(",");
                String[] timestamp = timestamp111[0].split("\\'");
                if (sensorid[1].equalsIgnoreCase(sensor)) {
                    i++;
                    hash[i-1] = Hashvalue(sensorid[1]+timestamp[1]);
                    System.out.println(hash[i-1]);
                    if (i==tran_num) {
                        out.write(line);
                        break;   //only keep the last 5 updates
                    }
                    else
                        out.write(line + "\r\n");
                }

            }
            out.flush();
            out.close();
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        //return the hash value according to <sensor_id, timestamp>
        return hash;
    }


    public static void TranProcessing() throws SQLException {
        String path = "/Users/binbingu/Documents/test.sql";
        BufferedReader reader;
        Connection conn = null;
        Statement pst = null;

        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5432/test", "postgres", "016111");
            pst = conn.createStatement();
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
            String line;
            int i = 0;
            while ((line = reader.readLine()) != null) {
                pst.addBatch(line);
                  /*  System.out.println("-----------------------");
                    System.out.println(line);
                    System.out.println("-----------------------");*/
                if (i % 10 == 0) {
                    System.out.println("执行了：" + i);
                    pst.executeBatch();
                }
                i += 1;
            }
            reader.close();
            // 执行批量更新
            pst.executeBatch();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }





}
