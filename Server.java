import java.io.*;
import java.net.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;


public class Server {
    boolean closed = false, inputFromAll = false;
    List<ClientThread> thread;
    List<String> data;
    private Connection connect = null;
    private Statement statement = null;


    Server() {
        thread = new ArrayList<ClientThread>();
        data = new ArrayList<String>();
    }

    public static void main(String args[])
    {
        Socket clientSocket = null;
        ServerSocket serverSocket = null;
        int port_number = 6666;
        Server server = new Server();
        //connect to sql
        //server.connectToPostgresSql();
        //start 2PC
        //write a transaction
        try
        {
            serverSocket = new ServerSocket(port_number);
        } catch (IOException e) {
            System.out.println(e);
        }
        while (!server.closed)
        {
            try {
                clientSocket = serverSocket.accept();
                ClientThread clientThread = new ClientThread(server, clientSocket);
                (server.thread).add(clientThread);
                System.out.println("\nThe number of clients is : " + (server.thread).size());
                (server.data).add("NOT_SENT");
                clientThread.start();
            } catch (IOException e) { }
        }
        try {
            serverSocket.close();
        } catch (Exception e1) { }
    }

}


class ClientThread extends Thread
{
    DataInputStream is = null;
    String line1;
    String line;
    String destClient = "";
    String name;
    PrintStream os = null;
    Socket clientSocket = null;
    String clientIdentity;
    Server server;

    public ClientThread(Server server, Socket clientSocket)
    {
        this.clientSocket = clientSocket;
        this.server = server;
    }

    @SuppressWarnings("deprecation")
    public void run()
    {
        String sensor_name = "3141_clwa_1422";
        String request_tran_path = "/Users/binbingu/Documents/requestTran.txt";
        String tran_log_path = "/Users/binbingu/Documents/tranlog.txt";   //used to log the state of transactions
        try {
            File writename = new File(tran_log_path);
            writename.createNewFile();
            BufferedWriter out = new BufferedWriter(new FileWriter(writename, true)); //log transaction


            try {
                is = new DataInputStream(clientSocket.getInputStream());
                os = new PrintStream(clientSocket.getOutputStream());
                os.println("Enter the name of a database: e.g. (node1, node2, node3)");
                name = is.readLine();
                clientIdentity = name;
                // connect to the corresponding database with clientIdentity
                String url = "jdbc:postgresql://localhost:5432/"+clientIdentity;
                String user = "postgres";
                String password = "016111";
                os.println("Welcome " + name + " to this 2PC.");
                os.println("Please type \"fail\" if you wanna make the server disconnected, otherwise type any other strings:");
                for (int i = 0; i < (server.thread).size(); i++)
                {
                    if ((server.thread).get(i) != this)
                    {
                        ((server.thread).get(i)).os.println("---A new user " + name + " entered ---");
                    }
                }
                while (true)
                {
                    //connect to Sql
                    Connection c = null;
                    try {
                        Class.forName("org.postgresql.Driver");
                        c = DriverManager.getConnection(url, user, password);
                        c.setAutoCommit(true);
                        System.out.println("Successfully Connect PostgreSql！");

                    } catch (Exception e) {
                        e.printStackTrace();
                        System.err.println(e.getClass().getName() + ": " + e.getMessage());
                        System.exit(0);
                    }
                    //failure test
                    int[] hash = TranProcessing(sensor_name);
                    //read the transaction
                    FileInputStream inputStream = new FileInputStream(request_tran_path);
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

                    String str = null;
                    String[] tran=new String[5];  //store the transactions
                    int ks = 0;
                    while((str = bufferedReader.readLine()) != null)
                    {
                        //System.out.println(str);
                        tran[ks]=str;
                        ks++;
                    }
                    //get the hash values for tran[]
                    out.write("Start a new transaction\n");
                    line1 = is.readLine();
                    if (line1.equalsIgnoreCase( "fail")) {
                        line = "ABORT";
                        out.write(clientIdentity+" replies "+line+"\n");
                        out.flush();
                    }
                    else{  //prepare
                        line = "COMMIT";
                        //assign the corresponding transactions to the right node or database
                        PrepareSql(c, clientIdentity, tran, hash);
                        out.write(clientIdentity+" replies "+line+"\n");
                        out.flush();
                    }

                    //line = is.readLine();
                    if (line.equalsIgnoreCase("ABORT"))
                    {
                        System.out.println("\nFrom '" + clientIdentity
                                + "' : ABORT\n\nSince aborted we will not wait for inputs from other clients.");
                        System.out.println("\nAborted....");
                        out.write("\nFrom '" + clientIdentity + "' : ABORT\n");
                        out.write("\nAborted");

                        for (int i = 0; i < (server.thread).size(); i++) {
                            if (!line1.equalsIgnoreCase( "fail"))
                                RollbackSql(c);
                            ((server.thread).get(i)).os.println("GLOBAL_ABORT");
                            ((server.thread).get(i)).os.close();
                            ((server.thread).get(i)).is.close();
                        }
                        break;
                    }
                    if (line.equalsIgnoreCase("COMMIT"))
                    {
                        System.out.println("\nFrom '" + clientIdentity + "' : COMMIT");
                        if ((server.thread).contains(this))
                        {
                            (server.data).set((server.thread).indexOf(this), "COMMIT");
                            for (int j = 0; j < (server.data).size(); j++)
                            {
                                if (!(((server.data).get(j)).equalsIgnoreCase("NOT_SENT")))
                                {
                                    server.inputFromAll = true;
                                    continue;
                                }
                                else{
                                    server.inputFromAll = false;
                                    System.out.println("\nWaiting for inputs from other clients.");
                                    break;
                                }
                            }
                            if (server.inputFromAll)
                            {
                                System.out.println("\n\nCommited....");
                                for (int i = 0; i < (server.thread).size(); i++)
                                {
                                    CommitSql(c);
                                    out.write("GLOBAL_COMMIT for "+clientIdentity);
                                    ((server.thread).get(i)).os.println("GLOBAL_COMMIT");
                                    ((server.thread).get(i)).os.close();
                                    ((server.thread).get(i)).is.close();
                                }
                                break;
                            }
                        } // if thread.contains
                    } // commit
                } // while
                server.closed = true;
                clientSocket.close();
            } catch (IOException e) { } catch (SQLException e) {
                e.printStackTrace();
            }

            out.flush();
            out.close();

        }catch (Exception e) {
            e.printStackTrace();
        }

    }


    public void PrepareSql(Connection c, String dbname, String[] tran, int hash[])
    {
        Statement stmt = null;
        try {
            stmt = c.createStatement();
            //find the transactions whose hash value is 1
            if (ExistTran(dbname, hash)) {
                String sql = "Begin;";
                stmt.executeUpdate(sql);

                for (int x = 0; x < hash.length; x++) {
                    if (hash[x] == 1) {
                        sql = tran[x];
                        stmt.executeUpdate(sql);
                    }
                }

                sql = "PREPARE TRANSACTION 'T2';";
                stmt.executeUpdate(sql);
            }
            stmt.close();
            //c.close();

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

    //judge if a transaction contains relevant updates to a database
    public boolean ExistTran(String dbname, int hash[]) {
        boolean ss =false;
        if (dbname.equalsIgnoreCase("node1")){
            for (int x = 0; x<hash.length; x++) {
                if (hash[x] == 1) {
                    ss = true;
                    break;
                }
            }
        }
        else
        if (dbname.equalsIgnoreCase("node2")){
            for (int x = 0; x<hash.length; x++) {
                if (hash[x] == 2) {
                    ss = true;
                    break;
                }
            }
        }
        else
        if (dbname.equalsIgnoreCase("node3")){
            for (int x = 0; x<hash.length; x++) {
                if (hash[x] == 3) {
                    ss = true;
                    break;
                }
            }
        }
        return ss;

    }

    public void CommitSql(Connection c)
    {
        Statement stmt = null;
        try {
            stmt = c.createStatement();

            String sql = "COMMIT PREPARED 'T2';";
            stmt.executeUpdate(sql);

            stmt.close();
            c.close();

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

    public void RollbackSql(Connection c)
    {
        Statement stmt = null;
        try {
            stmt = c.createStatement();

            String sql = "ROLLBACK PREPARED 'T2';";
            stmt.executeUpdate(sql);

            stmt.close();
            c.close();

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }


    public static int[] TranProcessing(String sensor) throws SQLException, IOException {
        //String sensor = "3141_clwa_1422";
        int tran_num =5;
        String request_tran_path = "/Users/binbingu/Documents/requestTran.txt";
        int[] hash={0,0,0,0,0};
        String path = "/Users/binbingu/Documents/test.sql";
        BufferedReader reader;


        try {
            File writename = new File(request_tran_path);
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
                    //System.out.println(hash[i-1]);
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

    public static void TranMaintain(){
        //Matin a log with state of transactions
    }

    public static int Hashvalue(String s) {
        int h = s.hashCode();
        return Math.abs(h%3)+1; //make the value in {1,2,3}
    }




}
