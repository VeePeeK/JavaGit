
package gs.veepeek.assessment;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import java.sql.*;

/**
 *
 * @author VeePeeK
 */
public class Assesment {

    private static String prFile = "properties.cfg";
    private static String defaultSeparator = ",";
    private static String newColumnDefaultType = "VARCHAR(50)";

    public static void main(String[] args) {

        //Properties values
        String path = null;
        String connectString = null;
        String table = null;
        String columns = null;
        String separator = null;
        String columnType = null;
        String[] cols = null;

        //Reading properties file
        String line = "";
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(prFile));

            while ((line = br.readLine()) != null) {
                if (line.startsWith("path:")) {
                    path = line.substring(5);
                } else if (line.startsWith("connect:")) {
                    connectString = line.substring(8);
                } else if (line.startsWith("table:")) {
                    table = line.substring(6);
                } else if (line.startsWith("columns:")) {
                    columns = line.substring(8);
                } else if (line.startsWith("separator:")) {
                    separator = line.substring(10);
                } else if (line.startsWith("coltype:")) {
                    columnType = line.substring(8);
                }
            }
            br.close();
        } catch (FileNotFoundException ex) {
            System.out.println("Error: properties file not found!");
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }

            }
        }

        //Checking properties for existing
        boolean errorExist = false;
        if (path == null || path.isEmpty()) {
            System.out.println("Error: path to csv file is not defined");
            errorExist = true;
        }
        if (connectString == null || connectString.isEmpty()) {
            System.out.println("Error: connection string is not defined");
            errorExist = true;
        }
        if (table == null || table.isEmpty()) {
            System.out.println("Error: table is not defined");
            errorExist = true;
        }
        if (columns == null || columns.isEmpty()) {
            System.out.println("Error: columns is not defined");
            errorExist = true;

        } else {
            cols = columns.split(",");
        }

        if (columnType == null || columnType.isEmpty()) {
            columnType = newColumnDefaultType;
        }
        if (separator == null || separator.isEmpty()) {
            separator = defaultSeparator;
        }

        //if all properties exist
        if (!errorExist) {
            Connection conn = null;
            CSVReader reader = null;
            try {
                //establish connection with database

                conn = DriverManager.getConnection(connectString);

                //check for table existence
                DatabaseMetaData dbm = conn.getMetaData();
                Statement stmt = conn.createStatement();
                ResultSet tablesSet = dbm.getTables(null, null, table, null);
                if (!tablesSet.next()) {
                    // Table does not exist, create it with all columns
                    String tableSql = "CREATE TABLE " + table + " (";
                    for (int i = 0; i < cols.length - 1; i++) {
                        tableSql += cols[i] + " " + columnType + ", ";
                    }
                    tableSql += cols[cols.length - 1] + " " + newColumnDefaultType + ")";
                    stmt.executeUpdate(tableSql);
                    System.out.println("New table \"" + table + "\" was created");
                }

                //check for all columns
                for (int i = 0; i < cols.length; i++) {
                    ResultSet columnsSet = dbm.getColumns(null, null, table, cols[i]);
                    if (!columnsSet.next()) {
                        //Column does not exist, add it with default type
                        stmt.executeUpdate("ALTER TABLE " + table + " ADD " + cols[i] + " " + newColumnDefaultType);
                        System.out.println("New column \"" + cols[i] + "\" was added");
                    }

                }

                //creating sql request depending on number of columns
                String sql = "INSERT INTO " + table + "(";

                for (int i = 0; i < cols.length - 1; i++) {
                    sql += cols[i] + ",";
                }
                sql += cols[cols.length - 1] + ") VALUES (";
                for (int i = 0; i < cols.length - 1; i++) {
                    sql += "?,";
                }
                sql += "?)";
                PreparedStatement st = conn.prepareStatement(sql);

                //opening csv file using openCSV
                CSVParser parser = new CSVParserBuilder()
                        .withSeparator(separator.charAt(0))
                        .withIgnoreQuotations(false)
                        .build();
                reader = new CSVReaderBuilder(new FileReader(path))
                        .withCSVParser(parser)
                        .build();
                String[] readed = null;

                int rowsAdded = 0;
                //starting to read
                while ((readed = reader.readNext()) != null) {
                    //check if  number of columns and readed values are the same
                    if (readed.length == cols.length) {
                        for (int i = 0; i < readed.length; i++) {
                            st.setObject(i + 1, readed[i]);
                        }
                        rowsAdded += st.executeUpdate();
                    }
                }
                System.out.println(rowsAdded + " rows was added");
            } catch (SQLException ex) {
                ex.printStackTrace();
            } catch (FileNotFoundException ex) {
                ex.printStackTrace();
            } catch (IOException ex) {
                ex.printStackTrace();
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {

                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException ex) {
                        ex.printStackTrace();
                    }
                }
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }

                }
            }
        }
    }

}
