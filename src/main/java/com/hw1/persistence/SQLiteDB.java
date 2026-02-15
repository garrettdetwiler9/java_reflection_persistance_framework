package com.hw1.persistence;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;

public class SQLiteDB {
    private Connection connection;
    private SQLiteDBHelper dbHelper;
    
    public SQLiteDB(String dbPath) throws SQLException {
        this.connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
        this.dbHelper = new SQLiteDBHelper(this.connection);
    }
    
    public void createTable(Class<?> clazz) throws SQLException {
        
        StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        sql.append(clazz.getSimpleName()).append(" (");
        
        Field[] fields = clazz.getDeclaredFields();
        List<Field> persistableFields = new ArrayList<>();
        
        // Only include fields marked with @Persistable annotation
        for (Field field : fields) {
            if (field.isAnnotationPresent(Persistable.class)) {
                persistableFields.add(field);
            }
        }
        
        for (int i = 0; i < persistableFields.size(); i++) {
            Field field = persistableFields.get(i);
            String fieldName = dbHelper.camelToSnakeCase(field.getName());
            String sqlType = dbHelper.getSQLType(field.getType());
            
            sql.append(fieldName).append(" ").append(sqlType);
            
            // Check if field is marked as PrimaryKey
            if (field.isAnnotationPresent(PrimaryKey.class)) {
                sql.append(" PRIMARY KEY");
            }
            
            if (i < persistableFields.size() - 1) {
                sql.append(", ");
            }
        }
        
        sql.append(")");
        
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql.toString());
        }
    }
    

    public void droptTable(Class<?> clazz) throws SQLException {
        String sql = "DROP TABLE IF EXISTS " + clazz.getSimpleName();
        
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }   
    
    /**
     * Insert the given object into the database using reflection.
     * Only fields annotated with @Persistable should be stored.
     * Check the handout for more details.
     */
    public void insertRow(Object obj) throws SQLException, IllegalAccessException {
        // get obj class and fields, store in vars
        Class<?> c = obj.getClass();
        Field[] fields = c.getDeclaredFields();

        // store @Persistable fields in List
        List<Field> persistables = new ArrayList<>();
        for (Field f : fields) {
            if (f.isAnnotationPresent(Persistable.class)) {
                persistables.add(f);
            }
        }

        // make INSERT query
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(c.getSimpleName()).append(" ("); // " (" for col names

        // col names
        for (int i = 0; i < persistables.size(); i++) {
            Field f = persistables.get(i);
            sql.append(dbHelper.camelToSnakeCase(f.getName())); // make cases compatible between Java and SQLite
            if (i < persistables.size() - 1) { // don't add for last item
                sql.append(", "); // prep next 
            }
        }
        sql.append(") VALUES ("); // ")" ends col names, "VALUES (" sets up vals

        // temp names
        for (int i = 0; i < persistables.size(); i++) {
            sql.append("?"); // data will be inserted momentarily
            if (i < persistables.size() - 1) { // don't add for last item
                sql.append(", "); // prep next 
            }
        }
        sql.append(")"); // cap val names with ")"

        // perform INSERT query
        try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {
            // set field vals
            for (int i = 0; i < persistables.size(); i++) {
                Field f = persistables.get(i);
                f.setAccessible(true);
                Object val = f.get(obj);

                // set data based on type
                // I'm genuinely upset that switch statements don't support f.getType()
                if (f.getType() == String.class) {
                    ps.setString(i + 1, (String) val);
                } else if (f.getType() == byte[].class) {
                    ps.setBytes(i + 1, (byte[]) val);
                } else if (f.getType() == int.class || f.getType() == Integer.class) {
                    ps.setInt(i + 1, (Integer) val);
                } 
            }

            ps.executeUpdate(); // execute query
        }
        

    }

    /**
     * Load a row from the database using reflection.
     * The object passed in should have its primary key field populated.
     * This method will load the other persistable fields from the database
     * and return a proxy object that lazy-loads fields annotated with @RemoteLazyLoad.
     * Check the handout for more details. 
     * 
     */
    public <T> T loadRow(T obj) throws SQLException, IllegalAccessException {


        // Return null if no row was found
        return null;
    }
    
    
    public Connection getConnection() {
        return connection;
    }
    
    public void close() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
}


