package com.hw1.persistence;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
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
        int n = persistables.size();

        // make INSERT query
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(c.getSimpleName()).append(" ("); // " (" for col names

        // col names
        for (int i = 0; i < n; i++) {
            Field f = persistables.get(i);
            sql.append(dbHelper.camelToSnakeCase(f.getName())); // make cases compatible between Java and SQLite
            if (i < n - 1) { // don't add for last item
                sql.append(", "); // prep next 
            }
        }
        sql.append(") VALUES ("); // ")" ends col names, "VALUES (" sets up vals

        // temp names
        for (int i = 0; i < n; i++) {
            sql.append("?"); // data will be inserted momentarily
            if (i < n - 1) { // don't add for last item
                sql.append(", "); // prep next 
            }
        }
        sql.append(")"); // cap val names with ")"

        // perform INSERT query
        try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {
            // set field vals
            for (int i = 0; i < n; i++) {
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
    // unchecked warning pmo
    @SuppressWarnings("unchecked") 
    // Gemini helped with the proxy instantiation, and its proposed solution required to add extra exception types in declaration
    public <T> T loadRow(T obj) throws SQLException, IllegalAccessException, java.lang.NoSuchMethodException, InstantiationException, InvocationTargetException{
        Class<?> c = obj.getClass();
        Field[] fields = c.getDeclaredFields();

        // locate primary key & its val
        Field pkField = null;
        Object pkVal = null;
        for (Field f : fields) {
            if (f.isAnnotationPresent(PrimaryKey.class)) {
                pkField = f;
                f.setAccessible(true);
                pkVal = f.get(obj);
                break; // only one primary key per class
            }
        }

        // if we failed to find a primary key:
        if (pkField == null || pkVal == null) {
            System.err.println("Failed either to find primary key or its value");
            System.exit(1);
            return null; // Return null if no row was found
        }

        // store @Persistable fields in List
        List<Field> persistables = new ArrayList<>();
        for (Field f : fields) {
            if (f.isAnnotationPresent(Persistable.class)) {
                persistables.add(f);
            }
        }
        int n = persistables.size();

        // make SELECT query
        // "SELECT *persistable field* FROM *class* WHERE *primary key field* = ?"
        StringBuilder sql = new StringBuilder("SELECT ");
        for (int i = 0; i < n; i++) {
            Field f = persistables.get(i);
            sql.append(dbHelper.camelToSnakeCase(f.getName()));
            if (i < n - 1) {
                sql.append(", ");
            }
        }
        sql.append(" FROM ").append(c.getSimpleName());
        sql.append(" WHERE ").append(dbHelper.camelToSnakeCase(pkField.getName())).append(" = ?");

        // perform query
        try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {
            // set pk val
            if (pkField.getType() == String.class) {
                ps.setString(1, (String) pkVal);
            } else if (pkField.getType() == int.class || pkField.getType() == Integer.class) {
                ps.setInt(1, (Integer) pkVal);
            }

            // resulting database set
            try (ResultSet rs = ps.executeQuery()) {
                // graceful exit if primary key can't be found
                if (!rs.next()) {
                    System.err.println("Could not find primary key");
                    System.exit(1);
                    return null;
                }

                // check for @RemoteLazyLoad
                boolean isLazy = false;
                List<String> lazyFields = new ArrayList<>();
                for (Field f : persistables) {
                    if (f.isAnnotationPresent(RemoteLazyLoad.class)) {
                        isLazy = true;
                        lazyFields.add(f.getName());
                    }
                }

                // make new obj
                T res;
                try {
                    res = (T) c.getDeclaredConstructor().newInstance();
                } catch (ReflectiveOperationException e) {
                    throw new RuntimeException("Could not create new instancee of " + c.getName(), e);
                }

                // populate fields
                for (Field f : persistables) {
                    f.setAccessible(true);
                    String col = dbHelper.camelToSnakeCase(f.getName());
                    
                    if (f.getType() == String.class) {
                        f.set(res, rs.getString(col));
                    } else if (f.getType() == int.class || f.getType() == Integer.class) {
                        f.set(res, rs.getInt(col));
                    } else if (f.getType() == byte[].class) {
                        f.set(res, rs.getBytes(col));
                    }
                }

                // if no lazies, return curr state of res
                if (!isLazy) {
                    return res;
                }

                // make proxy for lazy loading with Javassist
                ProxyFactory factory = new ProxyFactory();
                factory.setSuperclass(c);

                MethodHandler handler = new MethodHandler() {
                    @Override
                    public Object invoke(Object self, Method thisMethod, Method proceed, Object[] args) throws Throwable {
                        String methodName = thisMethod.getName();

                        // handle getters
                        if (methodName.startsWith("get") && methodName.length() > 3) {
                            // (getFieldName --> fieldName)
                            String fieldName = methodName.substring(3, 4).toLowerCase() + methodName.substring(4);

                            // lazy?
                            if (lazyFields.contains(fieldName)) {
                                // retreive field
                                Field f = null;
                                try {
                                    f = c.getDeclaredField(fieldName);
                                    f.setAccessible(true);
                                    Object val = f.get(self);

                                    // check for URL
                                    if (val instanceof byte[]) {
                                        byte[] bytes = (byte[]) val;
                                        String possibleURL = new String(bytes);

                                        if (possibleURL.startsWith("http")) {
                                            // load stuff
                                            byte[] loaded = dbHelper.fetchContentFromUrl(possibleURL);
                                            return loaded;
                                        }
                                    }
                                    return val;
                                } catch (NoSuchFieldException e) {}
                            }
                        }

                        // if not getter, just invoke
                        return proceed.invoke(self, args);
                    }
                };

                // create proxy obj with res obj
                T resultProxy = (T) factory.create(new Class<?>[0], new Object[0], handler);
                for (Field f : fields) {
                    f.setAccessible(true);
                    f.set(resultProxy, f.get(res));
                }

                return resultProxy;

            }
        }
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


