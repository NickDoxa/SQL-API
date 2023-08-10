package net.oasisgames.datasql.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

/**
 * @author Nick Doxa
 * @apiNote MySQL API created by Nick Doxa for free use by developers every where!
 * <br><br>
 * This API does 2 major things, creates an SQL Table inside a Database, and allows you to modify it. To use this table you need at minimum
 * basic Java knowledge, and basic understanding of how SQL works. This is more of a shortcut than a cheatsheet so if you are new to Java
 * I would say it would be best if you learned the language better first and then come back to this. If you don't know SQL at all this might
 * not fully cover you but it will certainly help. I have attached API Notes and information to all methods and the class constructor.
 * This is really meant for people who just don't want to do the whole process of writing up this code over and over or copy and pasting old
 * SQL classes and converting them for use in a new project. There are methods for deleting data, adding data, and editing data but make
 * sure to read the API Notes if you aren't sure what something does, the name can be a bit vague and so can the parameter names which is
 * why I added detailed notes.
 * <br><br>
 * This is a very basic API meant for helping developers like me who don't feel like writing the basic SQL code to get started sending
 * and receiving information every time they start a new project. Everything has notes written in plain english not nerd talk,
 * so it should be understandable even for people trying to get out of a bad situation without a lot of education like me.
 * Check out my company if you want more information on me.
 * <br><br>
 * <strong>Website:</strong> <a href="https://oasisgames.net">https://oasisgames.net</a>
 */
public abstract class SQL {
	
	protected String tableName;
	private Map<Integer, Map<String, String>> creationKeyValues;
	private Map<Integer, String> valueNames;
	private String primaryKey;
	private static boolean printStatements;
	
	/**
	 * @param print - (boolean) Whether or not to print Debug/Console statements.
	 * True for print, False for hide.
	 */
	public static void setPrintStatements(boolean print) {
		printStatements = true;
	}
	
    /**
     * @apiNote
     * createValueArray() is a method for defining an array containing a key name followed by an SQL data type.
     * Example:	String[] columns = new String[]{"EXAMPLE1 VARCHAR(100)", "EXAMPLE2 INT(100)", "EXAMPLE3 BOOL"}
     * SQL DATA TYPES: <a href="https://www.w3schools.com/sql/sql_datatypes.asp">https://www.w3schools.com/sql/sql_datatypes.asp</a>
     * The outcome of this array goes directly into your SQL Table and Database through this class.
     */
	protected abstract String[] createKeyValueArray();
	
	
	/**
	* @apiNote You must add the login credentials here as a String array
	* @param tableName - (String) The table name within database
	* @param primaryKeyValue - (String) The primary key NAME for the table A.K.A. what piece of data you will use to access the rest
	* @param credentials - (String Array) The array of necessary information to log into a SQL database.
	* The Strings must be in order and must not exceed or be less than 5 total strings. They go in the order of:
	* <i>Host, Port, Database, Username, Password. <strong>See the API Notes for the connect() method for more detail.</i></strong>
    * @param createTable - (Boolean) Whether or not to create a table or just connect.
	* True to create a table, false to just connect.
	*/
	public SQL(String tableName, String primaryKeyValue, String[] credentials, 
			boolean createTable) {
		this.tableName = tableName;
		creationKeyValues = createKeyValueMap();
		primaryKey = primaryKeyValue;
		//Numbered HashMap of value names (not types)
		valueNames = new HashMap<Integer, String>();
		for (int i=1;i<creationKeyValues.size()+1;i++) {
			for (String s : creationKeyValues.get(i).keySet()) {
				valueNames.put(i, s);
			}
		}
		try {
			connect(credentials[0], credentials[1], credentials[2], credentials[3], credentials[4]);
		} catch (SQLException e) {
			printToConsole("The database did not connect!");
			if (printStatements) e.printStackTrace();
		}
		if (isConnected() && createTable) {
			this.createTable(primaryKey);
		}
	}
	
	/**
	* @apiNote You must add the login credentials here as a String array
	* @param credentials - (String Array) The array of necessary information to log into a SQL database.
	* The Strings must be in order and must not exceed or be less than 5 total strings. They go in the order of:
	* <i>Host, Port, Database, Username, Password. <strong>See the API Notes for the connect() method for more detail.</i></strong>
	*/
	public SQL(String[] credentials) {
		try {
			connect(credentials[0], credentials[1], credentials[2], credentials[3], credentials[4]);
		} catch (SQLException e) {
			printToConsole("The database did not connect!");
			if (printStatements) e.printStackTrace();
		}
	}
    
    private static Connection connection;

    /**
     * @apiNote Method used to connect to the SQL database. Sets the private static Connection paramater of the class to the current
     * connection. Connections are singletons, only one may exist.
     * @param host - The address of the database host <i>(DO NOT INCLUDE PORT)</i>
     * @param port - The port of the database host
     * @param database - The name of the database you are using.
     * @param username - The username of the database.
     * @param password - The password for the database.
     * @throws SQLException
     */
    public void connect(String host, String port, String database, String username, String password) throws SQLException {
    	if (password == null) password = "";
    	printToConsole("Connecting to database!");
	    connection = DriverManager.getConnection("jdbc:mysql://" +
	    	     host + ":" + port + "/" + database + "?useSSL=false",
	    	     username, password);
	    printToConsole("Connected successfully to MySQL Database!");
    }
    
    /**
     * @apiNote Method used to disconnect from the SQL database.
     */
    public void disconnect() {
    	if (isConnected()) {
    		try {
    			connection.close();
    			printToConsole("Disconnected successfully from MySQL Database!");
    		} catch(SQLException e) {
    			e.printStackTrace();
    		}
    	}
    }
    
    /**
     * @return Connection - Returns the standing connection. Only one connection can be happening at a time.
     */
    protected static Connection getConnection() {
    	return connection;
    }
    
    /**
     * @return Boolean - returns true if connected and false if not connected.
     */
    public boolean isConnected() {
    	return (connection == null ? false : true);
    }
    
    /**
     * @param msg - (String) the message you want to send to console
     * @return Boolean - Returns true if printStatements are true and no errors occurred. returns false if printStatements is false or
     * an error occured.
     */
    public static boolean printToConsole(String msg) {
    	if (printStatements) System.out.println("[SQL API] " + msg);
    	return printStatements && !System.out.checkError();
    }
    
    /**
     * @apiNote this takes the <i>Key Value Array</i> from the abstract method <strong>createKeyValueArray()</strong> and converts it
     * to a numbered map with split Strings for the type and the Name of each data entry.
     * <br><br><strong>Learning Moment:</strong> There might be a more efficient way to do this and some day I'm sure
     * I will update this with more indepth information, but for now I want to explain what this method is doing for anyone interested.
     * This method takes the abstract method createKeyValueArray() and splits it from a String Array into a numbered map with a
     * String key and value map inside it. This means that the first entry in the map would look something like <0, <"INT", "AGE">>.
     * In this example the first key value (0 represents first in java) being stored on the table will be called 'AGE' and it will be an 
     * Integer. SQL Language is tricky so in order to make clean prepared statements, which are strings executed in an SQL database, we
     * need to use the exact phrasing and syntax. This method does not send the prepared statements it just creates the Map so that
     * the method createTable() can loop through the map and make a clean prepared statement.
     * @return Map<Integer, Map<String, String>> - Returns a numbered map containing another map with a String key and a String value
     * pair.
     */
    private Map<Integer, Map<String, String>> createKeyValueMap() {
    	Map<Integer, Map<String, String>> numericKeys = new HashMap<Integer, Map<String, String>>();
    	Map<String, String> keys;
    	int count = 0;
    	for (String s : this.createKeyValueArray()) {
    		keys  = new HashMap<String, String>();
    		String[] keyArray = s.split(" ", 2);
    		keys.put(keyArray[0], keyArray[1]);
    		printToConsole("Map: " + (count+1) + ", Key: " + keyArray[0] + ", Value: " + keyArray[1]);
    		numericKeys.put((count+1), keys);
    		printToConsole("Map: " + (count+1) + ", Size: " + numericKeys.get(count+1).size());
    		count++;
    	}
    	printToConsole("Key Value Map created with a total of: " + count + " keys.");
    	return numericKeys;
    }
    
    /**
     * @apiNote Creates a table in the database with the name predefined in the constructor. Only applies if a table with that 
     * name doesnt exist already.
     */
    public void createTable(String primaryKeyValue) {
    	creationKeyValues = createKeyValueMap();
		primaryKey = primaryKeyValue;
		//Numbered HashMap of value names (not types)
		valueNames = new HashMap<Integer, String>();
		for (int i=1;i<creationKeyValues.size()+1;i++) {
			for (String s : creationKeyValues.get(i).keySet()) {
				valueNames.put(i, s);
			}
		}
    	PreparedStatement ps;
    	String statement = "CREATE TABLE IF NOT EXISTS " + tableName + " (";
    	for (int i=1;i<creationKeyValues.size()+1;i++) {
    		for (String s : creationKeyValues.get(i).keySet()) {
    			statement = statement + s + " " + creationKeyValues.get(i).get(s) + ",";
    		}
    	}
    	statement = statement + "PRIMARY KEY (" + primaryKey + "))";
    	try {
    		ps = getConnection().prepareStatement(statement);
    		ps.executeUpdate();
    	} catch (NullPointerException e) {
    		printToConsole("Table name was null. "
    				+ "Data operation failed.\nDISCONNECTING");
    		if (this.isConnected()) disconnect();
    	} catch (SQLSyntaxErrorException e) {
    		printToConsole("SQL Syntax was wrong. This is the result of either "
    				+ "not adding any data columns, or adding an invalid name. \nDISCONNECTING");
    		e.printStackTrace();
    		if (this.isConnected()) disconnect();
    	} catch (SQLException e) {
    		printToConsole("General SQL Exception. DISCONNECTING\n"
    				+ "See stack trace:");
    		e.printStackTrace();
    		if (this.isConnected()) disconnect();
    	} finally {
    		printToConsole("Table creation method ran. If no errors appeared the table was"
    				+ " created successfully");
    	}
    }
    
    /**
     * @apiNote This method is used to create a new primary key value with null data under all the possible values. 
     * <br><br><strong><i>Dummy Explanation:</i></strong> Think of it as an excel sheet: Adding a new column but not putting any data.
     * @param key - (String) The data for the primary key
     */
    public void createNewKeyValue(String key) {
		try {
			if (!keyExists(key)) {
				String statement = "INSERT IGNORE INTO " + tableName + " (" + primaryKey;
				int q = 1;
				for (int i=1;i<valueNames.size();i++) {
					statement = statement + valueNames.get(i) + ",";
					q++;
				}
				statement = statement.substring(0, statement.length()) + ") VALUES (";
				for (int i=0;i<q;i++) {
					statement = statement + "?,";
				}
				statement = statement.substring(0, statement.length()) + ")";
				PreparedStatement ps = getConnection().prepareStatement(statement);
				ps.setString(1, key);
				for (int i=2;i<q;i++) {
					ps.setObject(i, null);
				}
				ps.executeUpdate();
				return;
			}
		} catch (SQLException | NullPointerException e) {
			return;
		}
	}
    
   	/**
   	 * @apiNote This method is used to add a new entry into the table with a new Primary Key and all new data entries.
   	 * <strong>NOTE:</strong> when using this method it is best to not leave any of the entries null, instead give them identifiable
   	 * but empty data. For instance if it was a string use "", if it's a number use 0, if it's a boolean use false. Using this method
   	 * with null properties can lead to Null Pointer Exceptions. <br><br><strong><i>Dummy Explanation:</i></strong> Think of it as an excel sheet: Adding a new column and adding the
   	 * correct values in each column corresponding to it.
   	 * @param key - (String) The data for the primary key
   	 * @param args - (Object Array) - The values the primary key has currently
   	 */
    public void createNewKeyValue(String key, Object[] args) {
		try {
			if (!keyExists(key)) {
				String statement = "INSERT IGNORE INTO " + tableName + " (" + primaryKey;
				int q = 1;
				for (int i=1;i<valueNames.size();i++) {
					statement = statement + valueNames.get(i) + ",";
					q++;
				}
				statement = statement.substring(0, statement.length()) + ") VALUES (";
				for (int i=0;i<q;i++) {
					statement = statement + "?,";
				}
				statement = statement.substring(0, statement.length()) + ")";
				PreparedStatement ps = getConnection().prepareStatement(statement);
				ps.setString(1, key);
				for (int i=2;i<q;i++) {
					ps.setObject(i, args[i-1]);
				}
				ps.setObject(q, args[q]);
				ps.executeUpdate();
				return;
			}
		} catch (SQLException | NullPointerException e) {
			return;
		}
	}
    
    /**
     * @apiNote This is the only method for changing already existing values on the table. By searching through the index for
     * a <i>primary key</i> and then finding the value of the given <i>key</i> it can change the data using whatever is given in the 
     * <i>value</i> parameter.<br><br><strong><i>Dummy Explanation:</i></strong> Think of it like an excel sheet: The primary key is the row, the key is the column, and the value is what
     * to put in the box.
     * @param key - (String) The name of the value you are trying to set.
     * @param value - (Object) The data of the value you are setting.
     * @param primary - (String) The data for the primary key. 
     */
    public void setValue(String key, Object value, String primary) {
		try {
			PreparedStatement ps = getConnection().prepareStatement("UPDATE " + tableName + " SET " + key + "=? WHERE " + primaryKey + "=?");
			ps.setObject(1, value);
			ps.setObject(2, primary);
			ps.executeUpdate();
		} catch (SQLException e) {
			printToConsole("Error executing value update method! @ SQLManager:setValue(String, Object, String)");
		}
    }
    
	/**
	 * @apiNote this method checks to see if a key exists but it does not check if the key values have data. If you are trying to
	 * search for data using the primary key and it says it exists but you are getting a <i>NullPointerException</i> that means that
	 * the key exists but the data you are trying to find is null and you must use the <strong>setValue() method</strong> to give it data.
	 * @param key - (String) The data of the primary key A.K.A. what we are searching for in the method.
	 * @return Boolean - Returns true if the key exists and false if it does not or it encounters an error searching.
	 */
	public boolean keyExists(String key) {
		try {
			PreparedStatement ps = getConnection().prepareStatement("SELECT * FROM " + tableName + " WHERE " + primaryKey + "=?");
			ps.setString(1, key);
			ResultSet results = ps.executeQuery();
			if (results.next()) {
				//Key found
				return true;
			}
			//Key not found
			return false;
		} catch (SQLException | NullPointerException e) {
			return false;
		}
	}
	
	/**
	 * @apiNote This method returns data from the SQL Database by searching for a data entry using the primaryKey
	 * and then returning whatever value is under the given dataset of the valueKey. In this instance <i>key</i> is the the value
	 * of <i>primaryKey.</i><br><br><strong><i>Dummy Explanation:</i></strong> Think of it like an excel sheet: It's like searching for a row that has a specific string in the
	 * first box (key parameter) and then returning whatever is in a specific column of that row (valueKey parameter).
	 * @param key - (String) The data of the primary key
	 * @param valueKey - (String) The type of value you are trying to get in the result
	 * @return ResultSet - Raw reults of the SQL query
	 */
	public ResultSet getValueByKey(String key, String valueKey) {
		try {
			PreparedStatement ps = getConnection().prepareStatement("SELECT " + valueKey + " FROM " + tableName + " WHERE " + primaryKey + "=?");
			ps.setString(1, key);
			ResultSet rs = ps.executeQuery();
			return rs;
		} catch (SQLException | NullPointerException e) {
			return null;
		}
	}
	
	/**
	 * @apiNote This method is used to get all the current tables by name in the database
	 * @return String Array - Returns an array of all the table names in the database
	 */
	public String[] getAllTables() throws SQLException {
		String[] output = new String[0];
		try {
			PreparedStatement ps = getConnection().prepareStatement("SHOW TABLES");
		    ResultSet tables = ps.executeQuery();
		    List<String> results = new ArrayList<String>();
		    int count = 0;
		    while (tables.next()) {
		    	results.add(tables.getString(1));
		    	count++;
		    }
		    if (count < 1) {
		    	net.oasisgames.datasql.database.Connection.printToConsole("No tables found!");
		    	return new String[0];
		    }
		    return results.stream()
		    .filter(str -> !str.isBlank() && !str.isEmpty())
		    .toArray(String[]::new);
		} catch (NullPointerException e) {
			net.oasisgames.datasql.database.Connection.printToConsole("Error: No tables were found!"
					+ "\nError:");
			e.printStackTrace();
		} finally {
			net.oasisgames.datasql.database.Connection.printToConsole("Tables fetched from database!");
		}
		return output;
	}
	
	/**
	 * @apiNote This method is used to check if the table name given exists within the database.
	 * @param table - (String) The name of the table to check
	 * @return Boolean - Returns true if the table exists, false if not
	 */
	public boolean tableExists(final String table) throws SQLException {
		return Arrays.asList(getAllTables())
				.stream()
				.map(str -> str.equals(table))
				.findAny()
				.get();
	}
	
	
	/**
	 * @apiNote Empties the entire table of all data entries. <br><br><strong><i>Dummy Explanation:</i></strong> Think of it as an excel sheet: Deleting the entire sheet.
	 */
	public void emptyTable() {
		try {
			PreparedStatement ps = getConnection().prepareStatement("TRUNCATE " + tableName);
			ps.executeUpdate();
		} catch (SQLException e) {
			printToConsole("Error truncating table!");
		}
	}
	
	/**
	 * @apiNote Removes a specific key and all the given data saved with it. <br><br><strong><i>Dummy Explanation:</i></strong> Think of it as an excel sheet: Removing an entire row but
	 * not deleting the entire sheet.
	 * @param key - (String) The data of the primary key
	 */
	public void removeAllValues(String key) {
		try {
			PreparedStatement ps = getConnection().prepareStatement("DELETE FROM " + tableName + " WHERE " + primaryKey + "=?");
			ps.setString(1, key);
			ps.executeUpdate();
		} catch (SQLException e) {
			printToConsole("Error removing key set from table!");
		}
	}
	
}
