package bookeeping.backend.utilities.configurationproperties;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

public class DatabaseConnectionProperty
{
	private Properties properties;
	
	public DatabaseConnectionProperty()
	{
		this.properties = new Properties();
		try
		{
			this.properties.load(new FileInputStream(new File(System.getProperty("user.dir") + File.separator + "configurations" + File.separator + "databaseconnection.ini")));
		}
		catch (Exception exception)
		{
			this.properties = null;
			System.out.println("ERROR: Unreadable/Missing properties file.");
			exception.printStackTrace();
			System.exit(1);
		}
	}
	
	public String getProperty(String key)
	{
		String value = this.properties.getProperty(key);
		
		if(value == null)
		{
			System.out.println("ERROR: No such key in the properties file! - \'" + key + "\'.");
			System.exit(1);
		}
		
		return value;
	}
}
