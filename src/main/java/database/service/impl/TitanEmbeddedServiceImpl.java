package database.service.impl;

import org.apache.commons.configuration.BaseConfiguration;

import utilities.configurationproperties.DatabaseConnectionProperty;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;

import database.service.DatabaseService;

public class TitanEmbeddedServiceImpl implements DatabaseService
{
	private TitanGraph titanGraph;
	
	public TitanEmbeddedServiceImpl()
	{
		DatabaseConnectionProperty databaseConnectionProperty = new DatabaseConnectionProperty();
		String databaseBackend = databaseConnectionProperty.getProperty("TitanEmbeddedServerBackend");
		String databaseHostname = databaseConnectionProperty.getProperty("TitanEmbeddedServerHostname");
		
		BaseConfiguration baseConfiguration = new BaseConfiguration();
		baseConfiguration.setProperty("storage.backend", databaseBackend);
		baseConfiguration.setProperty("storage.hostname", databaseHostname);
		
		this.titanGraph = TitanFactory.open(baseConfiguration);
	}
}
