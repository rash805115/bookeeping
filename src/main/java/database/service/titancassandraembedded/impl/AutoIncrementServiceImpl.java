package database.service.titancassandraembedded.impl;

import java.util.Iterator;

import utilities.AlphaNumericOperation;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.tinkerpop.blueprints.Vertex;

import database.connection.singleton.TitanCassandraEmbeddedConnection;
import database.service.AutoIncrementService;

public class AutoIncrementServiceImpl implements AutoIncrementService
{
	private TitanGraph titanGraph;
	
	public AutoIncrementServiceImpl()
	{
		this.titanGraph = TitanCassandraEmbeddedConnection.getInstance().getTitanGraphObject();
	}
	
	@Override
	public String getNextAutoIncrement()
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			Iterator<Vertex> iterator = this.titanGraph.getVertices("nodeId", "0").iterator();
			Vertex autoIncrement = null;
			while(iterator.hasNext())
			{
				autoIncrement = iterator.next();
			}
			
			String nextIncrement = autoIncrement.getProperty("next");
			autoIncrement.setProperty("next", AlphaNumericOperation.add(nextIncrement, 1));
			
			titanTransaction.commit();
			return nextIncrement;
		}
		finally
		{
			if(titanTransaction.isOpen())
			{
				titanTransaction.rollback();
			}
		}
	}
}
