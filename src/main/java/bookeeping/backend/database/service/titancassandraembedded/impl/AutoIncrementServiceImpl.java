package bookeeping.backend.database.service.titancassandraembedded.impl;

import java.util.Iterator;

import bookeeping.backend.database.MandatoryProperties;
import bookeeping.backend.database.connection.singleton.TitanCassandraEmbeddedConnection;
import bookeeping.backend.database.service.AutoIncrementService;
import bookeeping.backend.utilities.AlphaNumericOperation;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.tinkerpop.blueprints.Vertex;

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
			Iterator<Vertex> iterator = this.titanGraph.getVertices(MandatoryProperties.nodeId.name(), "0").iterator();
			Vertex autoIncrement = null;
			while(iterator.hasNext())
			{
				autoIncrement = iterator.next();
			}
			
			String nextIncrement = autoIncrement.getProperty(MandatoryProperties.next.name());
			autoIncrement.setProperty(MandatoryProperties.next.name(), AlphaNumericOperation.add(nextIncrement, 1));
			
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
