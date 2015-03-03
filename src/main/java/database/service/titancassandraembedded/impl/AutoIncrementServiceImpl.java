package database.service.titancassandraembedded.impl;

import java.util.Iterator;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.tinkerpop.blueprints.Vertex;

import database.connection.singleton.TitanCassandraEmbeddedConnection;
import database.service.AutoIncrementService;
import database.titan.NodeLabels;

public class AutoIncrementServiceImpl implements AutoIncrementService
{
	private TitanGraph titanGraph;
	
	public AutoIncrementServiceImpl()
	{
		this.titanGraph = TitanCassandraEmbeddedConnection.getInstance().getTitanGraphObject();
	}
	
	@Override
	public int getNextAutoIncrement()
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			Iterator<Vertex> iterator = this.titanGraph.getVertices("nodeId", 0).iterator();
			Vertex autoIncrement = null;
			while(iterator.hasNext())
			{
				autoIncrement = iterator.next();
			}
			
			if(autoIncrement == null)
			{
				Vertex vertex = this.titanGraph.addVertexWithLabel(NodeLabels.AutoIncrement.name());
				vertex.setProperty("nodeId", 0);
				vertex.setProperty("next", 2);
				
				titanTransaction.commit();
				return 1;
			}
			else
			{
				int nextIncrement = autoIncrement.getProperty("next");
				autoIncrement.setProperty("next", nextIncrement + 1);
				
				titanTransaction.commit();
				return nextIncrement;
			}
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
