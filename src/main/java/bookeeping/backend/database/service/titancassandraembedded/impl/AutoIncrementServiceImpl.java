package bookeeping.backend.database.service.titancassandraembedded.impl;

import bookeeping.backend.database.MandatoryProperties;
import bookeeping.backend.database.connection.singleton.TitanCassandraEmbeddedConnection;
import bookeeping.backend.database.service.AutoIncrementService;
import bookeeping.backend.exception.NodeNotFound;
import bookeeping.backend.utilities.AlphaNumericOperation;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.tinkerpop.blueprints.Vertex;

public class AutoIncrementServiceImpl implements AutoIncrementService
{
	private TitanGraph titanGraph;
	private CommonCode commonCode;
	
	public AutoIncrementServiceImpl()
	{
		this.titanGraph = TitanCassandraEmbeddedConnection.getInstance().getTitanGraphObject();
		this.commonCode = new CommonCode();
	}
	
	@Override
	public String getNextAutoIncrement()
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			Vertex autoIncrement = null;
			try
			{
				autoIncrement = this.commonCode.getNode("0");
			}
			catch (NodeNotFound e) {}
			String nextAutoIncrement = (String) autoIncrement.getProperty(MandatoryProperties.next.name());
			autoIncrement.setProperty(MandatoryProperties.next.name(), AlphaNumericOperation.add(nextAutoIncrement, 1));
			
			titanTransaction.commit();
			return nextAutoIncrement;
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
