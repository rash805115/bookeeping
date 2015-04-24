package bookeeping.backend.database.service.titancassandraembedded.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import bookeeping.backend.database.connection.singleton.TitanCassandraEmbeddedConnection;
import bookeeping.backend.database.service.XrayService;
import bookeeping.backend.exception.NodeNotFound;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.tinkerpop.blueprints.Vertex;

public class XrayServiceImpl implements XrayService
{
	private TitanGraph titanGraph;
	private CommonCode commonCode;
	
	public XrayServiceImpl()
	{
		this.titanGraph = TitanCassandraEmbeddedConnection.getInstance().getTitanGraphObject();
		this.commonCode = new CommonCode();
	}
	
	@Override
	public List<Map<String, Object>> xrayNode(String nodeId) throws NodeNotFound
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		try
		{
			List<Map<String, Object>> xray = new ArrayList<Map<String, Object>>();
			List<Vertex> children = this.commonCode.getChildren(nodeId);
			
			for(Vertex child : children)
			{
				xray.add(this.commonCode.getNodeProperties(child));
			}
			
			titanTransaction.commit();
			return xray;
		}
		finally
		{
			if(titanTransaction.isOpen())
			{
				titanTransaction.rollback();
			}
		}
	}
	
	@Override
	public List<Map<String, Object>> xrayVersion(String nodeId) throws NodeNotFound
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		try
		{
			List<Map<String, Object>> versions =  this.commonCode.getNodeVersions(nodeId);
			titanTransaction.commit();
			return versions;
		}
		finally
		{
			if(titanTransaction.isOpen())
			{
				titanTransaction.rollback();
			}
		}
		
	}
	
	@Override
	public List<Map<String, Object>> xrayDeleted(String nodeId) throws NodeNotFound
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		try
		{
			List<Map<String, Object>> xray = new ArrayList<Map<String, Object>>();
			List<Vertex> children = this.commonCode.getDeletedChildren(nodeId);
			
			for(Vertex child : children)
			{
				xray.add(this.commonCode.getNodeProperties(child));
			}
			
			titanTransaction.commit();
			return xray;
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
