package bookeeping.backend.database.service.titancassandraembedded.impl;

import java.util.HashMap;
import java.util.Map;

import bookeeping.backend.database.MandatoryProperties;
import bookeeping.backend.database.connection.singleton.TitanCassandraEmbeddedConnection;
import bookeeping.backend.database.service.GenericService;
import bookeeping.backend.exception.NodeNotFound;
import bookeeping.backend.exception.NodeUnavailable;
import bookeeping.backend.exception.VersionNotFound;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.tinkerpop.blueprints.Vertex;

public class GenericServiceImpl implements GenericService
{
	private TitanGraph titanGraph;
	private CommonCode commonCode;
	
	public GenericServiceImpl()
	{
		this.titanGraph = TitanCassandraEmbeddedConnection.getInstance().getTitanGraphObject();
		this.commonCode = new CommonCode();
	}
	
	@Override
	public String createNewVersion(String commitId, String nodeId, Map<String, Object> changeMetadata, Map<String, Object> changedProperties) throws NodeNotFound
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		try
		{
			Vertex versionedNode = this.commonCode.createNodeVersion(commitId, nodeId, changeMetadata, changedProperties);
			String versionedNodeId = (String) versionedNode.getProperty(MandatoryProperties.nodeId.name());
			titanTransaction.commit();
			return versionedNodeId;
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
	public Map<String, Object> getNode(String nodeId) throws NodeNotFound
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		try
		{
			Map<String, Object> nodeProperties = new HashMap<String, Object>();
			Vertex node = this.commonCode.getNode(nodeId);
			for(String key : node.getPropertyKeys())
			{
				nodeProperties.put(key, node.getProperty(key));
			}
			
			titanTransaction.commit();
			return nodeProperties;
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
	public Map<String, Object> getNodeVersion(String nodeId, int version) throws NodeNotFound, VersionNotFound
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		try
		{
			Vertex node = this.commonCode.getNodeVersion(nodeId, version);
			Map<String, Object> properties = new HashMap<String, Object>();
			
			Iterable<String> keys = node.getPropertyKeys();
			for(String key : keys)
			{
				properties.put(key, node.getProperty(key));
			}
			
			titanTransaction.commit();
			return properties;
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
	public void deleteNodeTemporarily(String commitId, String nodeId) throws NodeNotFound, NodeUnavailable
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		try
		{
			this.commonCode.deleteNodeTemporarily(commitId, nodeId);
			titanTransaction.commit();
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
