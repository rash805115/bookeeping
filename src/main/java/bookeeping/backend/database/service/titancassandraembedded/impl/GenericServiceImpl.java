package bookeeping.backend.database.service.titancassandraembedded.impl;

import java.util.Map;
import java.util.Map.Entry;

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
	public String createNewVersion(String commitId, String nodeId, Map<String, Object> changeMetadata, Map<String, Object> changedProperties) throws NodeNotFound, NodeUnavailable
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
			Vertex node = this.commonCode.getNode(nodeId);
			Map<String, Object> nodeProperties = this.commonCode.getNodeProperties(node);
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
	public Map<String, Object> getNodeVersion(String nodeId, int version) throws NodeNotFound, VersionNotFound, NodeUnavailable
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		try
		{
			Vertex node = this.commonCode.getNodeVersion(nodeId, version);
			Map<String, Object> properties = this.commonCode.getNodeProperties(node);
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

	@Override
	public void changeNodeProperties(String nodeId, Map<String, Object> properties) throws NodeNotFound
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		try
		{
			Vertex node = this.commonCode.getNode(nodeId);
			for(Entry<String, Object> entry : properties.entrySet())
			{
				String key = entry.getKey();
				boolean found = false;
				for(MandatoryProperties mandatoryProperty : MandatoryProperties.values())
				{
					if(key.equals(mandatoryProperty.name()))
					{
						found = true;
						break;
					}
				}
				
				if(!found)
				{
					node.setProperty(key, entry.getValue());
				}
			}
			
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
