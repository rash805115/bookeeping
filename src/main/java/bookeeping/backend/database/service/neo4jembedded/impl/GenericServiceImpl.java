package bookeeping.backend.database.service.neo4jembedded.impl;

import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import bookeeping.backend.database.MandatoryProperties;
import bookeeping.backend.database.connection.singleton.Neo4JEmbeddedConnection;
import bookeeping.backend.database.service.GenericService;
import bookeeping.backend.exception.NodeNotFound;
import bookeeping.backend.exception.NodeUnavailable;
import bookeeping.backend.exception.VersionNotFound;

public class GenericServiceImpl implements GenericService
{
	private Neo4JEmbeddedConnection neo4jEmbeddedConnection;
	private GraphDatabaseService graphDatabaseService;
	private CommonCode commonCode;
	
	public GenericServiceImpl()
	{
		this.neo4jEmbeddedConnection = Neo4JEmbeddedConnection.getInstance();
		this.graphDatabaseService = this.neo4jEmbeddedConnection.getGraphDatabaseServiceObject();
		this.commonCode = new CommonCode();
	}
	
	@Override
	public String createNewVersion(String commitId, String nodeId, Map<String, Object> changeMetadata, Map<String, Object> changedProperties) throws NodeNotFound, NodeUnavailable
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Node versionedNode = this.commonCode.createNodeVersion(commitId, nodeId, changeMetadata, changedProperties);
			String versionedNodeId = (String) versionedNode.getProperty(MandatoryProperties.nodeId.name());
			transaction.success();
			return versionedNodeId;
		}
	}
	
	@Override
	public Map<String, Object> getNode(String nodeId) throws NodeNotFound
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Node node = this.commonCode.getNode(nodeId);
			Map<String, Object> nodeProperties = this.commonCode.getNodeProperties(node);
			transaction.success();
			return nodeProperties;
		}
	}
	
	@Override
	public Map<String, Object> getNodeVersion(String nodeId, int version) throws NodeNotFound, VersionNotFound, NodeUnavailable
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Node node = this.commonCode.getNodeVersion(nodeId, version);
			Map<String, Object> properties = this.commonCode.getNodeProperties(node);
			transaction.success();
			return properties;
		}
	}

	@Override
	public void deleteNodeTemporarily(String commitId, String nodeId) throws NodeNotFound, NodeUnavailable
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			this.commonCode.deleteNodeTemporarily(commitId, nodeId);
			transaction.success();
		}
	}

	@Override
	public void changeNodeProperties(String nodeId, Map<String, Object> properties) throws NodeNotFound
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Node node = this.commonCode.getNode(nodeId);
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
			
			transaction.success();
		}
	}
}
