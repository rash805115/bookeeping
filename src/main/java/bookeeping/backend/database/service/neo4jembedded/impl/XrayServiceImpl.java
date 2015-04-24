package bookeeping.backend.database.service.neo4jembedded.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import bookeeping.backend.database.connection.singleton.Neo4JEmbeddedConnection;
import bookeeping.backend.database.service.XrayService;
import bookeeping.backend.exception.NodeNotFound;

public class XrayServiceImpl implements XrayService
{
	private Neo4JEmbeddedConnection neo4jEmbeddedConnection;
	private GraphDatabaseService graphDatabaseService;
	private CommonCode commonCode;
	
	public XrayServiceImpl()
	{
		this.neo4jEmbeddedConnection = Neo4JEmbeddedConnection.getInstance();
		this.graphDatabaseService = this.neo4jEmbeddedConnection.getGraphDatabaseServiceObject();
		this.commonCode = new CommonCode();
	}
	
	@Override
	public List<Map<String, Object>> xrayNode(String nodeId) throws NodeNotFound
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			List<Map<String, Object>> xray = new ArrayList<Map<String, Object>>();
			List<Node> children = this.commonCode.getChildren(nodeId);
			
			for(Node child : children)
			{
				xray.add(this.commonCode.getNodeProperties(child));
			}
			
			transaction.success();
			return xray;
		}
	}
	
	@Override
	public List<Map<String, Object>> xrayVersion(String nodeId) throws NodeNotFound
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			List<Map<String, Object>> versions =  this.commonCode.getNodeVersions(nodeId);
			transaction.success();
			return versions;
		}
		
	}
	
	@Override
	public List<Map<String, Object>> xrayDeleted(String nodeId) throws NodeNotFound
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			List<Map<String, Object>> xray = new ArrayList<Map<String, Object>>();
			List<Node> children = this.commonCode.getDeletedChildren(nodeId);
			
			for(Node child : children)
			{
				xray.add(this.commonCode.getNodeProperties(child));
			}
			
			transaction.success();
			return xray;
		}
	}
}
