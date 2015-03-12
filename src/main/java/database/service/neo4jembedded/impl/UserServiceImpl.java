package database.service.neo4jembedded.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import database.MandatoryProperties;
import database.connection.singleton.Neo4JEmbeddedConnection;
import database.neo4j.NodeLabels;
import database.neo4j.querybuilder.CypherQueryBuilder;
import database.neo4j.querybuilder.QueryType;
import database.service.UserService;
import exception.DuplicateUser;
import exception.UserNotFound;

public class UserServiceImpl implements UserService
{
	private Neo4JEmbeddedConnection neo4jEmbeddedConnection;
	private GraphDatabaseService graphDatabaseService;
	
	public UserServiceImpl()
	{
		this.neo4jEmbeddedConnection = Neo4JEmbeddedConnection.getInstance();
		this.graphDatabaseService = this.neo4jEmbeddedConnection.getGraphDatabaseServiceObject();
	}
	
	@Override
	public void createNewUser(String userId, Map<String, Object> userProperties) throws DuplicateUser
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			try
			{
				new CommonCode().getUser(userId);
				throw new DuplicateUser("ERROR: User already present! - \"" + userId + "\"");
			}
			catch(UserNotFound userNotFound)
			{
				Node node = this.graphDatabaseService.createNode(NodeLabels.User);
				node.setProperty(MandatoryProperties.nodeId.name(), new AutoIncrementServiceImpl().getNextAutoIncrement());
				node.setProperty(MandatoryProperties.userId.name(), userId);
				
				for(Entry<String, Object> userPropertiesEntry : userProperties.entrySet())
				{
					node.setProperty(userPropertiesEntry.getKey(), userPropertiesEntry.getValue());
				}
				
				transaction.success();
			}
		}
	}

	@Override
	public int countUsers()
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Iterator<Map<String, Object>> iterator = this.neo4jEmbeddedConnection.runCypherQuery(new CypherQueryBuilder(QueryType.MATCH_COUNT).buildQuery(NodeLabels.User, new HashMap<String, Object>(), true), new HashMap<String, Object>());
			int count = (int) iterator.next().get("count");
			transaction.success();
			return count;
		}
	}
	
	@Override
	public Map<String, Object> getUser(String userId) throws UserNotFound
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Node user = new CommonCode().getUser(userId);
			Map<String, Object> userProperties = new HashMap<String, Object>();
			
			Iterable<String> iterable = user.getPropertyKeys();
			for(String key : iterable)
			{
				userProperties.put(key, user.getProperty(key));
			}
			
			transaction.success();
			return userProperties;
		}
	}

	@Override
	public List<Map<String, Object>> getUsersByMatchingAllProperty(Map<String, Object> userProperties)
	{
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Iterator<Map<String, Object>> iterator = this.neo4jEmbeddedConnection.runCypherQuery(new CypherQueryBuilder(QueryType.MATCH_NODE)
														.buildQuery(NodeLabels.User, userProperties, true), userProperties);
			while(iterator.hasNext())
			{
				Node user = (Node) iterator.next().get("node");
				Map<String, Object> map = new HashMap<String, Object>();
				
				for(String key : user.getPropertyKeys())
				{
					map.put(key, user.getProperty(key));
				}
				
				list.add(map);
			}
			
			transaction.success();
		}
		
		return list;
	}

	@Override
	public List<Map<String, Object>> getUsersByMatchingAnyProperty(Map<String, Object> userProperties)
	{
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Iterator<Map<String, Object>> iterator = this.neo4jEmbeddedConnection.runCypherQuery(new CypherQueryBuilder(QueryType.MATCH_NODE)
														.buildQuery(NodeLabels.User, userProperties, false), userProperties);
			while(iterator.hasNext())
			{
				Node user = (Node) iterator.next().get("node");
				Map<String, Object> map = new HashMap<String, Object>();
				
				for(String key : user.getPropertyKeys())
				{
					map.put(key, user.getProperty(key));
				}
				
				list.add(map);
			}
			
			transaction.success();
		}
		
		return list;
	}
}
