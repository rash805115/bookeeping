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
import org.neo4j.graphdb.index.ReadableIndex;

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
		Node user = null;
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			ReadableIndex<Node> readableIndex = this.graphDatabaseService.index().getNodeAutoIndexer().getAutoIndex();
			user = readableIndex.get("userId", userId).getSingle();
			
			if(user != null)
			{
				transaction.success();
				throw new DuplicateUser("ERROR: User already present! - \"" + userId + "\"");
			}
			else
			{
				Node node = this.graphDatabaseService.createNode(NodeLabels.User);
				node.setProperty("nodeId", new AutoIncrementServiceImpl().getNextAutoIncrement());
				node.setProperty("userId", userId);
				
				for(Entry<String, Object> userPropertiesEntry : userProperties.entrySet())
				{
					node.setProperty(userPropertiesEntry.getKey(), userPropertiesEntry.getValue());
				}
				
				transaction.success();
			}
		}
	}

	@Override
	public long countUsers()
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Iterator<Map<String, Object>> iterator = this.neo4jEmbeddedConnection.runCypherQuery(new CypherQueryBuilder(QueryType.MATCH_COUNT).buildQuery(NodeLabels.User, new HashMap<String, Object>(), true), new HashMap<String, Object>());
			long count = (long) iterator.next().get("count");
			transaction.success();
			return count;
		}
	}

	@Override
	public void removeUser(String userId) throws UserNotFound
	{
		Node user = null;
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			ReadableIndex<Node> readableIndex = this.graphDatabaseService.index().getNodeAutoIndexer().getAutoIndex();
			user = readableIndex.get("userId", userId).getSingle();
			
			if(user == null)
			{
				transaction.success();
				throw new UserNotFound("ERROR: User not found! - \"" + userId + "\"");
			}
			else
			{
				user.delete();
				transaction.success();
			}
		}
	}
	
	@Override
	public Map<String, Object> getUser(String userId) throws UserNotFound
	{
		Node user = null;
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			ReadableIndex<Node> readableIndex = this.graphDatabaseService.index().getNodeAutoIndexer().getAutoIndex();
			user = readableIndex.get("userId", userId).getSingle();
			
			if(user == null)
			{
				transaction.success();
				throw new UserNotFound("ERROR: User not found! - \"" + userId + "\"");
			}
			else
			{
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
	}

	@Override
	public List<Map<String, Object>> getUsersByMatchingAllProperty(Map<String, Object> userProperties) throws UserNotFound
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
	public List<Map<String, Object>> getUsersByMatchingAnyProperty(Map<String, Object> userProperties) throws UserNotFound
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
