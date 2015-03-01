package database.service.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.ReadableIndex;

import database.connection.singleton.Neo4JRestConnection;
import database.service.DatabaseService;
import exception.DuplicateUser;
import exception.UserNotFound;

public class Neo4JRestServiceImpl implements DatabaseService
{
	private enum Labels implements Label
	{
		AutoIncrement, User
	}
	
	private GraphDatabaseService graphDatabaseService;
	
	public Neo4JRestServiceImpl()
	{
		this.graphDatabaseService = Neo4JRestConnection.getInstance().getGraphDatabaseServiceObject();
	}

	@Override
	public int getNextAutoIncrement()
	{
		Node autoIncrement = null;
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			ReadableIndex<Node> readableIndex = this.graphDatabaseService.index().getNodeAutoIndexer().getAutoIndex();
			autoIncrement = readableIndex.get("nodeId", 0).getSingle();
			
			if(autoIncrement == null)
			{
				Node node = this.graphDatabaseService.createNode(Labels.AutoIncrement);
				node.setProperty("nodeId", 0);
				node.setProperty("next", 2);
				
				transaction.success();
				return 1;
			}
			else
			{
				int nextAutoIncrement = (int) autoIncrement.getProperty("next");
				autoIncrement.setProperty("next", nextAutoIncrement + 1);
				
				transaction.success();
				return nextAutoIncrement;
			}
		}
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
				Node node = this.graphDatabaseService.createNode(Labels.User);
				node.setProperty("nodeId", this.getNextAutoIncrement());
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
}
