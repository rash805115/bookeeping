package database.service.neo4jembedded.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.ReadableIndex;

import database.connection.singleton.Neo4JEmbeddedConnection;
import database.neo4j.NodeLabels;
import database.neo4j.RelationshipLabels;
import database.service.FilesystemService;
import exception.DuplicateFilesystem;
import exception.FilesystemNotFound;
import exception.UserNotFound;

public class FilesystemServiceImpl implements FilesystemService
{
	private Neo4JEmbeddedConnection neo4jEmbeddedConnection;
	private GraphDatabaseService graphDatabaseService;
	
	public FilesystemServiceImpl()
	{
		this.neo4jEmbeddedConnection = Neo4JEmbeddedConnection.getInstance();
		this.graphDatabaseService = this.neo4jEmbeddedConnection.getGraphDatabaseServiceObject();
	}
	
	private Node getUser(String userId) throws UserNotFound
	{
		ReadableIndex<Node> readableIndex = this.graphDatabaseService.index().getNodeAutoIndexer().getAutoIndex();
		Node user = readableIndex.get("userId", userId).getSingle();
		
		if(user == null)
		{
			throw new UserNotFound("ERROR: User not found! - \"" + userId + "\"");
		}
		else
		{
			return user;
		}
	}
	
	@Override
	public void createNewFilesystem(String filesystemId, String userId, Map<String, Object> filesystemProperties) throws UserNotFound, DuplicateFilesystem
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Node user = this.getUser(userId);
			Iterable<Relationship> iterable = user.getRelationships(RelationshipLabels.has);
			for(Relationship relationship : iterable)
			{
				Node filesystem = relationship.getEndNode();
				String retrievedFilesystemId = (String) filesystem.getProperty("filesystemId");
				
				if(retrievedFilesystemId.equals(filesystemId))
				{
					transaction.success();
					throw new DuplicateFilesystem("ERROR: Filesystem already present! - \"" + filesystemId + "\"");
				}
			}
			
			Node node = this.graphDatabaseService.createNode(NodeLabels.Filesystem);
			node.setProperty("nodeId", new AutoIncrementServiceImpl().getNextAutoIncrement());
			node.setProperty("filesystemId", filesystemId);
			node.setProperty("version", 0);
			
			for(Entry<String, Object> filesystemPropertiesEntry : filesystemProperties.entrySet())
			{
				node.setProperty(filesystemPropertiesEntry.getKey(), filesystemPropertiesEntry.getValue());
			}
			
			user.createRelationshipTo(node, RelationshipLabels.has);
			
			transaction.success();
		}
	}

	@Override
	public void removeFilesystem(String userId, String filesystemId) throws UserNotFound, FilesystemNotFound
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Node user = this.getUser(userId);
			Iterable<Relationship> iterable = user.getRelationships(RelationshipLabels.has);
			for(Relationship relationship : iterable)
			{
				Node filesystem = relationship.getEndNode();
				String retrievedFilesystemId = (String) filesystem.getProperty("filesystemId");
				
				if(retrievedFilesystemId.equals(filesystemId))
				{
					relationship.delete();
					filesystem.delete();
					transaction.success();
					return;
				}
			}
			
			transaction.success();
			throw new FilesystemNotFound("ERROR: Filesystem not found! - \"" + filesystemId + "\"");
		}
	}

	@Override
	public Map<String, Object> getFilesystem(String userId, String filesystemId) throws UserNotFound, FilesystemNotFound
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Node user = this.getUser(userId);
			Iterable<Relationship> iterable = user.getRelationships(RelationshipLabels.has);
			for(Relationship relationship : iterable)
			{
				Node filesystem = relationship.getEndNode();
				String retrievedFilesystemId = (String) filesystem.getProperty("filesystemId");
				
				if(retrievedFilesystemId.equals(filesystemId))
				{
					Map<String, Object> filesystemProperties = new HashMap<String, Object>();
					
					Iterable<String> keys = filesystem.getPropertyKeys();
					for(String key : keys)
					{
						filesystemProperties.put(key, filesystem.getProperty(key));
					}
					
					transaction.success();
					return filesystemProperties;
				}
			}
			
			transaction.success();
			throw new FilesystemNotFound("ERROR: Filesystem not found! - \"" + filesystemId + "\"");
		}
	}
}
