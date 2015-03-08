package database.service.neo4jembedded.impl;

import java.util.HashMap;
import java.util.Iterator;
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
import database.neo4j.querybuilder.CypherQueryBuilder;
import database.neo4j.querybuilder.QueryType;
import database.service.DirectoryService;
import exception.DirectoryNotFound;
import exception.DuplicateDirectory;
import exception.FilesystemNotFound;
import exception.UserNotFound;

public class DirectoryServiceImpl implements DirectoryService
{
	private Neo4JEmbeddedConnection neo4jEmbeddedConnection;
	private GraphDatabaseService graphDatabaseService;
	
	public DirectoryServiceImpl()
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
	
	private Node getFilesystem(String userId, String filesystemId) throws FilesystemNotFound, UserNotFound
	{
		Node user  = this.getUser(userId);
		Iterable<Relationship> iterable = user.getRelationships(RelationshipLabels.has);
		for(Relationship relationship : iterable)
		{
			Node filesystem = relationship.getEndNode();
			String retrievedFilesystemId = (String) filesystem.getProperty("filesystemId");
			
			if(retrievedFilesystemId.equals(filesystemId))
			{
				return filesystem;
			}
		}
		
		throw new FilesystemNotFound("ERROR: Filesystem not found! - \"" + filesystemId + "\"");
	}
	
	private Node getRootDirectory(String userId, String filesystemId) throws FilesystemNotFound, UserNotFound
	{
		Node filesystem = this.getFilesystem(userId, filesystemId);
		return filesystem.getRelationships(RelationshipLabels.has).iterator().next().getEndNode();
	}
	
	private Node getDirectory(String userId, String filesystemId, String directoryPath, String directoryName) throws FilesystemNotFound, UserNotFound
	{
		Map<String, Object> directoryProperties = new HashMap<String, Object>();
		directoryProperties.put("directoryPath", directoryPath);
		directoryProperties.put("directoryName", directoryName);
		
		String query = "match (user:User), (filesystem:Filesystem), (node%s), (user)-[:has]->(filesystem) where filesystem.filesystemId=1 and directory.directoryPath="/" and directory.directoryName="abc1" return count(user), count(filesystem), count(directory);"
		Iterator<Map<String, Object>> iterator = this.neo4jEmbeddedConnection.runCypherQuery(new CypherQueryBuilder(QueryType.MATCH_NODE)
													.buildQuery(NodeLabels.Directory, directoryProperties, true), directoryProperties);
		Node directory = null;
		while(iterator.hasNext())
		{
			directory = (Node) iterator.next().get("node");
		}
		
		return directory;
		//match(user:User {userId: userId}) -[:has]- (filesystem: Filesystem {filesystemId: filesystemId}) -[]-
	}
	
	@Override
	public void createNewDirectory(String directoryPath, String directoryName, String filesystemId, String userId, Map<String, Object> directoryProperties) throws UserNotFound, FilesystemNotFound, DuplicateDirectory
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Node rootDirectory = this.getRootDirectory(userId, filesystemId);
			
			Iterable<Relationship> iterable = rootDirectory.getRelationships(RelationshipLabels.has);
			for(Relationship relationship : iterable)
			{
				Node node = relationship.getEndNode();
				if(node.hasLabel(NodeLabels.Directory))
				{
					String retrievedDirectoryPath = (String) node.getProperty("directoryPath");
					String retrievedDirectoryName = (String) node.getProperty("directoryName");
					
					if((retrievedDirectoryPath + retrievedDirectoryName).equals(directoryPath + directoryName))
					{
						transaction.success();
						throw new DuplicateDirectory("ERROR: Directory already present! - \"" + directoryPath + "\"");
					}
				}
			}
			
			Node node = this.graphDatabaseService.createNode(NodeLabels.Directory);
			node.setProperty("nodeId", new AutoIncrementServiceImpl().getNextAutoIncrement());
			node.setProperty("directoryId", directoryId);
			node.setProperty("version", 0);
			
			for(Entry<String, Object> directoryPropertiesEntry : directoryProperties.entrySet())
			{
				node.setProperty(directoryPropertiesEntry.getKey(), directoryPropertiesEntry.getValue());
			}
			
			rootDirectory.createRelationshipTo(node, RelationshipLabels.has);
			
			transaction.success();
		}
	}

	@Override
	public Map<String, Object> getDirectory(String userId, String filesystemId, String directoryId) throws UserNotFound, FilesystemNotFound, DirectoryNotFound
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Node rootDirectory = this.getRootDirectory(userId, filesystemId);
			
			Iterable<Relationship> iterable = rootDirectory.getRelationships(RelationshipLabels.has);
			for(Relationship relationship : iterable)
			{
				Node node = relationship.getEndNode();
				if(node.hasLabel(NodeLabels.Directory))
				{
					String retrievedDirectoryId = (String) node.getProperty("directoryId");
					
					if(retrievedDirectoryId.equals(directoryId))
					{
						Map<String, Object> directoryProperties = new HashMap<String, Object>();
						
						Iterable<String> keys = node.getPropertyKeys();
						for(String key : keys)
						{
							directoryProperties.put(key, node.getProperty(key));
						}
						
						transaction.success();
						return directoryProperties;
					}
				}
			}
			
			transaction.success();
			throw new DirectoryNotFound("ERROR: Directory not found! - \"" + directoryId + "\"");
		}
	}
}
