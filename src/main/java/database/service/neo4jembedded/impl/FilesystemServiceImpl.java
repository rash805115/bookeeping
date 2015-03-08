package database.service.neo4jembedded.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

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
	
	@Override
	public void createNewFilesystem(String filesystemId, String userId, Map<String, Object> filesystemProperties) throws UserNotFound, DuplicateFilesystem
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Node user = null;
			try
			{
				CommonCode commonCode = new CommonCode();
				user = commonCode.getUser(userId);
				commonCode.getFilesystem(userId, filesystemId);
				throw new DuplicateFilesystem("ERROR: Filesystem already present! - \"" + filesystemId + "\"");
			}
			catch(FilesystemNotFound filesystemNotFound)
			{
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
	}

	@Override
	public Map<String, Object> getFilesystem(String userId, String filesystemId) throws UserNotFound, FilesystemNotFound
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Node filesystem = new CommonCode().getFilesystem(userId, filesystemId);
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
}
