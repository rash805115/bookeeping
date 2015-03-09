package database.service.neo4jembedded.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import database.connection.singleton.Neo4JEmbeddedConnection;
import database.neo4j.NodeLabels;
import database.neo4j.RelationshipLabels;
import database.service.DirectoryService;
import exception.DirectoryNotFound;
import exception.DuplicateDirectory;
import exception.FileNotFound;
import exception.FilesystemNotFound;
import exception.UserNotFound;
import exception.VersionNotFound;

public class DirectoryServiceImpl implements DirectoryService
{
	private Neo4JEmbeddedConnection neo4jEmbeddedConnection;
	private GraphDatabaseService graphDatabaseService;
	
	public DirectoryServiceImpl()
	{
		this.neo4jEmbeddedConnection = Neo4JEmbeddedConnection.getInstance();
		this.graphDatabaseService = this.neo4jEmbeddedConnection.getGraphDatabaseServiceObject();
	}
	
	@Override
	public void createNewDirectory(String directoryPath, String directoryName, String filesystemId, String userId, Map<String, Object> directoryProperties) throws UserNotFound, FilesystemNotFound, DuplicateDirectory
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Node rootDirectory = null;
			try
			{
				CommonCode commonCode = new CommonCode();
				rootDirectory = commonCode.getRootDirectory(userId, filesystemId);
				commonCode.getDirectory(userId, filesystemId, directoryPath, directoryName);
				throw new DuplicateDirectory("ERROR: Directory already present! - \"" + directoryPath + "/" + directoryName + "\"");
			}
			catch(DirectoryNotFound directoryNotFound)
			{
				Node node = this.graphDatabaseService.createNode(NodeLabels.Directory);
				node.setProperty("nodeId", new AutoIncrementServiceImpl().getNextAutoIncrement());
				node.setProperty("directoryPath", directoryPath);
				node.setProperty("directoryName", directoryName);
				node.setProperty("version", 0);
				
				for(Entry<String, Object> directoryPropertiesEntry : directoryProperties.entrySet())
				{
					node.setProperty(directoryPropertiesEntry.getKey(), directoryPropertiesEntry.getValue());
				}
				
				rootDirectory.createRelationshipTo(node, RelationshipLabels.has);
				transaction.success();
			}
		}
	}
	
	@Override
	public void createNewVersion(String userId, String filesystemId, String directoryPath, String directoryName, Map<String, Object> changeMetadata, Map<String, Object> changedProperties) throws UserNotFound, FilesystemNotFound, DirectoryNotFound
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			CommonCode commonCode = new CommonCode();
			Node directory = null;
			try
			{
				directory = commonCode.getVersion("directory", userId, filesystemId, directoryPath, directoryName, -1);
			}
			catch (VersionNotFound | FileNotFound e) {}
			Node versionedDirectory = commonCode.copyNode(directory);
			
			int directoryLatestVersion = (int) directory.getProperty("version");
			versionedDirectory.setProperty("nodeId", new AutoIncrementServiceImpl().getNextAutoIncrement());
			versionedDirectory.setProperty("version", directoryLatestVersion + 1);
			for(Entry<String, Object> entry : changedProperties.entrySet())
			{
				versionedDirectory.setProperty(entry.getKey(), entry.getValue());
			}
			
			Relationship relationship = directory.createRelationshipTo(versionedDirectory, RelationshipLabels.hasVersion);
			for(Entry<String, Object> entry : changeMetadata.entrySet())
			{
				relationship.setProperty(entry.getKey(), entry.getValue());
			}
			
			transaction.success();
		}
	}
	
	@Override
	public Map<String, Object> getDirectory(String userId, String filesystemId, String directoryPath, String directoryName, int version) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, VersionNotFound
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Node directory = null;
			try
			{
				directory = new CommonCode().getVersion("directory", userId, filesystemId, directoryPath, directoryName, version);
			}
			catch (FileNotFound e) {}
			Map<String, Object> directoryProperties = new HashMap<String, Object>();
			
			Iterable<String> keys = directory.getPropertyKeys();
			for(String key : keys)
			{
				directoryProperties.put(key, directory.getProperty(key));
			}
			
			transaction.success();
			return directoryProperties;
		}
	}
}
