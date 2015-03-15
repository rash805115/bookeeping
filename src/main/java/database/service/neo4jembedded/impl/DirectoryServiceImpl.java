package database.service.neo4jembedded.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import database.MandatoryProperties;
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
	public void createNewDirectory(String commitId, String directoryPath, String directoryName, String filesystemId, String userId, Map<String, Object> directoryProperties) throws UserNotFound, FilesystemNotFound, DuplicateDirectory
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Node rootDirectory = null;
			try
			{
				CommonCode commonCode = new CommonCode();
				rootDirectory = commonCode.getRootDirectory(userId, filesystemId);
				commonCode.getDirectory(userId, filesystemId, directoryPath, directoryName, false);
				throw new DuplicateDirectory("ERROR: Directory already present! - \"" + directoryPath + "/" + directoryName + "\"");
			}
			catch(DirectoryNotFound directoryNotFound)
			{
				Node node = this.graphDatabaseService.createNode(NodeLabels.Directory);
				node.setProperty(MandatoryProperties.nodeId.name(), new AutoIncrementServiceImpl().getNextAutoIncrement());
				node.setProperty(MandatoryProperties.directoryPath.name(), directoryPath);
				node.setProperty(MandatoryProperties.directoryName.name(), directoryName);
				node.setProperty(MandatoryProperties.version.name(), 0);
				
				for(Entry<String, Object> directoryPropertiesEntry : directoryProperties.entrySet())
				{
					node.setProperty(directoryPropertiesEntry.getKey(), directoryPropertiesEntry.getValue());
				}
				
				rootDirectory.createRelationshipTo(node, RelationshipLabels.has).setProperty(MandatoryProperties.commitId.name(), commitId);
				transaction.success();
			}
		}
	}
	
	@Override
	public void createNewVersion(String commitId, String userId, String filesystemId, String directoryPath, String directoryName, Map<String, Object> changeMetadata, Map<String, Object> changedProperties) throws UserNotFound, FilesystemNotFound, DirectoryNotFound
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			CommonCode commonCode = new CommonCode();
			Node directory = null;
			try
			{
				directory = commonCode.getVersion("directory", userId, filesystemId, directoryPath, directoryName, -1, false);
			}
			catch (VersionNotFound | FileNotFound e) {}
			Node versionedDirectory = commonCode.copyNode(directory);
			
			int directoryLatestVersion = (int) directory.getProperty(MandatoryProperties.version.name());
			versionedDirectory.setProperty(MandatoryProperties.version.name(), directoryLatestVersion + 1);
			for(Entry<String, Object> entry : changedProperties.entrySet())
			{
				versionedDirectory.setProperty(entry.getKey(), entry.getValue());
			}
			versionedDirectory.removeProperty(MandatoryProperties.directoryPath.name());
			versionedDirectory.removeProperty(MandatoryProperties.directoryName.name());
			
			Relationship relationship = directory.createRelationshipTo(versionedDirectory, RelationshipLabels.hasVersion);
			for(Entry<String, Object> entry : changeMetadata.entrySet())
			{
				relationship.setProperty(entry.getKey(), entry.getValue());
			}
			relationship.setProperty(MandatoryProperties.commitId.name(), commitId);
			
			transaction.success();
		}
	}
	
	@Override
	public void deleteDirectoryTemporarily(String commitId, String userId, String filesystemId, String directoryPath, String directoryName) throws UserNotFound, FilesystemNotFound, DirectoryNotFound
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Node directory = new CommonCode().getDirectory(userId, filesystemId, directoryPath, directoryName, false);
			Relationship hasRelationship = directory.getSingleRelationship(RelationshipLabels.has, Direction.INCOMING);
			Node parentDirectory = hasRelationship.getStartNode();
			
			Relationship hadRelationship = parentDirectory.createRelationshipTo(directory, RelationshipLabels.had);
			for(String key : hasRelationship.getPropertyKeys())
			{
				hadRelationship.setProperty(key, hasRelationship.getProperty(key));
			}
			hadRelationship.setProperty(MandatoryProperties.commitId.name(), commitId);
			
			hasRelationship.delete();
			transaction.success();
		}
	}
	
	@Override
	public void restoreTemporaryDeletedDirectory(String commitId, String userId, String filesystemId, String directoryPath, String directoryName) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, DuplicateDirectory
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			CommonCode commonCode = new CommonCode();
			try
			{
				commonCode.getDirectory(userId, filesystemId, directoryPath, directoryName, false);
				throw new DuplicateDirectory("ERROR: Directory already present! - \"" + directoryPath + "/" + directoryName + "\"");
			}
			catch(DirectoryNotFound directoryNotFound)
			{
				Node directory = commonCode.getDirectory(userId, filesystemId, directoryPath, directoryName, true);
				Relationship hadRelationship = directory.getSingleRelationship(RelationshipLabels.had, Direction.INCOMING);
				Node parentDirectory = hadRelationship.getStartNode();
				
				Relationship hasRelationship = parentDirectory.createRelationshipTo(directory, RelationshipLabels.has);
				for(String key : hadRelationship.getPropertyKeys())
				{
					hasRelationship.setProperty(key, hadRelationship.getProperty(key));
				}
				hasRelationship.setProperty(MandatoryProperties.commitId.name(), commitId);
				
				hadRelationship.delete();
				transaction.success();
			}
		}
	}
	
	@Override
	public void moveDirectory(String commitId, String userId, String filesystemId, String oldDirectoryPath, String oldDirectoryName, String newDirectoryPath, String newDirectoryName) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, DuplicateDirectory
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Map<String, Object> directoryProperties = null;
			try
			{
				directoryProperties = this.getDirectory(userId, filesystemId, oldDirectoryPath, oldDirectoryName, -1);
				directoryProperties.remove(MandatoryProperties.nodeId.name());
				directoryProperties.remove(MandatoryProperties.directoryPath.name());
				directoryProperties.remove(MandatoryProperties.directoryName.name());
				directoryProperties.remove(MandatoryProperties.version.name());
			}
			catch (VersionNotFound e) {}
			
			this.deleteDirectoryTemporarily(commitId, userId, filesystemId, oldDirectoryPath, oldDirectoryName);
			this.createNewDirectory(commitId, newDirectoryPath, newDirectoryName, filesystemId, userId, directoryProperties);
			
			CommonCode commonCode = new CommonCode();
			Node oldDirectory = commonCode.getDirectory(userId, filesystemId, oldDirectoryPath, oldDirectoryName, true);
			Node newDirectory = commonCode.getDirectory(userId, filesystemId, newDirectoryPath, newDirectoryName, false);
			for(Relationship oldRelationship : oldDirectory.getRelationships(Direction.OUTGOING))
			{
				if(oldRelationship.isType(RelationshipLabels.has) || oldRelationship.isType(RelationshipLabels.had))
				{
					Node endNode = oldRelationship.getEndNode();
					endNode.setProperty(MandatoryProperties.filePath.name(), newDirectoryPath + "/" + newDirectoryName);
					Relationship newRelationship = newDirectory.createRelationshipTo(endNode, oldRelationship.getType());
					
					for(String key : oldRelationship.getPropertyKeys())
					{
						newRelationship.setProperty(key, oldRelationship.getProperty(key));
					}
					
					oldRelationship.delete();
				}
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
				directory = new CommonCode().getVersion("directory", userId, filesystemId, directoryPath, directoryName, version, false);
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
