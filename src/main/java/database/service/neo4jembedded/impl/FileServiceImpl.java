package database.service.neo4jembedded.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import database.connection.singleton.Neo4JEmbeddedConnection;
import database.neo4j.NodeLabels;
import database.neo4j.RelationshipLabels;
import database.service.FileService;
import exception.DirectoryNotFound;
import exception.DuplicateFile;
import exception.FileNotFound;
import exception.FilesystemNotFound;
import exception.UserNotFound;
import exception.VersionNotFound;

public class FileServiceImpl implements FileService
{
	private Neo4JEmbeddedConnection neo4jEmbeddedConnection;
	private GraphDatabaseService graphDatabaseService;
	
	public FileServiceImpl()
	{
		this.neo4jEmbeddedConnection = Neo4JEmbeddedConnection.getInstance();
		this.graphDatabaseService = this.neo4jEmbeddedConnection.getGraphDatabaseServiceObject();
	}

	@Override
	public void createNewFile(String filePath, String fileName, String filesystemId, String userId, Map<String, Object> fileProperties) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, DuplicateFile
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Node parentDirectory = null;
			try
			{
				CommonCode commonCode = new CommonCode();
				if(filePath.equals("/"))
				{
					parentDirectory = commonCode.getRootDirectory(userId, filesystemId);
				}
				else
				{
					String directoryName = filePath.substring(filePath.lastIndexOf("/") + 1, filePath.length());
					String directoryPath = filePath.substring(0, filePath.lastIndexOf("/" + directoryName));
					directoryPath = directoryPath.length() == 0 ? "/" : directoryPath;
					parentDirectory = commonCode.getDirectory(userId, filesystemId, directoryPath, directoryName);
				}
				
				commonCode.getFile(userId, filesystemId, filePath, fileName);
				throw new DuplicateFile("ERROR: File already present! - \"" + filePath + "/" + fileName + "\"");
			}
			catch(FileNotFound fileNotFound)
			{
				Node node = this.graphDatabaseService.createNode(NodeLabels.File);
				node.setProperty("nodeId", new AutoIncrementServiceImpl().getNextAutoIncrement());
				node.setProperty("filePath", filePath);
				node.setProperty("fileName", fileName);
				node.setProperty("version", 0);
				
				for(Entry<String, Object> filePropertiesEntry : fileProperties.entrySet())
				{
					node.setProperty(filePropertiesEntry.getKey(), filePropertiesEntry.getValue());
				}
				
				parentDirectory.createRelationshipTo(node, RelationshipLabels.has);
				transaction.success();
			}
		}
	}
	
	@Override
	public void createNewVersion(String userId, String filesystemId, String filePath, String fileName, Map<String, Object> changeMetadata, Map<String, Object> changedProperties) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			CommonCode commonCode = new CommonCode();
			Node file = null;
			try
			{
				file = commonCode.getVersion("file", userId, filesystemId, filePath, fileName, -1);
			}
			catch (VersionNotFound e) {}
			Node versionedFile = commonCode.copyNode(file);
			
			int fileLatestVersion = (int) file.getProperty("version");
			versionedFile.setProperty("nodeId", new AutoIncrementServiceImpl().getNextAutoIncrement());
			versionedFile.setProperty("version", fileLatestVersion + 1);
			for(Entry<String, Object> entry : changedProperties.entrySet())
			{
				versionedFile.setProperty(entry.getKey(), entry.getValue());
			}
			
			Relationship relationship = file.createRelationshipTo(versionedFile, RelationshipLabels.hasVersion);
			for(Entry<String, Object> entry : changeMetadata.entrySet())
			{
				relationship.setProperty(entry.getKey(), entry.getValue());
			}
			
			transaction.success();
		}
	}
	
	@Override
	public void deleteFileTemporarily(String userId, String filesystemId, String filePath, String fileName) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Node file = new CommonCode().getFile(userId, filesystemId, filePath, fileName);
			Relationship hasRelationship = file.getSingleRelationship(RelationshipLabels.has, Direction.INCOMING);
			Node parentDirectory = hasRelationship.getStartNode();
			
			Relationship hadRelationship = parentDirectory.createRelationshipTo(file, RelationshipLabels.had);
			for(String key : hasRelationship.getPropertyKeys())
			{
				hadRelationship.setProperty(key, hasRelationship.getProperty(key));
			}
			
			hasRelationship.delete();
			transaction.success();
		}
	}

	@Override
	public Map<String, Object> getFile(String userId, String filesystemId, String filePath, String fileName, int version) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound, VersionNotFound
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Node file = new CommonCode().getVersion("file", userId, filesystemId, filePath, fileName, version);
			Map<String, Object> fileProperties = new HashMap<String, Object>();
			
			Iterable<String> keys = file.getPropertyKeys();
			for(String key : keys)
			{
				fileProperties.put(key, file.getProperty(key));
			}
			
			transaction.success();
			return fileProperties;
		}
	}
}
