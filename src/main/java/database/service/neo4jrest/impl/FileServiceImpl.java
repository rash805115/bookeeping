package database.service.neo4jrest.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import database.MandatoryProperties;
import database.connection.singleton.Neo4JRestConnection;
import database.neo4j.NodeLabels;
import database.neo4j.RelationshipLabels;
import database.service.FileService;
import exception.DirectoryNotFound;
import exception.DuplicateFile;
import exception.FileNotFound;
import exception.FilesystemNotFound;
import exception.UserNotFound;
import exception.VersionNotFound;
import file.FilePermission;

public class FileServiceImpl implements FileService
{
	private Neo4JRestConnection neo4jRestConnection;
	private GraphDatabaseService graphDatabaseService;
	
	public FileServiceImpl()
	{
		this.neo4jRestConnection = Neo4JRestConnection.getInstance();
		this.graphDatabaseService = this.neo4jRestConnection.getGraphDatabaseServiceObject();
	}

	@Override
	public void createNewFile(String commitId, String filePath, String fileName, String filesystemId, String userId, Map<String, Object> fileProperties) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, DuplicateFile
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
					parentDirectory = commonCode.getDirectory(userId, filesystemId, directoryPath, directoryName, false);
				}
				
				commonCode.getFile(userId, filesystemId, filePath, fileName, false);
				throw new DuplicateFile("ERROR: File already present! - \"" + filePath + "/" + fileName + "\"");
			}
			catch(FileNotFound fileNotFound)
			{
				Node node = this.graphDatabaseService.createNode(NodeLabels.File);
				node.setProperty(MandatoryProperties.nodeId.name(), new AutoIncrementServiceImpl().getNextAutoIncrement());
				node.setProperty(MandatoryProperties.filePath.name(), filePath);
				node.setProperty(MandatoryProperties.fileName.name(), fileName);
				node.setProperty(MandatoryProperties.version.name(), 0);
				
				for(Entry<String, Object> filePropertiesEntry : fileProperties.entrySet())
				{
					node.setProperty(filePropertiesEntry.getKey(), filePropertiesEntry.getValue());
				}
				
				parentDirectory.createRelationshipTo(node, RelationshipLabels.has).setProperty(MandatoryProperties.commitId.name(), commitId);
				transaction.success();
			}
		}
	}
	
	@Override
	public void createNewVersion(String commitId, String userId, String filesystemId, String filePath, String fileName, Map<String, Object> changeMetadata, Map<String, Object> changedProperties) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			CommonCode commonCode = new CommonCode();
			Node file = null;
			try
			{
				file = commonCode.getVersion("file", userId, filesystemId, filePath, fileName, -1, false);
			}
			catch (VersionNotFound e) {}
			Node versionedFile = commonCode.copyNode(file);
			
			int fileLatestVersion = (int) file.getProperty(MandatoryProperties.version.name());
			versionedFile.setProperty(MandatoryProperties.version.name(), fileLatestVersion + 1);
			for(Entry<String, Object> entry : changedProperties.entrySet())
			{
				versionedFile.setProperty(entry.getKey(), entry.getValue());
			}
			
			Relationship relationship = file.createRelationshipTo(versionedFile, RelationshipLabels.hasVersion);
			for(Entry<String, Object> entry : changeMetadata.entrySet())
			{
				relationship.setProperty(entry.getKey(), entry.getValue());
			}
			relationship.setProperty(MandatoryProperties.commitId.name(), commitId);
			
			transaction.success();
		}
	}
	
	@Override
	public void shareFile(String commitId, String userId, String filesystemId, String filePath, String fileName, String shareWithUserId, FilePermission filePermission) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			CommonCode commonCode = new CommonCode();
			Node beneficiaryUser = commonCode.getUser(shareWithUserId);
			Node fileToBeShared = commonCode.getFile(userId, filesystemId, filePath, fileName, false);
			
			for(Relationship relationship : beneficiaryUser.getRelationships(Direction.OUTGOING, RelationshipLabels.hasAccess))
			{
				if(relationship.getEndNode().getProperty(MandatoryProperties.nodeId.name()).equals(fileToBeShared.getProperty(MandatoryProperties.nodeId.name())))
				{
					relationship.delete();
					break;
				}
			}
			
			Relationship newRelationship = beneficiaryUser.createRelationshipTo(fileToBeShared, RelationshipLabels.hasAccess);
			newRelationship.setProperty(MandatoryProperties.permission.name(), filePermission.name());
			newRelationship.setProperty(MandatoryProperties.commitId.name(), commitId);
			
			transaction.success();
		}
	}
	
	@Override
	public void unshareFile(String commitId, String userId, String filesystemId, String filePath, String fileName, String unshareWithUserId) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			CommonCode commonCode = new CommonCode();
			Node beneficiaryUser = commonCode.getUser(unshareWithUserId);
			Node fileToBeShared = commonCode.getFile(userId, filesystemId, filePath, fileName, false);
			
			Relationship hadAccessRelationship = beneficiaryUser.createRelationshipTo(fileToBeShared, RelationshipLabels.hadAccess);
			for(Relationship hasAccessRelationship : beneficiaryUser.getRelationships(Direction.OUTGOING, RelationshipLabels.hasAccess))
			{
				if(hasAccessRelationship.getEndNode().getProperty(MandatoryProperties.nodeId.name()).equals(fileToBeShared.getProperty(MandatoryProperties.nodeId.name())))
				{
					for(String key : hasAccessRelationship.getPropertyKeys())
					{
						hadAccessRelationship.setProperty(key, hasAccessRelationship.getProperty(key));
					}
					hadAccessRelationship.setProperty(MandatoryProperties.commitId.name(), commitId);
					
					hasAccessRelationship.delete();
					break;
				}
			}
			
			transaction.success();
		}
	}
	
	@Override
	public void deleteFileTemporarily(String commitId, String userId, String filesystemId, String filePath, String fileName) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Node file = new CommonCode().getFile(userId, filesystemId, filePath, fileName, false);
			Relationship hasRelationship = file.getSingleRelationship(RelationshipLabels.has, Direction.INCOMING);
			Node parentDirectory = hasRelationship.getStartNode();
			
			Relationship hadRelationship = parentDirectory.createRelationshipTo(file, RelationshipLabels.had);
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
	public void restoreTemporaryDeletedFile(String commitId, String userId, String filesystemId, String filePath, String fileName) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound, DuplicateFile
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			CommonCode commonCode = new CommonCode();
			try
			{
				commonCode.getFile(userId, filesystemId, filePath, fileName, false);
				throw new DuplicateFile("ERROR: File already present! - \"" + filePath + "/" + fileName + "\"");
			}
			catch(FileNotFound fileNotFound)
			{
				Node file = commonCode.getFile(userId, filesystemId, filePath, fileName, true);
				Relationship hadRelationship = file.getSingleRelationship(RelationshipLabels.had, Direction.INCOMING);
				Node parentDirectory = hadRelationship.getStartNode();
				
				Relationship hasRelationship = parentDirectory.createRelationshipTo(file, RelationshipLabels.has);
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
	public void moveFile(String commitId, String userId, String filesystemId, String oldFilePath, String oldFileName, String newFilePath, String newFileName) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound, DuplicateFile
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Map<String, Object> fileProperties = null;
			try
			{
				fileProperties = this.getFile(userId, filesystemId, oldFilePath, oldFileName, -1);
				fileProperties.remove(MandatoryProperties.nodeId.name());
				fileProperties.remove(MandatoryProperties.filePath.name());
				fileProperties.remove(MandatoryProperties.fileName.name());
				fileProperties.remove(MandatoryProperties.version.name());
			}
			catch (VersionNotFound e) {}
			
			this.deleteFileTemporarily(commitId, userId, filesystemId, oldFilePath, oldFileName);
			this.createNewFile(commitId, newFilePath, newFileName, filesystemId, userId, fileProperties);
			transaction.success();
		}
	}

	@Override
	public Map<String, Object> getFile(String userId, String filesystemId, String filePath, String fileName, int version) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound, VersionNotFound
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Node file = new CommonCode().getVersion("file", userId, filesystemId, filePath, fileName, version, false);
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
