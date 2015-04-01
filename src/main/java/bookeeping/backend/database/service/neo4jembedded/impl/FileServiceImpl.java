package bookeeping.backend.database.service.neo4jembedded.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import bookeeping.backend.database.MandatoryProperties;
import bookeeping.backend.database.connection.singleton.Neo4JEmbeddedConnection;
import bookeeping.backend.database.neo4j.NodeLabels;
import bookeeping.backend.database.neo4j.RelationshipLabels;
import bookeeping.backend.database.service.FileService;
import bookeeping.backend.database.service.GenericService;
import bookeeping.backend.exception.DirectoryNotFound;
import bookeeping.backend.exception.DuplicateFile;
import bookeeping.backend.exception.FileNotFound;
import bookeeping.backend.exception.FilesystemNotFound;
import bookeeping.backend.exception.NodeNotFound;
import bookeeping.backend.exception.NodeUnavailable;
import bookeeping.backend.exception.UserNotFound;
import bookeeping.backend.exception.VersionNotFound;

public class FileServiceImpl implements FileService
{
	private Neo4JEmbeddedConnection neo4jEmbeddedConnection;
	private GraphDatabaseService graphDatabaseService;
	private CommonCode commonCode;
	
	public FileServiceImpl()
	{
		this.neo4jEmbeddedConnection = Neo4JEmbeddedConnection.getInstance();
		this.graphDatabaseService = this.neo4jEmbeddedConnection.getGraphDatabaseServiceObject();
		this.commonCode = new CommonCode();
	}

	@Override
	public String createNewFile(String commitId, String userId, String filesystemId, int filesystemVersion, String filePath, String fileName, Map<String, Object> fileProperties) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, VersionNotFound, DuplicateFile
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			try
			{
				this.commonCode.getFile(userId, filesystemId, filesystemVersion, filePath, fileName);
				throw new DuplicateFile("ERROR: File already present! - \"" + (filePath.equals("/") ? "" : filePath) + "/" + fileName + "\"");
			}
			catch(FileNotFound fileNotFound)
			{
				String directoryName = filePath.substring(filePath.lastIndexOf("/") + 1, filePath.length());
				String directoryPath = filePath.substring(0, filePath.lastIndexOf("/" + directoryName));
				
				Node parentDirectory = null;
				if(directoryPath.length() == 0)
				{
					parentDirectory = this.commonCode.getRootDirectory(userId, filesystemId, filesystemVersion);
				}
				else
				{
					parentDirectory = this.commonCode.getDirectory(userId, filesystemId, filesystemVersion, directoryPath, directoryName);
				}
				
				Node file = this.commonCode.createNode(NodeLabels.File);
				file.setProperty(MandatoryProperties.filePath.name(), filePath);
				file.setProperty(MandatoryProperties.fileName.name(), fileName);
				file.setProperty(MandatoryProperties.version.name(), 0);
				String fileNodeId = (String) file.getProperty(MandatoryProperties.nodeId.name());
				
				for(Entry<String, Object> filePropertiesEntry : fileProperties.entrySet())
				{
					file.setProperty(filePropertiesEntry.getKey(), filePropertiesEntry.getValue());
				}
				
				parentDirectory.createRelationshipTo(file, RelationshipLabels.has).setProperty(MandatoryProperties.commitId.name(), commitId);
				transaction.success();
				return fileNodeId;
			}
		}
	}
	
	@Override
	public void shareFile(String commitId, String userId, String filesystemId, int filesystemVersion, String filePath, String fileName, String shareWithUserId, String filePermission) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound, VersionNotFound
	{
		if(userId.equals(shareWithUserId))
		{
			return;
		}
		
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Node beneficiaryUser = this.commonCode.getUser(shareWithUserId);
			Node fileToBeShared = this.commonCode.getFile(userId, filesystemId, filesystemVersion, filePath, fileName);
			
			for(Relationship relationship : beneficiaryUser.getRelationships(Direction.OUTGOING, RelationshipLabels.hasAccess))
			{
				if(relationship.getEndNode().getProperty(MandatoryProperties.nodeId.name()).equals(fileToBeShared.getProperty(MandatoryProperties.nodeId.name())))
				{
					relationship.delete();
					break;
				}
			}
			
			Relationship newRelationship = beneficiaryUser.createRelationshipTo(fileToBeShared, RelationshipLabels.hasAccess);
			newRelationship.setProperty(MandatoryProperties.permission.name(), filePermission);
			newRelationship.setProperty(MandatoryProperties.commitId.name(), commitId);
			
			transaction.success();
		}
	}
	
	@Override
	public void unshareFile(String commitId, String userId, String filesystemId, int filesystemVersion, String filePath, String fileName, String unshareWithUserId) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound, VersionNotFound
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Node beneficiaryUser = this.commonCode.getUser(unshareWithUserId);
			Node fileToBeShared = this.commonCode.getFile(userId, filesystemId, filesystemVersion, filePath, fileName);
			
			for(Relationship hasAccessRelationship : beneficiaryUser.getRelationships(Direction.OUTGOING, RelationshipLabels.hasAccess))
			{
				if(hasAccessRelationship.getEndNode().getProperty(MandatoryProperties.nodeId.name()).equals(fileToBeShared.getProperty(MandatoryProperties.nodeId.name())))
				{
					Relationship hadAccessRelationship = beneficiaryUser.createRelationshipTo(fileToBeShared, RelationshipLabels.hadAccess);
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
	public void restoreFile(String commitId, String userId, String filesystemId, int filesystemVersion, String filePath, String fileName, String nodeIdToBeRestored) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound, VersionNotFound, DuplicateFile, NodeNotFound, NodeUnavailable
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			try
			{
				this.commonCode.getFile(userId, filesystemId, filesystemVersion, filePath, fileName);
				throw new DuplicateFile("ERROR: File already present! - \"" + (filePath.equals("/") ? "" : filePath) + "/" + fileName + "\"");
			}
			catch(FileNotFound fileNotFound)
			{
				this.commonCode.restoreNode(commitId, nodeIdToBeRestored);
				transaction.success();
			}
		}
	}
	
	@Override
	public String moveFile(String commitId, String userId, String filesystemId, int filesystemVersion, String oldFilePath, String oldFileName, String newFilePath, String newFileName) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound, VersionNotFound, DuplicateFile
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Map<String, Object> fileProperties = this.getFile(userId, filesystemId, filesystemVersion, oldFilePath, oldFileName);
			String nodeId = (String) fileProperties.remove(MandatoryProperties.nodeId.name());
			fileProperties.remove(MandatoryProperties.filePath.name());
			fileProperties.remove(MandatoryProperties.fileName.name());
			fileProperties.remove(MandatoryProperties.version.name());
			
			GenericService genericService = new GenericServiceImpl();
			try
			{
				genericService.deleteNodeTemporarily(commitId, nodeId);
			}
			catch (NodeNotFound | NodeUnavailable e) {}
			
			String fileNodeId = this.createNewFile(commitId, userId, filesystemId, filesystemVersion, newFilePath, newFileName, fileProperties);
			transaction.success();
			return fileNodeId;
		}
	}

	@Override
	public Map<String, Object> getFile(String userId, String filesystemId, int filesystemVersion, String filePath, String fileName) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound, VersionNotFound
	{
		try(Transaction transaction = this.graphDatabaseService.beginTx())
		{
			Node file = this.commonCode.getFile(userId, filesystemId, filesystemVersion, filePath, fileName);
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
