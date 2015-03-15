package database.service.titancassandraembedded.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import database.MandatoryProperties;
import database.connection.singleton.TitanCassandraEmbeddedConnection;
import database.service.FileService;
import database.titan.NodeLabels;
import database.titan.RelationshipLabels;
import exception.DirectoryNotFound;
import exception.DuplicateFile;
import exception.FileNotFound;
import exception.FilesystemNotFound;
import exception.UserNotFound;
import exception.VersionNotFound;
import file.FilePermission;

public class FileServiceImpl implements FileService
{
	private TitanGraph titanGraph;
	
	public FileServiceImpl()
	{
		this.titanGraph = TitanCassandraEmbeddedConnection.getInstance().getTitanGraphObject();
	}
	
	@Override
	public void createNewFile(String commitId, String filePath, String fileName, String filesystemId, String userId, Map<String, Object> fileProperties) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, DuplicateFile
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			Vertex parentDirectory = null;
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
					parentDirectory = commonCode.getDirectory(userId, filesystemId, directoryPath, directoryName, false, null);
				}
				
				commonCode.getFile(userId, filesystemId, filePath, fileName, false);
				throw new DuplicateFile("ERROR: File already present! - \"" + filePath + "/" + fileName + "\"");
			}
			catch(FileNotFound fileNotFound)
			{
				Vertex node = this.titanGraph.addVertexWithLabel(NodeLabels.File.name());
				node.setProperty(MandatoryProperties.nodeId.name(), new AutoIncrementServiceImpl().getNextAutoIncrement());
				node.setProperty(MandatoryProperties.filePath.name(), filePath);
				node.setProperty(MandatoryProperties.fileName.name(), fileName);
				node.setProperty(MandatoryProperties.version.name(), 0);
				
				for(Entry<String, Object> filePropertiesEntry : fileProperties.entrySet())
				{
					node.setProperty(filePropertiesEntry.getKey(), filePropertiesEntry.getValue());
				}
				
				parentDirectory.addEdge(RelationshipLabels.has.name(), node).setProperty(MandatoryProperties.commitId.name(), commitId);
				titanTransaction.commit();
			}
		}
		finally
		{
			if(titanTransaction.isOpen())
			{
				titanTransaction.rollback();
			}
		}
	}

	@Override
	public void createNewVersion(String commitId, String userId, String filesystemId, String filePath, String fileName, Map<String, Object> changeMetadata, Map<String, Object> changedProperties) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			CommonCode commonCode = new CommonCode();
			Vertex file = null;
			try
			{
				file = commonCode.getVersion("file", userId, filesystemId, filePath, fileName, -1, false, null);
			}
			catch (VersionNotFound e) {}
			Vertex versionedFile = commonCode.copyNode(file);
			
			int fileLatestVersion = (int) file.getProperty(MandatoryProperties.version.name());
			versionedFile.setProperty(MandatoryProperties.version.name(), fileLatestVersion + 1);
			for(Entry<String, Object> entry : changedProperties.entrySet())
			{
				versionedFile.setProperty(entry.getKey(), entry.getValue());
			}
			versionedFile.removeProperty(MandatoryProperties.filePath.name());
			versionedFile.removeProperty(MandatoryProperties.fileName.name());
			
			Edge relationship = file.addEdge(RelationshipLabels.hasVersion.name(), versionedFile);
			for(Entry<String, Object> entry : changeMetadata.entrySet())
			{
				relationship.setProperty(entry.getKey(), entry.getValue());
			}
			relationship.setProperty(MandatoryProperties.commitId.name(), commitId);
			
			titanTransaction.commit();
		}
		finally
		{
			if(titanTransaction.isOpen())
			{
				titanTransaction.rollback();
			}
		}
	}
	
	@Override
	public void shareFile(String commitId, String userId, String filesystemId, String filePath, String fileName, String shareWithUserId, FilePermission filePermission) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			CommonCode commonCode = new CommonCode();
			Vertex beneficiaryUser = commonCode.getUser(shareWithUserId);
			Vertex fileToBeShared = commonCode.getFile(userId, filesystemId, filePath, fileName, false);
			
			for(Edge relationship : beneficiaryUser.getEdges(Direction.OUT, RelationshipLabels.hasAccess.name()))
			{
				if(relationship.getVertex(Direction.IN).getProperty(MandatoryProperties.nodeId.name()).equals(fileToBeShared.getProperty(MandatoryProperties.nodeId.name())))
				{
					relationship.remove();
					break;
				}
			}
			
			Edge newRelationship = beneficiaryUser.addEdge(RelationshipLabels.hasAccess.name(), fileToBeShared);
			newRelationship.setProperty(MandatoryProperties.permission.name(), filePermission.name());
			newRelationship.setProperty(MandatoryProperties.commitId.name(), commitId);
			
			titanTransaction.commit();
		}
		finally
		{
			if(titanTransaction.isOpen())
			{
				titanTransaction.rollback();
			}
		}
	}

	@Override
	public void unshareFile(String commitId, String userId, String filesystemId, String filePath, String fileName, String unshareWithUserId) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			CommonCode commonCode = new CommonCode();
			Vertex beneficiaryUser = commonCode.getUser(unshareWithUserId);
			Vertex fileToBeShared = commonCode.getFile(userId, filesystemId, filePath, fileName, false);
			
			Edge hadAccessRelationship = beneficiaryUser.addEdge(RelationshipLabels.hadAccess.name(), fileToBeShared);
			for(Edge hasAccessRelationship : beneficiaryUser.getEdges(Direction.OUT, RelationshipLabels.hasAccess.name()))
			{
				if(hasAccessRelationship.getVertex(Direction.IN).getProperty(MandatoryProperties.nodeId.name()).equals(fileToBeShared.getProperty(MandatoryProperties.nodeId.name())))
				{
					for(String key : hasAccessRelationship.getPropertyKeys())
					{
						hadAccessRelationship.setProperty(key, hasAccessRelationship.getProperty(key));
					}
					hadAccessRelationship.setProperty(MandatoryProperties.commitId.name(), commitId);
					
					hasAccessRelationship.remove();
					break;
				}
			}
			
			titanTransaction.commit();
		}
		finally
		{
			if(titanTransaction.isOpen())
			{
				titanTransaction.rollback();
			}
		}
	}

	@Override
	public void deleteFileTemporarily(String commitId, String userId, String filesystemId, String filePath, String fileName) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			Vertex file = new CommonCode().getFile(userId, filesystemId, filePath, fileName, false);
			Edge hasRelationship = file.getEdges(Direction.IN, RelationshipLabels.has.name()).iterator().next();
			Vertex parentDirectory = hasRelationship.getVertex(Direction.OUT);
			
			Edge hadRelationship = parentDirectory.addEdge(RelationshipLabels.had.name(), file);
			for(String key : hasRelationship.getPropertyKeys())
			{
				hadRelationship.setProperty(key, hasRelationship.getProperty(key));
			}
			hadRelationship.setProperty(MandatoryProperties.commitId.name(), commitId);
			
			hasRelationship.remove();
			titanTransaction.commit();
		}
		finally
		{
			if(titanTransaction.isOpen())
			{
				titanTransaction.rollback();
			}
		}
	}
	
	@Override
	public void restoreTemporaryDeletedFile(String commitId, String userId, String filesystemId, String filePath, String fileName) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound, DuplicateFile
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			CommonCode commonCode = new CommonCode();
			try
			{
				commonCode.getFile(userId, filesystemId, filePath, fileName, false);
				throw new DuplicateFile("ERROR: File already present! - \"" + filePath + "/" + fileName + "\"");
			}
			catch(FileNotFound fileNotFound)
			{
				Vertex file = commonCode.getFile(userId, filesystemId, filePath, fileName, true);
				Edge hadRelationship = file.getEdges(Direction.IN, RelationshipLabels.had.name()).iterator().next();
				Vertex parentDirectory = hadRelationship.getVertex(Direction.OUT);
				
				Edge hasRelationship = parentDirectory.addEdge(RelationshipLabels.has.name(), file);
				for(String key : hadRelationship.getPropertyKeys())
				{
					hasRelationship.setProperty(key, hadRelationship.getProperty(key));
				}
				hasRelationship.setProperty(MandatoryProperties.commitId.name(), commitId);
				
				hadRelationship.remove();
				titanTransaction.commit();
			}
		}
		finally
		{
			if(titanTransaction.isOpen())
			{
				titanTransaction.rollback();
			}
		}
	}
	
	@Override
	public void moveFile(String commitId, String userId, String filesystemId, String oldFilePath, String oldFileName, String newFilePath, String newFileName) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound, DuplicateFile
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
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
			titanTransaction.commit();
		}
		finally
		{
			if(titanTransaction.isOpen())
			{
				titanTransaction.rollback();
			}
		}
	}

	@Override
	public Map<String, Object> getFile(String userId, String filesystemId, String filePath, String fileName, int version) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound, VersionNotFound
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		
		try
		{
			Vertex file = new CommonCode().getVersion("file", userId, filesystemId, filePath, fileName, version, false, null);
			Map<String, Object> fileProperties = new HashMap<String, Object>();
			
			Iterable<String> keys = file.getPropertyKeys();
			for(String key : keys)
			{
				fileProperties.put(key, file.getProperty(key));
			}
			
			titanTransaction.commit();
			return fileProperties;
		}
		finally
		{
			if(titanTransaction.isOpen())
			{
				titanTransaction.rollback();
			}
		}
	}
}
