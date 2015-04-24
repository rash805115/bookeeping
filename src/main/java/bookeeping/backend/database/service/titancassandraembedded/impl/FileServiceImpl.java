package bookeeping.backend.database.service.titancassandraembedded.impl;

import java.util.Map;
import java.util.Map.Entry;

import bookeeping.backend.database.MandatoryProperties;
import bookeeping.backend.database.connection.singleton.TitanCassandraEmbeddedConnection;
import bookeeping.backend.database.service.FileService;
import bookeeping.backend.database.service.GenericService;
import bookeeping.backend.database.titan.NodeLabels;
import bookeeping.backend.database.titan.RelationshipLabels;
import bookeeping.backend.exception.DirectoryNotFound;
import bookeeping.backend.exception.DuplicateFile;
import bookeeping.backend.exception.FileNotFound;
import bookeeping.backend.exception.FilesystemNotFound;
import bookeeping.backend.exception.NodeNotFound;
import bookeeping.backend.exception.NodeUnavailable;
import bookeeping.backend.exception.UserNotFound;
import bookeeping.backend.exception.VersionNotFound;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class FileServiceImpl implements FileService
{
	private TitanGraph titanGraph;
	private CommonCode commonCode;
	
	public FileServiceImpl()
	{
		this.titanGraph = TitanCassandraEmbeddedConnection.getInstance().getTitanGraphObject();
		this.commonCode = new CommonCode();
	}
	
	@Override
	public String createNewFile(String commitId, String userId, String filesystemId, int filesystemVersion, String filePath, String fileName, Map<String, Object> fileProperties) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, VersionNotFound, DuplicateFile
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		try
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
				
				Vertex parentDirectory = null;
				if(directoryPath.length() == 0)
				{
					parentDirectory = this.commonCode.getRootDirectory(userId, filesystemId, filesystemVersion);
				}
				else
				{
					parentDirectory = this.commonCode.getDirectory(userId, filesystemId, filesystemVersion, directoryPath, directoryName);
				}
				
				Vertex file = this.commonCode.createNode(NodeLabels.File);
				file.setProperty(MandatoryProperties.filePath.name(), filePath);
				file.setProperty(MandatoryProperties.fileName.name(), fileName);
				file.setProperty(MandatoryProperties.version.name(), 0);
				String fileNodeId = (String) file.getProperty(MandatoryProperties.nodeId.name());
				
				for(Entry<String, Object> filePropertiesEntry : fileProperties.entrySet())
				{
					file.setProperty(filePropertiesEntry.getKey(), filePropertiesEntry.getValue());
				}
				
				parentDirectory.addEdge(RelationshipLabels.has.name(), file).setProperty(MandatoryProperties.commitId.name(), commitId);
				titanTransaction.commit();
				return fileNodeId;
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
	public void shareFile(String commitId, String userId, String filesystemId, int filesystemVersion, String filePath, String fileName, String shareWithUserId, String filePermission) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound, VersionNotFound
	{
		if(userId.equals(shareWithUserId))
		{
			return;
		}
		
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		try
		{
			Vertex beneficiaryUser = this.commonCode.getUser(shareWithUserId);
			Vertex fileToBeShared = this.commonCode.getFile(userId, filesystemId, filesystemVersion, filePath, fileName);
			
			for(Edge relationship : beneficiaryUser.getEdges(Direction.OUT, RelationshipLabels.hasAccess.name()))
			{
				if(relationship.getVertex(Direction.IN).getProperty(MandatoryProperties.nodeId.name()).equals(fileToBeShared.getProperty(MandatoryProperties.nodeId.name())))
				{
					relationship.remove();
					break;
				}
			}
			
			Edge newRelationship = beneficiaryUser.addEdge(RelationshipLabels.hasAccess.name(), fileToBeShared);
			newRelationship.setProperty(MandatoryProperties.permission.name(), filePermission);
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
	public void unshareFile(String commitId, String userId, String filesystemId, int filesystemVersion, String filePath, String fileName, String unshareWithUserId) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound, VersionNotFound
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		try
		{
			Vertex beneficiaryUser = this.commonCode.getUser(unshareWithUserId);
			Vertex fileToBeShared = this.commonCode.getFile(userId, filesystemId, filesystemVersion, filePath, fileName);
			
			for(Edge hasAccessRelationship : beneficiaryUser.getEdges(Direction.OUT, RelationshipLabels.hasAccess.name()))
			{
				if(hasAccessRelationship.getVertex(Direction.IN).getProperty(MandatoryProperties.nodeId.name()).equals(fileToBeShared.getProperty(MandatoryProperties.nodeId.name())))
				{
					Edge hadAccessRelationship = beneficiaryUser.addEdge(RelationshipLabels.hadAccess.name(), fileToBeShared);
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
	public void restoreFile(String commitId, String userId, String filesystemId, int filesystemVersion, String filePath, String fileName, String nodeIdToBeRestored) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound, VersionNotFound, DuplicateFile, NodeNotFound, NodeUnavailable
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		try
		{
			try
			{
				this.commonCode.getFile(userId, filesystemId, filesystemVersion, filePath, fileName);
				throw new DuplicateFile("ERROR: File already present! - \"" + (filePath.equals("/") ? "" : filePath) + "/" + fileName + "\"");
			}
			catch(FileNotFound fileNotFound)
			{
				this.commonCode.restoreNode(commitId, nodeIdToBeRestored);
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
	public String moveFile(String commitId, String userId, String filesystemId, int filesystemVersion, String oldFilePath, String oldFileName, String newFilePath, String newFileName) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound, VersionNotFound, DuplicateFile
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		try
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
			titanTransaction.commit();
			return fileNodeId;
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
	public Map<String, Object> getFile(String userId, String filesystemId, int filesystemVersion, String filePath, String fileName) throws UserNotFound, FilesystemNotFound, DirectoryNotFound, FileNotFound, VersionNotFound
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		try
		{
			Vertex file = this.commonCode.getFile(userId, filesystemId, filesystemVersion, filePath, fileName);
			Map<String, Object> fileProperties = null;
			try
			{
				fileProperties = this.commonCode.getNodeProperties(file);
			}
			catch(NodeNotFound nodeNotFound) {}
			
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
