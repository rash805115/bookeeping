package bookeeping.backend.database.service.titancassandraembedded.impl;

import java.util.Map;
import java.util.Map.Entry;

import bookeeping.backend.database.MandatoryProperties;
import bookeeping.backend.database.connection.singleton.TitanCassandraEmbeddedConnection;
import bookeeping.backend.database.service.FilesystemService;
import bookeeping.backend.database.titan.NodeLabels;
import bookeeping.backend.database.titan.RelationshipLabels;
import bookeeping.backend.exception.DuplicateFilesystem;
import bookeeping.backend.exception.FilesystemNotFound;
import bookeeping.backend.exception.NodeNotFound;
import bookeeping.backend.exception.NodeUnavailable;
import bookeeping.backend.exception.UserNotFound;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.tinkerpop.blueprints.Vertex;

public class FilesystemServiceImpl implements FilesystemService
{
	private TitanGraph titanGraph;
	private CommonCode commonCode;
	
	public FilesystemServiceImpl()
	{
		this.titanGraph = TitanCassandraEmbeddedConnection.getInstance().getTitanGraphObject();
		this.commonCode = new CommonCode();
	}
	
	@Override
	public String createNewFilesystem(String commitId, String userId, String filesystemId, Map<String, Object> filesystemProperties) throws UserNotFound, DuplicateFilesystem
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		try
		{
			try
			{
				this.commonCode.getFilesystem(userId, filesystemId);
				throw new DuplicateFilesystem("ERROR: Filesystem already present! - \"" + filesystemId + "\"");
			}
			catch(FilesystemNotFound filesystemNotFound)
			{
				Vertex filesystem = this.commonCode.createNode(NodeLabels.Filesystem);
				filesystem.setProperty(MandatoryProperties.filesystemId.name(), filesystemId);
				filesystem.setProperty(MandatoryProperties.version.name(), 0);
				String filesystemNodeId = (String) filesystem.getProperty(MandatoryProperties.nodeId.name());
				
				for(Entry<String, Object> filesystemPropertiesEntry : filesystemProperties.entrySet())
				{
					filesystem.setProperty(filesystemPropertiesEntry.getKey(), filesystemPropertiesEntry.getValue());
				}
				
				Vertex rootDirectory = this.commonCode.createNode(NodeLabels.Directory);
				Vertex user = this.commonCode.getUser(userId);
				user.addEdge(RelationshipLabels.has.name(), filesystem).setProperty(MandatoryProperties.commitId.name(), commitId);
				
				filesystem.addEdge(RelationshipLabels.has.name(), rootDirectory).setProperty(MandatoryProperties.commitId.name(), commitId);
				titanTransaction.commit();
				return filesystemNodeId;
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
	public void restoreFilesystem(String commitId, String userId, String filesystemId, String nodeIdToBeRestored) throws UserNotFound, FilesystemNotFound, DuplicateFilesystem, NodeNotFound, NodeUnavailable
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		try
		{
			try
			{
				this.commonCode.getFilesystem(userId, filesystemId);
				throw new DuplicateFilesystem("ERROR: Filesystem already present! - \"" + filesystemId + "\"");
			}
			catch(FilesystemNotFound filesystemNotFound)
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
	public Map<String, Object> getFilesystem(String userId, String filesystemId) throws UserNotFound, FilesystemNotFound
	{
		TitanTransaction titanTransaction = this.titanGraph.newTransaction();
		try
		{
			Vertex filesystem = this.commonCode.getFilesystem(userId, filesystemId);
			Map<String, Object> filesystemProperties = null;
			try
			{
				filesystemProperties = this.commonCode.getNodeProperties(filesystem);
			}
			catch(NodeNotFound nodeNotFound) {}
			
			titanTransaction.commit();
			return filesystemProperties;
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
