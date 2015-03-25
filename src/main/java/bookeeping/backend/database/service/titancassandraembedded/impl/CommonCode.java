package bookeeping.backend.database.service.titancassandraembedded.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import bookeeping.backend.database.MandatoryProperties;
import bookeeping.backend.database.connection.singleton.TitanCassandraEmbeddedConnection;
import bookeeping.backend.database.titan.NodeLabels;
import bookeeping.backend.database.titan.RelationshipLabels;
import bookeeping.backend.exception.DirectoryNotFound;
import bookeeping.backend.exception.FileNotFound;
import bookeeping.backend.exception.FilesystemNotFound;
import bookeeping.backend.exception.UserNotFound;
import bookeeping.backend.exception.VersionNotFound;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class CommonCode
{
	private TitanGraph titanGraph;
	
	public CommonCode()
	{
		this.titanGraph = TitanCassandraEmbeddedConnection.getInstance().getTitanGraphObject();
	}
	
	public Vertex getUser(String userId) throws UserNotFound
	{
		Iterator<Vertex> iterator = this.titanGraph.getVertices(MandatoryProperties.userId.name(), userId).iterator();
		if(iterator.hasNext())
		{
			return iterator.next();
		}
		else
		{
			throw new UserNotFound("ERROR: User not found! - \"" + userId + "\"");
		}
	}
	
	public Vertex getFilesystem(String userId, String filesystemId, boolean deleted) throws FilesystemNotFound, UserNotFound
	{
		Vertex user = this.getUser(userId);
		Iterable<Vertex> iterable;
		
		if(deleted)
		{
			iterable = user.getVertices(Direction.OUT, RelationshipLabels.had.name());
		}
		else
		{
			iterable = user.getVertices(Direction.OUT, RelationshipLabels.has.name());
		}
		
		for(Vertex vertex : iterable)
		{
			String retrievedFilesystemId = vertex.getProperty(MandatoryProperties.filesystemId.name());
			if(retrievedFilesystemId.equals(filesystemId))
			{
				return vertex;
			}
		}
		
		throw new FilesystemNotFound("ERROR: Filesystem not found! - \"" + filesystemId + "\"");
	}
	
	public Vertex getRootDirectory(String userId, String filesystemId) throws FilesystemNotFound, UserNotFound
	{
		Vertex filesystem = this.getFilesystem(userId, filesystemId, false);
		return filesystem.getVertices(Direction.OUT, RelationshipLabels.has.name()).iterator().next();
	}
	
	public Vertex getDirectory(String userId, String filesystemId, String directoryPath, String directoryName, boolean deleted, String commitId) throws FilesystemNotFound, UserNotFound, DirectoryNotFound
	{
		Vertex rootDirectory = this.getRootDirectory(userId, filesystemId);
		Iterable<Vertex> iterable;
		if(deleted)
		{
			iterable = rootDirectory.getVertices(Direction.OUT, RelationshipLabels.had.name());
		}
		else
		{
			iterable = rootDirectory.getVertices(Direction.OUT, RelationshipLabels.has.name());
		}
		
		for(Vertex vertex : iterable)
		{
			String retrievedDirectoryPath = vertex.getProperty(MandatoryProperties.directoryPath.name());
			String retrievedDirectoryName = vertex.getProperty(MandatoryProperties.directoryName.name());
			String retrievedCommitId = vertex.getProperty(MandatoryProperties.commitId.name());
			
			if(retrievedDirectoryPath.equals(directoryPath) && retrievedDirectoryName.equals(directoryName) && (commitId == null ? true : retrievedCommitId.equals(commitId)))
			{
				return vertex;
			}
		}
		
		throw new DirectoryNotFound("ERROR: Directory not found! - \"" + (directoryPath.equals("/") ? "" : directoryPath) + "/" + directoryName + "\"");
	}
	
	public Vertex getFile(String userId, String filesystemId, String filePath, String fileName, boolean deleted, String commitId) throws FilesystemNotFound, UserNotFound, DirectoryNotFound, FileNotFound
	{
		Vertex parentDirectory = null;
		if(filePath.equals("/"))
		{
			parentDirectory = this.getRootDirectory(userId, filesystemId);
		}
		else
		{
			String directoryName = filePath.substring(filePath.lastIndexOf("/") + 1, filePath.length());
			String directoryPath = filePath.substring(0, filePath.lastIndexOf("/" + directoryName));
			directoryPath = directoryPath.length() == 0 ? "/" : directoryPath;
			parentDirectory = this.getDirectory(userId, filesystemId, directoryPath, directoryName, false, null);
		}
		
		Iterable<Vertex> iterable;
		if(deleted)
		{
			iterable = parentDirectory.getVertices(Direction.OUT, RelationshipLabels.had.name());
		}
		else
		{
			iterable = parentDirectory.getVertices(Direction.OUT, RelationshipLabels.has.name());
		}
		
		for(Vertex vertex : iterable)
		{
			String retrievedFilePath = vertex.getProperty(MandatoryProperties.filePath.name());
			String retrievedFileName = vertex.getProperty(MandatoryProperties.fileName.name());
			String retrievedCommitId = vertex.getProperty(MandatoryProperties.commitId.name());
			
			if(retrievedFilePath.equals(filePath) && retrievedFileName.equals(fileName) && (commitId == null ? true : retrievedCommitId.equals(commitId)))
			{
				return vertex;
			}
		}
		
		throw new FileNotFound("ERROR: File not found! - \"" + (filePath.equals("/") ? "" : filePath) + "/" + fileName + "\"");
	}
	
	public Vertex getVersion(String nodeType, String userId, String filesystemId, String path, String name, int version, boolean deleted, String commitId) throws FilesystemNotFound, UserNotFound, DirectoryNotFound, FileNotFound, VersionNotFound
	{
		Vertex node = null;
		if(nodeType.equalsIgnoreCase("filesystem"))
		{
			node = this.getFilesystem(userId, filesystemId, deleted);
		}
		else if(nodeType.equalsIgnoreCase("directory"))
		{
			node = this.getDirectory(userId, filesystemId, path, name, deleted, commitId);
		}
		else
		{
			node = this.getFile(userId, filesystemId, path, name, deleted, commitId);
		}
		
		do
		{
			if(version != -1)
			{
				int retrievedVersion = (int) node.getProperty(MandatoryProperties.version.name());
				if(retrievedVersion == version)
				{
					return node;
				}
			}
			
			Iterator<Vertex> iterator = node.getVertices(Direction.OUT, RelationshipLabels.hasVersion.name()).iterator();
			if(! iterator.hasNext())
			{
				if(version == -1)
				{
					return node;
				}
				else
				{
					node = null;
				}
			}
			else
			{
				node = iterator.next();
			}
		}
		while(node != null);
		
		if(nodeType.equalsIgnoreCase("filesystem"))
		{
			throw new VersionNotFound("ERROR: Version not found! - Filesystem: \"" + filesystemId + "\", Version - \"" + version + "\"");
		}
		else if(nodeType.equalsIgnoreCase("directory"))
		{
			throw new VersionNotFound("ERROR: Version not found! - Directory: \"" + path + "/" + name + "\", Version - \"" + version + "\"");
		}
		else
		{
			throw new VersionNotFound("ERROR: Version not found! - File: \"" + path + "/" + name + "\", Version - \"" + version + "\"");
		}
	}
	
	public Vertex copyNode(Vertex node)
	{
		TitanVertex titanVertex = (TitanVertex) node;
		Vertex copyNode = this.titanGraph.addVertexWithLabel(titanVertex.getLabel());
		for(String key : node.getPropertyKeys())
		{
			if(! key.equals(MandatoryProperties.nodeId.name()))
			{
				copyNode.setProperty(key, node.getProperty(key));
			}
		}
		copyNode.setProperty(MandatoryProperties.nodeId.name(), new AutoIncrementServiceImpl().getNextAutoIncrement());
		
		return copyNode;
	}
	
	public Vertex copyNodeTree(Vertex node)
	{
		List<Vertex> pendingNodeList = new ArrayList<Vertex>();
		pendingNodeList.add(node);
		
		List<Vertex> pendingNodeCopyList = new ArrayList<Vertex>();
		Vertex rootNodeCopy = this.copyNode(node);
		pendingNodeCopyList.add(rootNodeCopy);
		
		do
		{
			List<Vertex> childNodeList = new ArrayList<Vertex>();
			List<Vertex> childNodeCopyList = new ArrayList<Vertex>();
			
			for(int i = 0; i < pendingNodeList.size(); i++)
			{
				Vertex currentNode = pendingNodeList.get(i);
				Vertex currentNodeCopy = pendingNodeCopyList.get(i);
				
				for(NodeLabels nodeLabels : NodeLabels.values())
				{
					Iterable<Edge> iterable = currentNode.getEdges(Direction.OUT, nodeLabels.name());
					for(Edge edge : iterable)
					{
						Vertex childNode = edge.getVertex(Direction.IN);
						Vertex childNodeCopy = this.copyNode(childNode);
						
						Edge currentNodeCopyRelationship = currentNodeCopy.addEdge(edge.getLabel(), childNodeCopy);
						for(String key : edge.getPropertyKeys())
						{
							currentNodeCopyRelationship.setProperty(key, edge.getProperty(key));
						}
						
						childNodeList.add(childNode);
						childNodeCopyList.add(childNodeCopy);
					}
				}
			}
			
			pendingNodeList.clear();
			pendingNodeList.addAll(childNodeList);
			pendingNodeCopyList.clear();
			pendingNodeCopyList.addAll(childNodeCopyList);
		}
		while(! pendingNodeList.isEmpty());
		
		return rootNodeCopy;
	}
}