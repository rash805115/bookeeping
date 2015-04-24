package bookeeping.backend.database.service.titancassandraembedded.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.NotFoundException;

import bookeeping.backend.database.MandatoryProperties;
import bookeeping.backend.database.connection.singleton.TitanCassandraEmbeddedConnection;
import bookeeping.backend.database.titan.NodeLabels;
import bookeeping.backend.database.titan.RelationshipLabels;
import bookeeping.backend.exception.DirectoryNotFound;
import bookeeping.backend.exception.FileNotFound;
import bookeeping.backend.exception.FilesystemNotFound;
import bookeeping.backend.exception.NodeNotFound;
import bookeeping.backend.exception.NodeUnavailable;
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
	
	public Vertex createNode(NodeLabels nodeLabel)
	{
		Vertex node = this.titanGraph.addVertexWithLabel(nodeLabel.name());
		node.setProperty(MandatoryProperties.nodeId.name(), new AutoIncrementServiceImpl().getNextAutoIncrement());
		return node;
	}
	
	public Vertex getNode(String nodeId) throws NodeNotFound
	{
		Iterator<Vertex> iterator = this.titanGraph.getVertices(MandatoryProperties.nodeId.name(), nodeId).iterator();
		if(iterator.hasNext())
		{
			return iterator.next();
		}
		else
		{
			throw new NodeNotFound("ERROR: Node not found! - \"" + nodeId + "\"");
		}
	}
	
	public Map<String, Object> getNodeProperties(Vertex node) throws NodeNotFound
	{
		Map<String, Object> nodeProperties = new HashMap<String, Object>();
		Iterable<String> nodeKeys = node.getPropertyKeys();
		
		for(String key : nodeKeys)
		{
			nodeProperties.put(key, node.getProperty(key));
		}
		
		return nodeProperties;
	}
	
	public Vertex createNodeVersion(String commidId, String nodeId, Map<String, Object> changeMetadata, Map<String, Object> changedProperties) throws NodeNotFound, NodeUnavailable
	{
		Vertex node = null;
		try
		{
			node = this.getNodeVersion(nodeId, -1);
		}
		catch (VersionNotFound e) {}
		Vertex versionedNode = this.copyNodeTree(node, new ArrayList<String>());
		
		int nodeVersion = (int) node.getProperty(MandatoryProperties.version.name());
		versionedNode.setProperty(MandatoryProperties.version.name(), nodeVersion + 1);
		for(Entry<String, Object> entry : changedProperties.entrySet())
		{
			versionedNode.setProperty(entry.getKey(), entry.getValue());
		}
		
		Edge relationship = node.addEdge(RelationshipLabels.hasVersion.name(), versionedNode);
		for(Entry<String, Object> entry : changeMetadata.entrySet())
		{
			relationship.setProperty(entry.getKey(), entry.getValue());
		}
		relationship.setProperty(MandatoryProperties.commitId.name(), commidId);
		
		return versionedNode;
	}
	
	public Vertex getNodeVersion(String nodeId, int version) throws NodeNotFound, VersionNotFound, NodeUnavailable
	{
		Vertex node = this.getNode(nodeId);
		do
		{
			int nodeVersion = -1;
			try
			{
				nodeVersion = (int) node.getProperty(MandatoryProperties.version.name());
			}
			catch(NotFoundException notFoundException)
			{
				throw new NodeUnavailable("ERROR: No version property for this node! - \"" + nodeId + "(v=" + version + ")\"");
			}
			
			if(nodeVersion == version)
			{
				return node;
			}
			else
			{
				Iterator<Edge> iterator = node.getEdges(Direction.OUT, RelationshipLabels.hasVersion.name()).iterator();
				if(iterator.hasNext())
				{
					node = iterator.next().getVertex(Direction.IN);
				}
				else
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
			}
		}
		while(node != null);
		
		throw new VersionNotFound("ERROR: Node version not found! - \"" + nodeId + "(v=" + version + ")\"");
	}
	
	public List<Map<String, Object>> getNodeVersions(String nodeId) throws NodeNotFound
	{
		List<Map<String, Object>> versionList = new ArrayList<Map<String, Object>>();
		Vertex node = this.getNode(nodeId);
		versionList.add(this.getNodeProperties(node));
		
		do
		{
			Iterator<Edge> iterator = node.getEdges(Direction.OUT, RelationshipLabels.hasVersion.name()).iterator();
			if(iterator.hasNext())
			{
				node = iterator.next().getVertex(Direction.IN);
			}
			else
			{
				node = null;
			}
		}
		while(node != null);
		
		return versionList;
	}
	
	public List<Vertex> getChildren(String nodeId) throws NodeNotFound
	{
		List<Vertex> nodeList = new ArrayList<Vertex>();
		Vertex node = this.getNode(nodeId);
		Iterable<Edge> iterable = node.getEdges(Direction.OUT, RelationshipLabels.has.name());
		for(Edge relationship : iterable)
		{
			nodeList.add(relationship.getVertex(Direction.IN));
		}
		
		return nodeList;
	}
	
	public List<Vertex> getDeletedChildren(String nodeId) throws NodeNotFound
	{
		List<Vertex> nodeList = new ArrayList<Vertex>();
		Vertex node = this.getNode(nodeId);
		Iterable<Edge> iterable = node.getEdges(Direction.OUT, RelationshipLabels.had.name());
		for(Edge relationship : iterable)
		{
			nodeList.add(relationship.getVertex(Direction.IN));
		}
		
		return nodeList;
	}
	
	public Vertex deleteNodeTemporarily(String commitId, String nodeId) throws NodeNotFound, NodeUnavailable
	{
		Vertex node = this.getNode(nodeId);
		Edge hasRelationship = null;
		Iterator<Edge> iterator = node.getEdges(Direction.IN, RelationshipLabels.has.name()).iterator();
		if(! iterator.hasNext())
		{
			throw new NodeUnavailable("ERROR: Node is unavailable! - \"" + nodeId + "\". Could be it has already been deleted.");
		}
		else
		{
			hasRelationship = iterator.next();
		}

		if(hasRelationship.getVertex(Direction.OUT).getProperty(MandatoryProperties.filesystemId.name()) != null)
		{
			throw new NodeUnavailable("ERROR: Root node cannot be deleted! - \"" + nodeId + "\"");
		}
		
		Vertex parentNode = hasRelationship.getVertex(Direction.OUT);
		Edge hadRelationship = parentNode.addEdge(RelationshipLabels.had.name(), node);
		for(String key : hasRelationship.getPropertyKeys())
		{
			hadRelationship.setProperty(key, hasRelationship.getProperty(key));
		}
		hadRelationship.setProperty(MandatoryProperties.commitId.name(), commitId);
		
		hasRelationship.remove();
		return node;
	}
	
	public Vertex restoreNode(String commitId, String nodeId) throws NodeNotFound, NodeUnavailable
	{
		Vertex node = this.getNode(nodeId);
		Edge hadRelationship = null;
		Iterator<Edge> iterator = node.getEdges(Direction.IN, RelationshipLabels.had.name()).iterator();
		if(! iterator.hasNext())
		{
			throw new NodeUnavailable("ERROR: Node is unavailable! - \"" + nodeId + "\". Could be it has not been deleted.");
		}
		else
		{
			hadRelationship = iterator.next();
		}
		
		Vertex parentNode = hadRelationship.getVertex(Direction.OUT);
		Edge hasRelationship = parentNode.addEdge(RelationshipLabels.has.name(), node);
		for(String key : hadRelationship.getPropertyKeys())
		{
			hasRelationship.setProperty(key, hadRelationship.getProperty(key));
		}
		hasRelationship.setProperty(MandatoryProperties.commitId.name(), commitId);
		
		hadRelationship.remove();
		return node;
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
	
	public Vertex getFilesystem(String userId, String filesystemId) throws UserNotFound, FilesystemNotFound
	{
		Vertex user = this.getUser(userId);
		Iterable<Edge> iterable = user.getEdges(Direction.OUT, RelationshipLabels.has.name());
		
		for(Edge relationship : iterable)
		{
			Vertex filesystem = relationship.getVertex(Direction.IN);
			String retrievedFilesystemId = (String) filesystem.getProperty(MandatoryProperties.filesystemId.name());
			
			if(retrievedFilesystemId.equals(filesystemId))
			{
				return filesystem;
			}
		}
		
		throw new FilesystemNotFound("ERROR: Filesystem not found! - \"" + filesystemId + "\"");
	}
	
	public Vertex getRootDirectory(String userId, String filesystemId, int filesystemVersion) throws UserNotFound, FilesystemNotFound, VersionNotFound
	{
		Vertex filesystem = this.getFilesystem(userId, filesystemId);
		Vertex versionedFilesystem = null;
		try
		{
			versionedFilesystem = this.getNodeVersion((String) filesystem.getProperty(MandatoryProperties.nodeId.name()), filesystemVersion);
		}
		catch (NodeNotFound | NodeUnavailable e) {}
		return versionedFilesystem.getEdges(Direction.OUT, RelationshipLabels.has.name()).iterator().next().getVertex(Direction.IN);
	}
	
	public Vertex getDirectory(String userId, String filesystemId, int filesystemVersion, String directoryPath, String directoryName) throws UserNotFound, FilesystemNotFound, VersionNotFound, DirectoryNotFound
	{
		Vertex rootDirectory = this.getRootDirectory(userId, filesystemId, filesystemVersion);
		Iterable<Edge> iterable = rootDirectory.getEdges(Direction.OUT, RelationshipLabels.has.name());
		
		for(Edge relationship : iterable)
		{
			Vertex node = relationship.getVertex(Direction.IN);
			TitanVertex vertex = (TitanVertex) node;
			if(vertex.getLabel().equals(NodeLabels.Directory.name()))
			{
				String retrievedDirectoryPath = (String) node.getProperty(MandatoryProperties.directoryPath.name());
				String retrievedDirectoryName = (String) node.getProperty(MandatoryProperties.directoryName.name());
				
				if(retrievedDirectoryPath.equals(directoryPath) && retrievedDirectoryName.equals(directoryName))
				{
					return node;
				}
			}
		}
		
		throw new DirectoryNotFound("ERROR: Directory not found! - \"" + (directoryPath.equals("/") ? "" : directoryPath) + "/" + directoryName + "\"");
	}
	
	public List<Vertex> getAllDirectory(String userId, String filesystemId, int filesystemVersion) throws UserNotFound, FilesystemNotFound, VersionNotFound
	{
		List<Vertex> directoryList = new ArrayList<Vertex>();
		Vertex rootDirectory = this.getRootDirectory(userId, filesystemId, filesystemVersion);
		Iterable<Edge> iterable = rootDirectory.getEdges(Direction.OUT, RelationshipLabels.has.name());
		
		for(Edge relationship : iterable)
		{
			Vertex node = relationship.getVertex(Direction.IN);
			TitanVertex vertex = (TitanVertex) node;
			if(vertex.getLabel().equals(NodeLabels.Directory.name()))
			{
				directoryList.add(node);
			}
		}
		
		return directoryList;
	}
	
	public Vertex getFile(String userId, String filesystemId, int filesystemVersion, String filePath, String fileName) throws UserNotFound, FilesystemNotFound, VersionNotFound, DirectoryNotFound, FileNotFound
	{
		Vertex parentDirectory = null;
		if(filePath.equals("/"))
		{
			parentDirectory = this.getRootDirectory(userId, filesystemId, filesystemVersion);
		}
		else
		{
			String directoryName = filePath.substring(filePath.lastIndexOf("/") + 1, filePath.length());
			String directoryPath = filePath.substring(0, filePath.lastIndexOf("/" + directoryName));
			directoryPath = directoryPath.length() == 0 ? "/" : directoryPath;
			parentDirectory = this.getDirectory(userId, filesystemId, filesystemVersion, directoryPath, directoryName);
		}
		
		Iterable<Edge> iterable = parentDirectory.getEdges(Direction.OUT, RelationshipLabels.has.name());
		for(Edge relationship : iterable)
		{
			Vertex node = relationship.getVertex(Direction.IN);
			TitanVertex vertex = (TitanVertex) node;
			if(vertex.getLabel().equals(NodeLabels.File.name()))
			{
				String retrievedFilePath = (String) node.getProperty(MandatoryProperties.filePath.name());
				String retrievedFileName = (String) node.getProperty(MandatoryProperties.fileName.name());
				
				if(retrievedFilePath.equals(filePath) && retrievedFileName.equals(fileName))
				{
					return node;
				}
			}
		}
		
		throw new FileNotFound("ERROR: File not found! - \"" + (filePath.equals("/") ? "" : filePath) + "/" + fileName + "\"");
	}
	
	public Vertex copyNode(Vertex node)
	{
		TitanVertex vertex = (TitanVertex) node;
		Vertex copyNode = this.createNode(NodeLabels.valueOf(vertex.getLabel()));
		
		for(String key : node.getPropertyKeys())
		{
			if(! key.equals(MandatoryProperties.nodeId.name()))
			{
				copyNode.setProperty(key, node.getProperty(key));
			}
		}
		
		return copyNode;
	}
	
	public Vertex copyNodeTree(Vertex node, List<String> ignoreRelationships)
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
				
				for(RelationshipLabels relationshipLabel : RelationshipLabels.values())
				{
					if(ignoreRelationships.contains(relationshipLabel.name()))
					{
						continue;
					}
					
					Iterable<Edge> relationships = currentNode.getEdges(Direction.OUT, relationshipLabel.name());
					for(Edge relationship : relationships)
					{
						Vertex childNode = relationship.getVertex(Direction.IN);
						Vertex childNodeCopy = this.copyNode(childNode);
						Edge currentNodeCopyRelationship = currentNodeCopy.addEdge(relationship.getLabel(), childNodeCopy);
						
						for(String key : relationship.getPropertyKeys())
						{
							currentNodeCopyRelationship.setProperty(key, relationship.getProperty(key));
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
