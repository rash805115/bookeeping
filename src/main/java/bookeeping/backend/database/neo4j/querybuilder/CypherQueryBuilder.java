package bookeeping.backend.database.neo4j.querybuilder;

import java.util.Map;

import bookeeping.backend.database.neo4j.NodeLabels;

public class CypherQueryBuilder
{
	private String query;
	
	public CypherQueryBuilder(QueryType queryType)
	{
		switch(queryType)
		{
			case MATCH_NODE:
			{
				this.query = "match (node%s) %s return node";
				break;
			}
			case MATCH_COUNT:
			{
				this.query = "match (node%s) %s return count(node) as count";
				break;
			}
			default:
			{
				this.query = null;
				break;
			}
		}
	}
	
	public String buildQuery(NodeLabels nodeLabel, Map<String, Object> userProperties, boolean intersection)
	{
		if(nodeLabel == null && userProperties.size() == 0)
		{
			this.query = String.format(this.query, "", "");
		}
		else if(nodeLabel != null && userProperties.size() == 0)
		{
			this.query = String.format(this.query, ":" + nodeLabel.name(), "");
		}
		else if(nodeLabel == null && userProperties.size() != 0)
		{
			String whereClause = "where ";
			String whereClauseSeparator = (intersection) ? " and " : " or ";
			for(String key : userProperties.keySet())
			{
				whereClause += "node." + key + "={" + key + "}" + whereClauseSeparator;
			}
			whereClause = whereClause.substring(0, whereClause.length() - whereClauseSeparator.length());
			
			this.query = String.format(this.query, "", whereClause);
		}
		else if(nodeLabel != null && userProperties.size() != 0)
		{
			String whereClause = "where ";
			String whereClauseSeparator = (intersection) ? " and " : " or ";
			for(String key : userProperties.keySet())
			{
				whereClause += "node." + key + "={" + key + "}" + whereClauseSeparator;
			}
			whereClause = whereClause.substring(0, whereClause.length() - whereClauseSeparator.length());
			
			this.query = String.format(this.query, ":" + nodeLabel.name(), whereClause);
		}
		
		return this.query;
	}
}
