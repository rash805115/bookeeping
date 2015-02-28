package database.service;

import java.util.Map;

import exception.DuplicateUser;
import exception.UserNotFound;

public interface DatabaseService
{
	public int getNextAutoIncrement();
	public void createNewUser(String userId, Map<String, Object> userProperties) throws DuplicateUser;
	public Map<String, Object> getUser(String userId) throws UserNotFound;
}
