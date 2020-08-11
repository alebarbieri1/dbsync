package br.com.itau.ingest.dao;

import java.util.Map;

import br.com.itau.ingest.exception.EntityAlreadyExistsException;
import br.com.itau.ingest.model.EntityStream;

public interface GenericDAO {

	public int insert(String schema, EntityStream.Entity entity, Map<String, Long> foreignKeys)
			throws EntityAlreadyExistsException;

	public int update(String schema, EntityStream.Entity entity, Map<String, Long> foreignKeys);
}
