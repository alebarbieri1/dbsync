package br.com.itau.dbsync.service;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import br.com.itau.dbsync.dao.GenericDAO;
import br.com.itau.dbsync.model.EntityStream;

@Service
public class SyncService {

	@Autowired
	private GenericDAO genericDAO;

	@Transactional
	public void sync(EntityStream stream) {
		String schema = stream.getSchema();
		Map<String, BigInteger> foreignKeys = new HashMap<>();
		for (EntityStream.Entity entity : stream.getEntities()) {
			persistEntity(schema, foreignKeys, entity);
		}
	}

	private void persistEntity(String schema, Map<String, BigInteger> foreignKeys, EntityStream.Entity entity) {
		genericDAO.insert(schema, entity, foreignKeys);
		if (entity.getEntities() != null) {
			for (EntityStream.Entity childEntity : entity.getEntities()) {
				persistEntity(schema, foreignKeys, childEntity);
			}
		}
	}
}