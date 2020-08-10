package br.com.itau.dbsync.service;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import br.com.itau.dbsync.dao.GenericDAO;
import br.com.itau.dbsync.exception.EntityAlreadyExistsException;
import br.com.itau.dbsync.model.EntityStream;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class SyncService {

	@Autowired
	private GenericDAO genericDAO;

	@Transactional
	public void sync(EntityStream stream) {
		String schema = stream.getSchema();
		Map<String, Long> foreignKeys = new HashMap<>();
		for (int i = stream.getEntities().size() - 1; i >= 0; i--) {
			persistEntity(schema, foreignKeys, stream.getEntities().get(i));
		}
	}

	private void persistEntity(String schema, Map<String, Long> foreignKeys, EntityStream.Entity entity) {
		try {
			genericDAO.insert(schema, entity, foreignKeys);
		} catch (EntityAlreadyExistsException e) {
			log.info(e.getMessage() + " | Trying to update");
			genericDAO.update(schema, entity, foreignKeys);
		}
	}
}