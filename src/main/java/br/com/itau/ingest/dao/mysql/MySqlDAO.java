package br.com.itau.ingest.dao.mysql;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import br.com.itau.ingest.dao.GenericDAO;
import br.com.itau.ingest.exception.EntityAlreadyExistsException;
import br.com.itau.ingest.model.EntityStream;
import lombok.extern.slf4j.Slf4j;

@Repository
@Slf4j
class MySqlDAO extends JdbcDaoSupport implements GenericDAO {

	@Autowired
	private DataSource dataSource;

	public int insert(String schema, EntityStream.Entity entity, Map<String, Long> foreignKeys)
			throws EntityAlreadyExistsException {
		KeyHolder keyHolder = new GeneratedKeyHolder();
		JSONObject json = new JSONObject(entity.getData());
		String primaryKeyColumn = null;
		Object primaryKeyValue = null;

		List<String> keys = new ArrayList<>(json.keySet());
		List<String> keysSql = new ArrayList<>();
		for (String key : keys) {
			if (key.endsWith("_pk") || key.endsWith("_fk")) {
				if (key.endsWith("_pk")) {
					primaryKeyColumn = key.substring(0, key.length() - 3);
					primaryKeyValue = json.get(key);
				}
				keysSql.add(key.substring(0, key.length() - 3));
			} else {
				keysSql.add(key);
			}
		}
		String insertStatement = generateInsertStatement(schema, entity.getName(), keysSql);

		try {
			int rowsAffected = this.getJdbcTemplate().update(connection -> {
				PreparedStatement ps = createPreparedStatement(connection, insertStatement, true, keys, json,
						foreignKeys);
				return ps;
			}, keyHolder);

			if (keyHolder != null && keyHolder.getKey() != null) {
				log.info("{} row(s) affected for entity {} | insert | [ID = {}]", rowsAffected, entity.getName(),
						keyHolder.getKey());
				foreignKeys.put(primaryKeyColumn, ((BigInteger) keyHolder.getKey()).longValue());
			} else {
				log.info("{} row(s) affected for entity {} | insert");
			}

			return rowsAffected;
		} catch (DuplicateKeyException e) {
			throw new EntityAlreadyExistsException(
					"Entity " + entity.getName() + " [ID = " + primaryKeyValue + "] already exists");
		}

	}

	public int update(String schema, EntityStream.Entity entity, Map<String, Long> foreignKeys) {
		JSONObject json = new JSONObject(entity.getData());
		String primaryKeyColumn = null;
		Object primaryKeyValue = null;

		List<String> keys = new ArrayList<>(json.keySet());
		List<String> keysSql = new ArrayList<>();
		for (String key : keys) {
			if (key.endsWith("_pk") || key.endsWith("_fk")) {
				if (key.endsWith("_pk")) {
					primaryKeyColumn = key.substring(0, key.length() - 3);
					primaryKeyValue = json.get(key);
				}
				keysSql.add(key.substring(0, key.length() - 3));
			} else {
				keysSql.add(key);
			}
		}

		String updateStatement = generateUpdateStatement(schema, entity.getName(), keysSql, primaryKeyColumn);

		int rowsAffected = this.getJdbcTemplate().update(connection -> {
			PreparedStatement ps = createPreparedStatement(connection, updateStatement, false, keys, json, foreignKeys);
			return ps;
		});

		log.info("{} row(s) affected for entity {} | update | [ID = {}]", rowsAffected, entity.getName(),
				primaryKeyValue);

		return rowsAffected;
	}

	private PreparedStatement createPreparedStatement(Connection connection, String sqlStatement,
			boolean returnGeneratedKeys, List<String> columnNames, JSONObject json, Map<String, Long> foreignKeys)
			throws SQLException {
		PreparedStatement ps = null;
		if (returnGeneratedKeys) {
			ps = connection.prepareStatement(sqlStatement, Statement.RETURN_GENERATED_KEYS);
		} else {
			ps = connection.prepareStatement(sqlStatement);
		}

		setPreparedStatementValues(ps, columnNames, json, foreignKeys, returnGeneratedKeys);

		return ps;
	}

	private void setPreparedStatementValues(PreparedStatement ps, List<String> columnNames, JSONObject json,
			Map<String, Long> foreignKeys, boolean returnGeneratedKeys) throws SQLException {

		for (int i = 0; i < columnNames.size(); i++) {
			Object keyValue = json.get((String) columnNames.get(i));
			String keyName = columnNames.get(i);
			if (keyName.endsWith("_fk")) {
				if (keyValue.equals(JSONObject.NULL)) {
					ps.setLong(i + 1, foreignKeys.get(keyName.substring(0, keyName.length() - 3)));
				} else {
					ps.setLong(i + 1, (Integer) keyValue);
				}
			} else {
				if (keyValue.equals(JSONObject.NULL)) {
					ps.setNull(i + 1, Types.NULL);
				} else if (keyValue instanceof Number) {
					ps.setLong(i + 1, (Integer) keyValue);
				} else if (keyValue instanceof String) {
					ps.setString(i + 1, (String) keyValue);
				}
			}

			if (!returnGeneratedKeys) {
				// Where condition
				if (keyName.endsWith("_pk")) {
					ps.setLong(columnNames.size() + 1, (Integer) json.get(keyName));
				}
			}
		}
	}

	private String generateInsertStatement(String schema, String entityName, List<String> columnNames) {
		List<String> values = new ArrayList<>(Collections.nCopies(columnNames.size(), "?"));

		StringBuilder sql = new StringBuilder();
		sql.append("INSERT INTO ").append(schema).append(".").append(entityName);
		sql.append("(").append(String.join(",", columnNames)).append(") ");
		sql.append("VALUES (").append(String.join(",", values)).append(")");
		return sql.toString();
	}

	private String generateUpdateStatement(String schema, String entityName, List<String> columnNames,
			String primaryKeyColumn) {

		StringBuilder sql = new StringBuilder();
		sql.append("UPDATE ").append(schema).append(".").append(entityName);
		sql.append(" SET ");
		for (int i = 0; i < columnNames.size(); i++) {
			if (i == columnNames.size() - 1) {
				sql.append(columnNames.get(i)).append(" = ? ");
				sql.append("WHERE ").append(primaryKeyColumn).append(" = ?");
			} else {
				sql.append(columnNames.get(i)).append(" = ?, ");
			}
		}
		return sql.toString();
	}

	@PostConstruct
	private void init() {
		setDataSource(dataSource);
	}
}