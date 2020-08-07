package br.com.itau.dbsync.dao;

import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import br.com.itau.dbsync.model.EntityStream;

@Repository
public class GenericDAO extends JdbcDaoSupport {

	@Autowired
	private DataSource dataSource;

	public int insert(String schema, EntityStream.Entity entity, Map<String, BigInteger> foreignKeys) {
		KeyHolder keyHolder = new GeneratedKeyHolder();
		JSONObject json = new JSONObject(entity.getData());

		List<String> keys = new ArrayList<>(json.keySet());
		List<String> values = new ArrayList<>(Collections.nCopies(keys.size(), "?"));

		StringBuilder sql = new StringBuilder();
		sql.append("INSERT INTO ").append(schema).append(".").append(entity.getName());
		sql.append("(").append(String.join(",", keys)).append(") ");
		sql.append("VALUES (").append(String.join(",", values)).append(")");

		int rowsAffected = this.getJdbcTemplate().update(connection -> {
			PreparedStatement ps = connection.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);

			for (int i = 0; i < keys.size(); i++) {
				Object keyValue = json.get((String) keys.get(i));
				if (keyValue instanceof Number) {
					ps.setLong(i + 1, (Integer) keyValue);
				} else if (keyValue instanceof String) {
					ps.setString(i + 1, (String) keyValue);
				}
			}
			return ps;
		}, keyHolder);

		foreignKeys.put(entity.getName(), (BigInteger) keyHolder.getKey());

		return rowsAffected;
	}

	@PostConstruct
	private void init() {
		setDataSource(dataSource);
	}
}
