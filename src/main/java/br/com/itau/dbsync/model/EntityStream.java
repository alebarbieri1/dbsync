package br.com.itau.dbsync.model;

import java.io.Serializable;
import java.util.List;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class EntityStream implements Serializable {
	private static final long serialVersionUID = 1L;
	private final String schema;
	private final List<Entity> entities;

	@Getter
	@Builder
	public static class Entity implements Serializable {
		private static final long serialVersionUID = 1L;
		private final String name;
		private final String data;
		private final List<Entity> entities;
	}
}