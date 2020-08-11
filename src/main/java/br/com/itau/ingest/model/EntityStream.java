package br.com.itau.ingest.model;

import java.io.Serializable;
import java.util.List;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class EntityStream implements Serializable {
	private static final long serialVersionUID = 1L;
	@NotBlank
	private final String schema;
	@Valid
	private final List<Entity> entities;

	@Getter
	@Builder
	public static class Entity implements Serializable {
		private static final long serialVersionUID = 1L;
		@NotBlank
		private final String name;
		@NotBlank
		private final String data;
	}
}