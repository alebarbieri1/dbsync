package br.com.itau.ingest.service;

import br.com.itau.ingest.model.EntityStream;

public interface IngestService {
	void ingest(EntityStream stream);
}