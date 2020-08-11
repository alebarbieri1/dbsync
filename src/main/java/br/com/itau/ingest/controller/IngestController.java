package br.com.itau.ingest.controller;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import br.com.itau.ingest.model.EntityStream;
import br.com.itau.ingest.model.Response;
import br.com.itau.ingest.service.IngestService;

@RestController
public class IngestController {

	@Autowired
	private IngestService service;

	@PostMapping("/ingest")
	private ResponseEntity<Response> sync(@RequestBody @Valid EntityStream stream) {
		service.ingest(stream);
		return ResponseEntity.ok(Response.builder().message("Success").build());
	}
}