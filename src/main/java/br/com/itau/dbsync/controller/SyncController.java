package br.com.itau.dbsync.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import br.com.itau.dbsync.model.EntityStream;
import br.com.itau.dbsync.model.Response;
import br.com.itau.dbsync.service.SyncService;

@RestController
public class SyncController {

	@Autowired
	private SyncService service;

	@PostMapping("/sync")
	private ResponseEntity<Response> sync(@RequestBody EntityStream stream) {
		service.sync(stream);
		return ResponseEntity.ok(Response.builder().message("Success").build());
	}
}