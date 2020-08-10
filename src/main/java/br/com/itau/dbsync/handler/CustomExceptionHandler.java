package br.com.itau.dbsync.handler;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import br.com.itau.dbsync.model.Response;
import lombok.extern.slf4j.Slf4j;

@ControllerAdvice
@Slf4j
public class CustomExceptionHandler {

	@ExceptionHandler(Exception.class)
	public ResponseEntity<Response> handleException(HttpServletRequest req, Exception e) {
		log.error(e.getMessage(), e);
		Response response = new Response(e.getMessage());

		return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
	}

	@ExceptionHandler(MethodArgumentNotValidException.class)
	public ResponseEntity<Response> handleValidationExceptions(MethodArgumentNotValidException ex)
			throws JsonProcessingException {
		Map<String, String> errors = new HashMap<>();
		ex.getBindingResult().getAllErrors().forEach((error) -> {
			String fieldName = ((FieldError) error).getField();
			String errorMessage = error.getDefaultMessage();
			errors.put(fieldName, errorMessage);
		});
		ObjectMapper mapper = new ObjectMapper();

		Response response = new Response(mapper.writeValueAsString(errors));
		return ResponseEntity.badRequest().body(response);
	}

}