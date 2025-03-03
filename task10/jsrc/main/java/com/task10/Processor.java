package com.task10;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.syndicate.deployment.annotations.lambda.LambdaUrlConfig;
import com.syndicate.deployment.model.lambda.url.AuthType;
import com.syndicate.deployment.model.lambda.url.InvokeMode;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.xray.AWSXRay;
import com.amazonaws.xray.handlers.TracingHandler;
import okhttp3.*;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.syndicate.deployment.model.TracingMode;



import java.io.IOException;
import java.util.UUID;
import java.util.HashMap;
import java.util.Map;

@LambdaHandler(
    lambdaName = "processor",
	roleName = "processor-role",
	isPublishVersion = true,
	aliasName = "${lambdas_alias_name}",
	logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@EnvironmentVariables(value = {
		@EnvironmentVariable(key = "target_table", value = "${target_table}")
})
@LambdaUrlConfig(
		authType = AuthType.NONE,
		invokeMode = InvokeMode.BUFFERED
)
public class Processor implements RequestHandler<Object, Map<String, Object>> {
	private final String WEATHER_API_URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52" +
			"&longitude=13.41" +
			"&current=temperature_2m,wind_speed_10m" +
			"&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m";
	private final ObjectMapper objectMapper = new ObjectMapper();

	private final String TABLE_NAME = System.getenv("target_table");
	private final OkHttpClient httpClient = new OkHttpClient();
	private final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
			.withRequestHandlers(new TracingHandler(AWSXRay.getGlobalRecorder())) // Enable X-Ray tracing
			.build();
	private final DynamoDB dynamoDB = new DynamoDB(client);
	private final Table table = dynamoDB.getTable(TABLE_NAME);

	@Override
	public Map<String, Object> handleRequest(Object request, Context context) {
		Map<String, Object> response = new HashMap<>();
		try {
			// Fetch weather data as JSON string
			String weatherData = fetchWeatherData();

			// Convert JSON string to Map
			Map<String, Object> parsedData = objectMapper.readValue(weatherData, new TypeReference<>() {});

			// Store parsed data
			storeWeatherData(parsedData, context);

			response.put("status", "success");
			response.put("message", "Weather data stored successfully!");
		} catch (Exception e) {
			context.getLogger().log("Error: " + e.getMessage());
			response.put("status", "error");
			response.put("message", "Failed to store weather data.");
		}
		return response;
	}
	private String fetchWeatherData() throws IOException {
		Request request = new Request.Builder()
				.url(WEATHER_API_URL)
				.get()
				.build();

		try (Response response = httpClient.newCall(request).execute()) {
			if (!response.isSuccessful()) {
				throw new IOException("Unexpected response: " + response);
			}

			// Parse response JSON
			Map<String, Object> jsonMap = objectMapper.readValue(response.body().string(), new TypeReference<>() {});

			// Extract required fields
			Map<String, Object> filteredData = new HashMap<>();
			filteredData.put("id", UUID.randomUUID().toString());
			filteredData.put("latitude", jsonMap.get("latitude"));
			filteredData.put("longitude", jsonMap.get("longitude"));
			filteredData.put("elevation", jsonMap.get("elevation"));
			filteredData.put("generationtime_ms", jsonMap.get("generationtime_ms"));
			filteredData.put("timezone", jsonMap.get("timezone"));
			filteredData.put("timezone_abbreviation", jsonMap.get("timezone_abbreviation"));
			filteredData.put("utc_offset_seconds", jsonMap.get("utc_offset_seconds"));

			// Extract hourly data
			Map<String, Object> hourly = (Map<String, Object>) jsonMap.get("hourly");
			Map<String, Object> hourlyUnits = (Map<String, Object>) jsonMap.get("hourly_units");

			Map<String, Object> hourlyData = new HashMap<>();
			hourlyData.put("time", hourly.get("time"));
			hourlyData.put("temperature_2m", hourly.get("temperature_2m"));

			// Include hourly units
			Map<String, Object> hourlyUnitsData = new HashMap<>();
			hourlyUnitsData.put("time", hourlyUnits.get("time"));
			hourlyUnitsData.put("temperature_2m", hourlyUnits.get("temperature_2m"));

			// Create forecast data
			Map<String, Object> forecast = new HashMap<>();
			forecast.putAll(filteredData);
			forecast.put("hourly_units", hourlyUnitsData);
			forecast.put("hourly", hourlyData);

			// Create final data object
			Map<String, Object> finalData = new HashMap<>();
			finalData.put("id", filteredData.get("id"));
			finalData.put("forecast", forecast);

			return objectMapper.writeValueAsString(finalData);
		}
	}

	private void storeWeatherData(Map<String, Object> weatherData, Context context) {
		context.getLogger().log("Weather Data to Store: " + weatherData.toString());

		Item item = new Item()
				.withPrimaryKey("id", (String) weatherData.get("id"))
				.withMap("forecast", (Map<String, Object>) weatherData.get("forecast"));

		try {
			table.putItem(item);
			context.getLogger().log("Successfully stored item: " + item.toJSONPretty());
		} catch (Exception e) {
			context.getLogger().log("DynamoDB putItem failed: " + e.getMessage());
			throw e;
		}
	}

}
