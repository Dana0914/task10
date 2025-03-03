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
import org.json.JSONObject;
import org.json.JSONArray;
import java.util.List;
import java.util.ArrayList;



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
	private static final String WEATHER_API_URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52" +
			"&longitude=13.41" +
			"&current=temperature_2m,wind_speed_10m" +
			"&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m";

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
			String weatherData = fetchWeatherData();
			JSONObject parsedData = new JSONObject(weatherData);
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
			JSONObject jsonObject = new JSONObject(response.body().string());

			// Extract only required fields
			JSONObject filteredData = new JSONObject();
			filteredData.put("id", UUID.randomUUID().toString());
			filteredData.put("latitude", jsonObject.getDouble("latitude"));
			filteredData.put("longitude", jsonObject.getDouble("longitude"));
			filteredData.put("elevation", jsonObject.getDouble("elevation"));
			filteredData.put("generationtime_ms", jsonObject.getDouble("generationtime_ms"));
			filteredData.put("timezone", jsonObject.getString("timezone"));
			filteredData.put("timezone_abbreviation", jsonObject.getString("timezone_abbreviation"));
			filteredData.put("utc_offset_seconds", jsonObject.getInt("utc_offset_seconds"));

			// Extract hourly data
			JSONObject hourly = jsonObject.getJSONObject("hourly");
			JSONObject hourlyUnits = jsonObject.getJSONObject("hourly_units");

			JSONObject hourlyData = new JSONObject();
			hourlyData.put("time", hourly.getJSONArray("time"));
			hourlyData.put("temperature_2m", hourly.getJSONArray("temperature_2m"));

			// Include hourly units
			JSONObject hourlyUnitsData = new JSONObject();
			hourlyUnitsData.put("time", hourlyUnits.getString("time"));
			hourlyUnitsData.put("temperature_2m", hourlyUnits.getString("temperature_2m"));

			// Add extracted data to forecast
			JSONObject forecast = new JSONObject();
			forecast.put("latitude", filteredData.getDouble("latitude"));
			forecast.put("longitude", filteredData.getDouble("longitude"));
			forecast.put("generationtime_ms", filteredData.getDouble("generationtime_ms"));
			forecast.put("utc_offset_seconds", filteredData.getInt("utc_offset_seconds"));
			forecast.put("timezone", filteredData.getString("timezone"));
			forecast.put("timezone_abbreviation", filteredData.getString("timezone_abbreviation"));
			forecast.put("elevation", filteredData.getDouble("elevation"));
			forecast.put("hourly_units", hourlyUnitsData);
			forecast.put("hourly", hourlyData);

			// Create final object
			JSONObject finalData = new JSONObject();
			finalData.put("id", filteredData.getString("id"));
			finalData.put("forecast", forecast);

			return finalData.toString();
		}

	}
	private void storeWeatherData(JSONObject weatherData, Context context) {
		context.getLogger().log("Weather Data to Store: " + weatherData.toString());

		Item item = new Item()
				.withPrimaryKey("id", weatherData.getString("id"))
				.withMap("forecast", jsonToMap(weatherData.getJSONObject("forecast")));

		table.putItem(item);
		context.getLogger().log("Table: " + table);
	}

	private Map<String, Object> jsonToMap(JSONObject json) {
		Map<String, Object> map = new HashMap<>();
		for (String key : json.keySet()) {
			Object value = json.get(key);
			if (value instanceof JSONObject) {
				map.put(key, jsonToMap((JSONObject) value)); // Recursively convert
			} else if (value instanceof JSONArray) {
				map.put(key, jsonArrayToList((JSONArray) value)); // Convert JSONArray
			} else {
				map.put(key, value);
			}
		}
		return map;
	}

	private List<Object> jsonArrayToList(JSONArray jsonArray) {
		List<Object> list = new ArrayList<>();
		for (int i = 0; i < jsonArray.length(); i++) {
			Object value = jsonArray.get(i);
			if (value instanceof JSONObject) {
				list.add(jsonToMap((JSONObject) value));
			} else if (value instanceof JSONArray) {
				list.add(jsonArrayToList((JSONArray) value));
			} else {
				list.add(value);
			}
		}
		return list;
	}
}
