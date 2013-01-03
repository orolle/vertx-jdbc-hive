package org.muhaaa.vertx.hive;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.deploy.Verticle;

public class HiveVertx extends Verticle  {
	public final static String ADDRESS = "vertx.jdbc.hive";

	@Override
	public void start() throws Exception {

		JsonObject jdbc = new JsonObject()
		.putString("driver", "org.apache.hadoop.hive.jdbc.HiveDriver")
		.putString("url", "jdbc:hive://localhost:10000/default")
		.putString("address", ADDRESS)
		.putNumber("maxpool", 6);

		
		
		this.container.deployVerticle("com.bloidonia.vertx.mods.JdbcBusMod", jdbc, 1, new Handler<String>() {
			public void handle(String id) {
				doTest();
			}
		});
	}

	private void doTest() {
		vertx.setPeriodic(1000, new Handler<Long>() {
			
			@Override
			public void handle(Long event) {
				vertx.eventBus().send(ADDRESS, new JsonObject()
				.putString("action", "select")
				.putString("stmt", "select * from comments"),
				new Handler<Message<JsonObject>>() {
						public void handle(Message<JsonObject> arg0) {
							System.out.println(arg0.body.toString());
						}
					}	
				);
			}
		});
	}

}
