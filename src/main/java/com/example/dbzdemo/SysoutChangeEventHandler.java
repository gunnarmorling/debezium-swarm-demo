/**
 *  Copyright 2018 Gunnar Morling (http://www.gunnarmorling.de/). See
 *  the copyright.txt file in the distribution for a full listing of all
 *  contributors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.example.dbzdemo;

import java.io.StringReader;

import javax.enterprise.context.ApplicationScoped;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;

import org.aerogear.kafka.cdi.annotation.Consumer;
import org.aerogear.kafka.cdi.annotation.KafkaConfig;

@KafkaConfig(bootstrapServers = "localhost:9092")
@ApplicationScoped
public class SysoutChangeEventHandler {

    @Consumer(topics = "dbserver1_inventory_Hike_json", groupId = "sysout-handler")
    public void receiver(final String key, final String value) {
        JsonReader reader = Json.createReader( new StringReader( value ) );

        JsonValue payload = reader.readObject().get( "payload" );
        String before = payload instanceof JsonObject ? ( (JsonObject)payload ).get( "before" ).toString() : "";
        String after = payload instanceof JsonObject ? ( (JsonObject)payload ).get( "after" ).toString() : "";

        String message = "### Received change event\n# Before: " + before + "\n# After: " + after;

        System.out.println( message );
    }
}
