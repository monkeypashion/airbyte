/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.serde;

import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.version.AirbyteVersion;
import lombok.Getter;

public class AirbyteMessageGenericDeserializer<T> implements AirbyteMessageDeserializer<T> {

  @Getter
  final AirbyteVersion targetVersion;
  final Class<T> typeClass;

  public AirbyteMessageGenericDeserializer(final AirbyteVersion targetVersion, final Class<T> typeClass) {
    this.targetVersion = targetVersion;
    this.typeClass = typeClass;
  }

  @Override
  public T deserialize(String json) {
    return Jsons.deserialize(json, typeClass);
  }

}
