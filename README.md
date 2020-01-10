## Stream Wikipedia entries using Apache Flink

Stand alone Flink job which reads Wikipedia entries and aggregrates number of bytes edited,
per user, aggregated over a 5 second tumbling window. Based on the Flink tutorial, converted to Scala.

As an addition, every 5 seconds the contents of the current window are written to a 
MQTT topic.