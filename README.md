# flink-backup

```bash
mvn clean package -Pbuild-jar
```

```bash
flink run -p 10 -c master2017.flink.VehicleTelematics target/$JAR_FILE $INPUT_FILE $OUTPUT_FOLDER
```

[traffic.csv](http://lsd11.ls.fi.upm.es/traffic-3xways-new.7z)

[flink-1.3.2-bin-hadoop27-scala_2.11.tgz](https://archive.apache.org/dist/flink/flink-1.3.2/)

[flink-best-practices](https://ci.apache.org/projects/flink/flink-docs-release-1.6/dev/best_practices.html)
