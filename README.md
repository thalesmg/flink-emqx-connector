# Flink EMQX Connector

## Building

```sh
mvn clean package
# without running tests
mvn clean package -DskipTests
```

## Testing

```sh
mvn clean test
# Run specific test suite
mvn clean -Dtest=EMQXSourceIntegrationTests test
# Run specific test case
mvn clean -Dtest=EMQXSourceIntegrationTests#recoverAfterFailure test
```
