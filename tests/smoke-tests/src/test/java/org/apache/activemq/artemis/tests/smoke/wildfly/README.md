# WildFly Client Smoke Test

This smoke test validates the integration between Apache ActiveMQ Artemis and WildFly application server.

## Test Architecture

```
External JMS Client
       |
       v
  InitialQueue (Artemis)
       |
       v
   FirstMDB (WildFly)
       |
       +---> SecondaryQueue (Artemis) ---> SecondMDB (WildFly)
       |
       +---> SecondaryTopic (Artemis) ---> TopicMDB (WildFly)
```

## Components

### Artemis Server
- Standalone Artemis broker running on port 61616
- Hosts queues: `InitialQueue`, `SecondaryQueue`
- Hosts topic: `SecondaryTopic`

### WildFly Server
- WildFly 31.0.1.Final
- Configured to use external Artemis broker (no embedded messaging)
- Deploys MDBs to consume from Artemis queues/topics

### Message-Driven Beans (MDBs)

1. **FirstMDB**
   - Listens on: `InitialQueue`
   - Behavior: Receives message, then produces to both `SecondaryQueue` and `SecondaryTopic`

2. **SecondMDB**
   - Listens on: `SecondaryQueue`
   - Behavior: Consumes and logs messages from SecondaryQueue

3. **TopicMDB**
   - Listens on: `SecondaryTopic`
   - Behavior: Consumes and logs messages from SecondaryTopic

## Test Flow

1. Test starts standalone Artemis server
2. Test downloads WildFly (if not cached)
3. Test configures WildFly to connect to external Artemis
4. Test starts WildFly server
5. Test deploys MDBs to WildFly
6. External JMS client sends message to `InitialQueue`
7. FirstMDB receives message from `InitialQueue`
8. FirstMDB sends messages to `SecondaryQueue` and `SecondaryTopic`
9. SecondMDB receives from `SecondaryQueue`
10. TopicMDB receives from `SecondaryTopic`
11. Test validates message flow completed successfully

## Running the Test

```bash
cd /home/clebertsuconic/work/apache/apache-artemis
mvn test -pl tests/smoke-tests -Dtest=WildflyClientSmokeTest
```

## Requirements

- Java 11+
- Maven 3.6+
- `unzip` command available on PATH
- Internet connection (for first-time WildFly download)
- ~500MB disk space for WildFly download

## Notes

- WildFly is downloaded once and cached in `target/wildfly-31.0.1.Final`
- Test creates temporary Artemis instance in `target/wildfly-artemis`
- Both servers are cleaned up after test completion
