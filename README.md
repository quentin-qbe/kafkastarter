# Kafka Starter

POC autour de l'écosystème Kafka permettant de publier des événements métier,
de les agréger à l'aide de Kafka Streams afin de construire un Product Master,
de générer des exports XML/CSV et de gérer des alertes de stock avec
historisation des actions utilisateur en base MySQL.


## Quick Start

```bash
docker compose -f docker/docker-compose.yml up -d --build

chmod +x scripts/publish.sh
./scripts/publish.sh
```

Interfaces :

- Swagger : http://localhost:8080/kafkastarter/swagger-ui/index.html
- Kafka UI : http://localhost:8090
- Prometheus : http://localhost:9090
- Grafana : http://localhost:3000
- SonarQube : http://localhost:9000


## Technologies

- Java 25
- Spring Boot
- JUnit 5 / Mockito
- Apache Kafka / Kafka Streams
- Avro
- Confluent Schema Registry
- MapStruct
- Docker / Docker Compose
- Swagger / OpenAPI
- TestDriver / TestContainers
- Prometheus / Grafana

---

## Fonctionnalités implémentées

### Kafka

- Création automatique des topics Kafka
- Production de messages via API REST + script shell automatique (alerte de stock faible créée automatiquement)
- Consommation de messages Kafka
- Agrégation de flux avec Kafka Streams
- Production de documents XML et CSV
- Queryable State Store Kafka Streams pour les alertes de stock faible
- Insertion de l'historique des actions sur les alertes de stock dans une base MySQL
- Exactly Once Processing avec Kafka Streams (`exactly_once_v2`)
- Idempotence des événements d'actions grâce à un `eventId` unique
- Dead letter queue

### Base de données MySQL (Historisation des actions)

Le projet inclut une base MySQL permettant d'historiser les actions réalisées sur les alertes de stock.

Exemples :

- acquittement d'une alerte (ACK)
- réouverture d'une alerte (REOPEN)

Table principale : stock_alert_action_history (id, event_id, product_id, action, user, processed_at, created_at)

Le champ `event_id` est soumis à une contrainte d'unicité et
permet de détecter les réémissions éventuelles d'un même
événement métier.

Flux de traitement :

```text
topic-stock-alert-actions
            ↓
      Kafka Streams
            ↓
      Traitement OK
            ↓
           MySQL

            OU

      Traitement KO
            ↓
stock-alert-actions-dead-letter-queue
```

### Exactly Once Processing

Le projet utilise la garantie Kafka Streams `exactly_once_v2`
afin de sécuriser le traitement des événements en cas de crash,
redémarrage ou rejeu Kafka.

Les actions sur les alertes de stock possèdent un identifiant
fonctionnel unique `eventId`.

La table `stock_alert_action_history` applique une contrainte
d'unicité sur ce champ afin d'empêcher toute insertion en double.

Cette approche garantit qu'un même événement métier identifié
par son `eventId` ne peut être persisté qu'une seule fois en base
de données, même en cas de rejeu Kafka, redémarrage de
l'application ou nouvel essai de traitement.

### Dead Letter Queue (DLQ)

Le projet implémente une Dead Letter Queue permettant
de conserver les événements dont le traitement a échoué.

Topic utilisé :

```text
stock-alert-actions-dead-letter-queue
```

Les événements sont redirigés vers ce topic lorsqu'une
erreur survient pendant le traitement Kafka Streams.

Exemples :

- indisponibilité de MySQL (ex : couper le container mysql et appeler l'API PATCH /stock-alerts)
- erreur de persistance
- exception métier
- erreur de traitement

Les événements rejetés restent disponibles dans Kafka
et peuvent être analysés ou rejoués ultérieurement.

### Avro

- Sérialisation Avro
- Schema Registry Confluent
- Gestion des types logiques (`Instant`)

### Mapping

- Mapping DTO ↔ Avro avec MapStruct

### API

- Swagger UI
- Endpoints REST pour publier les événements et traiter l'historique des actions sur les alertes de stock faible

### Export

- Agrégation des données produit (Génération XML et CSV)

### Tests unitaires
- JUnit 5
- Mockito

---

## Architecture

Composants Docker :

```text
                     +---------+
                     | Grafana |
                     +----+----+
                          |
                          v
                     +----+----+
                     |Prometheus|
                     +----+----+
                          |
                          v
+------------+      +-----+-----+      +-------+
| Kafka      |<---->|Kafkastarter|<--->| MySQL |
+------+-----+      +-----------+      +-------+
       |
       v
+--------------+
|Schema Registry|
+--------------+
```

Projet :

```text
                                 +----------------------+
 Product ----------------------->|                      |
 Price ------------------------->|                      |
 Stock ------------------------->| ProductMasterTopology|
 Supplier ---------------------->|                      |
 Marketing --------------------->|                      |
                                 +----------+-----------+
                                            |
                                            v

                                topic-master-product
                                            |
             +------------------------------+-----------------------------+
             |                                                            |
             v                                                            v

      XML / CSV Export                                        StockAlertTopology
                                                                     |
                                                                     v

                                                            topic-stock-alert
                                                                     |
                                                                     v

                                                            Kafka State Store
                                                            (stock-alert-store)
                                                                     |
                                                                     v

                                           REST API /stock-alerts (findAll / findById)

---------------------------------------------------------------------------------------

Utilisateur
      |
      v

POST /stock-alert-actions
      |
      v

StockAlertActionController
      |
      v

Kafka Producer
      |
      v

topic-stock-alert-actions
      |
      v

StockAlertActionTopology
      |
      +------------------------+
      |                        |
      v                        v

Traitement OK           Traitement KO
      |                        |
      v                        v

StockAlertActionService   stock-alert-actions-dead-letter-queue
      |
      v

stock_alert_action_history (MySQL)
```

Topics utilisés :

```text
topic-product
topic-price
topic-stock
topic-supplier
topic-marketing
topic-master-product (topic d'aggrégation)
topic-stock-alert
topic-stock-alert-actions
stock-alert-actions-dead-letter-queue
```

Store utilisé :

```text
stock-alert-store
```
Topologies Kafka Streams utilisées :

```text
ProductMasterTopology
StockAlertTopology
StockAlertActionTopology
```

---

## Démarrage

### Nettoyage de l'environnement

```bash
docker compose -f docker/docker-compose.yml down -v
```

### Rebuild complet sans cache

```bash
docker compose -f docker/docker-compose.yml build --no-cache
```

### Lancement des containers

```bash
docker compose -f docker/docker-compose.yml up -d --force-recreate
```

---

## Interfaces

### Swagger

```text
http://localhost:8080/kafkastarter/swagger-ui/index.html
```

### Kafka UI

```text
http://localhost:8090
```

---

## Validation fonctionnelle

Tests réalisés avec succès :

- Swagger accessible / Application démarrée correctement
- Kafka UI accessible
- Publication d'un message de chaque type (stock faible automatique)
- Agrégation Kafka Streams fonctionnelle
- Génération du Product Master
- Exports XML et CSV générés dans `docker/exports`
- Tests unitaires (> 80% coverage)
- Tests Kafka Streams avec TopologyTestDriver
- Alertes de stock faible avec State store Kafka Streams
- API REST de consultation sur Queryable State Store existant (findAll et findById)
- Insertion de l'historique des actions dans une BDD MySQL (table `stock_alert_action_history`) avec Spring Data JPA
- Publication d'actions d'alerte dans Kafka
- Traitement des actions via Kafka Streams
- Persistance des actions en base MySQL
- Intégration SonarQube
- Tests d'intégration avec Testcontainers
- Prometheus
- Grafana
- Validation de l'idempotence des actions grâce à `eventId`
- Exactly Once Processing activé sur Kafka Streams (`exactly_once_v2`)
- Validation du routage vers `stock-alert-actions-dead-letter-queue` en cas d'erreur de traitement
- Vérification de la présence des événements rejetés dans Kafka UI

---

## TODO

### Priorité haute

Objectif : faire évoluer le POC vers une architecture proche de la production.
- [ ] Ajouter des tests de performance
- [ ] Compléter ce fichier README.md

### Évolutions prévues pour une application Kafka Streams complète
- [ ] Spring Security + JWT
- [ ] Audit des événements Kafka
- [ ] Sécurisation Kafka (SASL/SCRAM)

---

## Structure des événements

### Product

Informations produit (la clé d'aggrégation est le productId, champ disponible dans tous les objets) :

```json
{
  "productId": "P001",
  "sku": "KB001",
  "name": "Keyboard",
  "brand": "Microsoft",
  "category": "Accessories",
  "active": true,
  "createdAt": "2026-08-27T08:00:00Z"
}
```

### Price

Informations tarifaires :

```json
{
  "productId": "P001",
  "amount": 49.99,
  "currency": "EUR",
  "lastUpdatedAt": "2026-08-27T08:00:00Z"
}
```

### Stock

Informations de stock :

```json
{
  "productId": "P001",
  "availableQuantity": 10,
  "reservedQuantity": 10,
  "warehouseCode": "WH001",
  "lastUpdatedAt": "2026-09-01T09:00:00Z"
}
```

### Supplier

Informations fournisseur :

```json
{
  "productId": "P001",
  "supplierId": "SUP001",
  "supplierName": "TechSupplier",
  "country": "France",
  "lastUpdatedAt": "2026-08-27T08:00:00Z"
}
```

### Marketing

Informations marketing :

```json
{
  "productId": "P001",
  "shortDescription": "Wireless Keyboard",
  "longDescription": "Ergonomic wireless keyboard",
  "tags": [
    "office",
    "wireless"
  ],
  "lastUpdatedAt": "2026-08-27T08:00:00Z"
}
```


### ProductMaster

Evénement agrégé par Kafka Streams.

```json
{
  "productId": "P001",
  "sku": "KB001",
  "name": "Mechanical Keyboard",
  "brand": "Microsoft",
  "category": "Accessories",
  "active": true,
  "price": 49.99,
  "currency": "EUR",
  "availableStock": 10,
  "reservedStock": 2,
  "supplierId": "SUP001",
  "supplierName": "TechSupplier",
  "supplierCountry": "FR",
  "shortDescription": "Mechanical keyboard",
  "longDescription": "High quality mechanical keyboard with RGB lighting",
  "tags": [
    "keyboard",
    "gaming",
    "rgb"
  ],
  "alertThreshold": 20,
  "createdAt": "2026-08-29T10:00:00Z",
  "lastUpdatedAt": "2026-09-02T13:00:00Z"
}
```


### StockAlertAction

Action réalisée par un utilisateur sur une alerte de stock.

```json
{
  "eventId": "550e8400-e29b-41d4-a716-446655440000",
  "productId": "P001",
  "action": "ACKNOWLEDGE",
  "user": "user",
  "processedAt": "2026-09-02T10:00:00Z"
}
```


### StockAlert

Alerte générée automatiquement lorsqu'un stock passe sous le seuil défini.

```json
{
  "productId": "P001",
  "sku": "KB001",
  "currentStock": 5,
  "threshold": 10,
  "severity": "CRITICAL",
  "alertDate": "2026-09-02T10:00:00Z"
}
```

---

## IntelliJ

Pour lancer les commandes Docker depuis IntelliJ :

### Option 1 : Run Configurations

Créer des configurations de type :

```text
Docker Down
Docker Build
Docker Up
```

avec les commandes :

```bash
docker compose -f docker/docker-compose.yml down -v
```

```bash
docker compose -f docker/docker-compose.yml build --no-cache
```

```bash
docker compose -f docker/docker-compose.yml up -d --force-recreate
```


## Exécution rapide des jeux de données

### Publication des messages de test

Publication automatique d'un scénario métier complet.

Le script publie :

- un Product
- un Price
- un Stock
- un Supplier
- un Marketing

Le stock initial est volontairement inférieur au seuil d'alerte 
afin de générer automatiquement une alerte de stock faible exploitable immédiatement via l'API REST.

```bash
chmod +x scripts/publish.sh
./scripts/publish.sh
```

Vérifier que les messages ont bien été publiés à l'aide de Kafka UI (http://localhost:8090/ui/clusters/local/all-topics?perPage=25).

### Tester l'historisation des actions 

Envoyer une action d'acquittement : 

```json
{
  "eventId": "550e8400-e29b-41d4-a716-446655440000",
  "productId": "P001",
  "action": "ACKNOWLEDGE",
  "user": "username",
  "processedAt": "2026-09-02T10:00:00Z"
}
```

Une nouvelle ligne doit être créée en BDD dans la table stock_alert_action_history.

### Tester la Dead Letter Queue

1. Démarrer Kafka, MySQL et l'application.
2. Arrêter MySQL.
3. Envoyer une action d'alerte via l'API REST.
4. Vérifier l'échec de persistance dans les logs.
5. Vérifier la présence de l'événement dans :

```text
stock-alert-actions-dead-letter-queue
```

### Désactivation de l'alerte de stock faible

Appeler le service POST /stocks avec le payload suivant pour désactiver l'alerte de stock faible (availableQuantity > 20) :

```json
{
  "productId": "P001",
  "availableQuantity": 50,
  "reservedQuantity": 0,
  "warehouseCode": "WH001",
  "lastUpdatedAt": "2026-09-01T07:30:00Z"
}
```

---

## SonarQube
### Installation

- Url : localhost:9000 (connexion avec admin/admin)
- Créer un projet Sonar : (Projects → Create à local project)
- Project Key : kafkastarter et Display Name : kafkastarter
- Générer un Token : My Account > Security > Generate Token (variable appelée <TOKEN> dans la commande ci-dessous)
- Lancer la compilation, les tests (avec rapport Jacoco) et l'analyse SonarQube en local avec la commande suivante :

Exemple :
```bash
mvn clean verify org.sonarsource.scanner.maven:sonar-maven-plugin:sonar \
  -Dsonar.projectKey=kafkastarter \
  -Dsonar.projectName='kafkastarter' \
  -Dsonar.host.url=http://localhost:9000 \
  -Dsonar.scm.disabled=true \
  -Dsonar.token=<TOKEN>
```

Le rapport est disponible à l'adresse : http://localhost:9000/dashboard?id=kafkastarter

---

## Monitoring

### Prometheus

#### Accès

- Métriques applicatives :
    - http://localhost:8080/kafkastarter/actuator/prometheus
- Interface Prometheus :
    - http://localhost:9090/query

#### Vérification des métriques exposées

Lister les métriques personnalisées exposées par l'application :

```bash
curl http://localhost:8080/kafkastarter/actuator/prometheus | grep kafkastarter
```

#### Requêtes PromQL utiles

##### Product Master

Nombre total de Product Master créés :

```promql
kafkastarter_productmaster_created_total
```

Résultat attendu après exécution du script `publish.sh` :

```text
5
```

Nombre de Product Master créés sur les 15 dernières minutes :

```promql
increase(kafkastarter_productmaster_created_total[15m])
```

Détection d'une absence de création de Product Master sur les 15 dernières minutes :

```promql
increase(kafkastarter_productmaster_created_total[15m]) == 0
```

##### Stock Alert

Nombre total d'alertes créées :

```promql
kafkastarter_stockalert_created_total
```

Nombre d'alertes créées par niveau de sévérité sur les 15 dernières minutes :

```promql
sum by(severity) (
  increase(kafkastarter_stockalert_created_total[15m])
)
```

Nombre total d'événements envoyés en DLQ :

```promql
kafkastarter_stockalertaction_dead_letter_queue_total
```

##### Stock Alert Action

Nombre total d'actions traitées avec succès :

```promql
kafkastarter_stockalertaction_creation_success_total
```

Nombre total d'actions traitées en erreur :

```promql
kafkastarter_stockalertaction_creation_failed_total
```

Nombre d'actions traitées avec succès sur les 15 dernières minutes :

```promql
increase(kafkastarter_stockalertaction_creation_success_total[15m])
```

Nombre d'actions traitées en erreur sur les 15 dernières minutes :

```promql
increase(kafkastarter_stockalertaction_creation_failed_total[15m])
```

Taux de succès des traitements :

```promql
100 *
sum(rate(kafkastarter_stockalertaction_creation_success_total[5m]))
/
(
  sum(rate(kafkastarter_stockalertaction_creation_success_total[5m]))
  +
  sum(rate(kafkastarter_stockalertaction_creation_failed_total[5m]))
)
```

Taux d'erreur des traitements :

```promql
100 *
sum(rate(kafkastarter_stockalertaction_creation_failed_total[5m]))
/
(
  sum(rate(kafkastarter_stockalertaction_creation_success_total[5m]))
  +
  sum(rate(kafkastarter_stockalertaction_creation_failed_total[5m]))
)
```

##### Temps de traitement

Temps moyen de traitement d'un Product Master :

```promql
rate(kafkastarter_productmaster_pipeline_seconds_sum[5m])
/
rate(kafkastarter_productmaster_pipeline_seconds_count[5m])
```

Temps moyen de traitement d'une alerte de stock :

```promql
rate(kafkastarter_stockalert_pipeline_seconds_sum[5m])
/
rate(kafkastarter_stockalert_pipeline_seconds_count[5m])
```

Temps moyen de traitement d'une action sur alerte :

```promql
rate(kafkastarter_stockalertaction_pipeline_seconds_sum[5m])
/
rate(kafkastarter_stockalertaction_pipeline_seconds_count[5m])
```

P95 du temps de traitement Product Master :

```promql
kafkastarter_productmaster_pipeline_seconds{quantile="0.95"}
```

P95 du temps de traitement Stock Alert :

```promql
kafkastarter_stockalert_pipeline_seconds{quantile="0.95"}
```

P95 du temps de traitement Stock Alert Action :

```promql
kafkastarter_stockalertaction_pipeline_seconds{quantile="0.95"}
```

---

### Grafana

#### Accès

- Grafana : http://localhost:3000
- Prometheus : http://localhost:9090
- Métriques applicatives : http://localhost:8080/actuator/prometheus

#### Configuration de Prometheus comme source de données

Dans Grafana :

1. Aller dans **Connections > Data Sources**
2. Ajouter une source **Prometheus**
3. URL :

```text
http://prometheus:9090
```

4. Cliquer sur **Save & Test**

---

## Dashboard Kafka Starter

### Product Master

#### Nombre total de Product Master créés

Visualisation : **Stat**

```promql
kafkastarter_productmaster_created_total
```

#### Nombre de Product Master créés sur les 15 dernières minutes

Visualisation : **Time Series**

```promql
increase(kafkastarter_productmaster_created_total[15m])
```

#### Détection d'une absence de création de Product Master

Visualisation : **Stat**

```promql
increase(kafkastarter_productmaster_created_total[15m]) == 0
```

---

### Stock Alert

#### Nombre total d'alertes créées

Visualisation : **Stat**

```promql
kafkastarter_stockalert_created_total
```

#### Répartition des alertes par niveau de sévérité

Visualisation : **Pie Chart**

```promql
sum by(severity) (
  increase(kafkastarter_stockalert_created_total[15m])
)
```

---

### Stock Alert Action

#### Nombre total de traitements réussis

Visualisation : **Stat**

```promql
kafkastarter_stockalertaction_creation_success_total
```

#### Nombre total de traitements en erreur

Visualisation : **Stat**

```promql
kafkastarter_stockalertaction_creation_failed_total
```

#### Nombre de traitements réussis sur les 15 dernières minutes

Visualisation : **Time Series**

```promql
increase(kafkastarter_stockalertaction_creation_success_total[15m])
```

#### Nombre de traitements en erreur sur les 15 dernières minutes

Visualisation : **Time Series**

```promql
increase(kafkastarter_stockalertaction_creation_failed_total[15m])
```

#### Taux de succès des traitements

Visualisation : **Gauge**

```promql
100 *
sum(rate(kafkastarter_stockalertaction_creation_success_total[5m]))
/
(
  sum(rate(kafkastarter_stockalertaction_creation_success_total[5m]))
  +
  sum(rate(kafkastarter_stockalertaction_creation_failed_total[5m]))
)
```

#### Taux d'erreur des traitements

Visualisation : **Gauge**

```promql
100 *
sum(rate(kafkastarter_stockalertaction_creation_failed_total[5m]))
/
(
  sum(rate(kafkastarter_stockalertaction_creation_success_total[5m]))
  +
  sum(rate(kafkastarter_stockalertaction_creation_failed_total[5m]))
)
```

Seuils recommandés :

- Vert : < 1 %
- Orange : ≥ 1 %
- Rouge : ≥ 5 %

---

### Temps de traitement

#### Temps moyen de traitement Product Master

Visualisation : **Time Series**

```promql
rate(kafkastarter_productmaster_pipeline_seconds_sum[5m])
/
rate(kafkastarter_productmaster_pipeline_seconds_count[5m])
```

#### Temps moyen de traitement Stock Alert

Visualisation : **Time Series**

```promql
rate(kafkastarter_stockalert_pipeline_seconds_sum[5m])
/
rate(kafkastarter_stockalert_pipeline_seconds_count[5m])
```

#### Temps moyen de traitement Stock Alert Action

Visualisation : **Time Series**

```promql
rate(kafkastarter_stockalertaction_pipeline_seconds_sum[5m])
/
rate(kafkastarter_stockalertaction_pipeline_seconds_count[5m])
```

---

## Dashboard recommandé

Créer un dashboard composé des panneaux suivants :

1. Product Master créés (Stat)
2. Product Master créés sur les 15 dernières minutes (Time Series)
3. Alertes créées par sévérité (Pie Chart)
4. Succès traitements (Stat)
5. Échecs traitements (Stat)
6. Taux d'erreur (Gauge)
7. Temps moyen Product Master (Time Series)
8. Temps moyen Stock Alert (Time Series)
9. Temps moyen Stock Alert Action (Time Series)

Ce dashboard permet de suivre à la fois :
- le volume des messages traités ;
- le nombre d'alertes générées ;
- les succès et échecs du traitement ;
- les performances du pipeline Kafka Streams ;
- les éventuelles régressions ou indisponibilités du système.

---

## Cas d'usage métier

1. Réception des données produit
2. Construction du Product Master
3. Détection automatique des stocks faibles
4. Consultation des alertes actives
5. Traitement utilisateur d'une alerte
6. Historisation en base MySQL

## Statut

Projet actuellement au stade :

```text
POC avancé validant les principaux patterns Kafka Streams
(agrégation, state store, queryable state, exactly once processing,
idempotence, dead letter queue et historisation en base)
```

Objectif :

```text
Architecture Kafka Streams proche d'un environnement de production
```