# Déploiement Kubernetes — RetailPulse

Manifestes organisés **par fonction** dans le namespace `retailpulse`.

| Fichier | Ressource | Usage |
| --- | --- | --- |
| `00-namespace.yaml` | Namespace | isolation des charges de travail |
| `01-config.yaml` | ConfigMap + Secret | configuration / identifiants (ne pas committer de vrais secrets) |
| `02-postgres.yaml` | Service + StatefulSet + PVC | PostgreSQL (schéma chargé au 1er démarrage) |
| `03-kafka.yaml` | Service + StatefulSet + PVC | Kafka 4.0 KRaft (broker unique, RF=1) |
| `04-kafka-topics-job.yaml` | Job | création des topics (idempotent) |
| `05-collector-job.yaml` | Job | collecte DummyJSON -> Kafka |
| `06-reference-loader-job.yaml` | Job | Kafka -> PostgreSQL (upsert) |
| `07-producer.yaml` | Deployment | simulation continue |
| `08-spark.yaml` | PVC + Deployment | Spark Streaming (checkpoint persistant) |
| `09-dashboard.yaml` | Service + Deployment | dashboard Streamlit (NodePort 30085) |

## 1. Construire et versionner les images

```bash
# Adapter <registry>/<org> à votre environnement (Docker Hub, GHCR, ECR...)
export IMAGE_PREFIX="ghcr.io/votre-org"   # ex. docker.io/votre-user
export TAG="1.0.0"

docker build -t ${IMAGE_PREFIX}/retailpulse-collector:${TAG} ../collector
docker build -t ${IMAGE_PREFIX}/retailpulse-reference-loader:${TAG} ../reference_loader
docker build -t ${IMAGE_PREFIX}/retailpulse-producer:${TAG} ../producer
docker build -t ${IMAGE_PREFIX}/retailpulse-spark-streaming:${TAG} ../spark-streaming
docker build -t ${IMAGE_PREFIX}/retailpulse-dashboard:${TAG} ../dashboard

docker push ${IMAGE_PREFIX}/retailpulse-collector:${TAG}      # idem pour les autres
```

Puis remplacer les noms d'images (`retailpulse/...:1.0.0`) dans les manifestes
par `${IMAGE_PREFIX}/retailpulse-...:${TAG}`.

**Cluster local sans registre** (minikube / kind) :

```bash
# minikube : construire directement dans le daemon Docker du cluster
eval $(minikube docker-env) && docker build -t retailpulse/collector:1.0.0 ../collector
# kind : charger une image déjà construite
kind load docker-image retailpulse/collector:1.0.0 --name <cluster>
```

## 2. Générer la ConfigMap du schéma PostgreSQL

Le schéma reste versionné dans `postgresql/init/01_schema.sql` (source unique).

```bash
kubectl create configmap retailpulse-postgres-init \
  --from-file=01_schema.sql=../postgresql/init/01_schema.sql \
  -n retailpulse --dry-run=client -o yaml | kubectl apply -f -
```

## 3. Définir le Secret (jamais en clair dans Git)

```bash
kubectl create secret generic retailpulse-postgres \
  --from-literal=POSTGRES_USER=retail \
  --from-literal=POSTGRES_PASSWORD='vrai-mot-de-passe' \
  -n retailpulse --dry-run=client -o yaml | kubectl apply -f -
```

> Si vous appliquez `01-config.yaml` tel quel, le Secret de démonstration
> (`CHANGE-ME-retailpulse`) est utilisé : cohérent pour une démo, à remplacer
> pour toute utilisation réelle.

## 4. Ordre d'application

```bash
kubectl apply -f 00-namespace.yaml
kubectl apply -f 01-config.yaml          # ConfigMap (le Secret si non créé au §3)
kubectl apply -f 02-postgres.yaml
kubectl apply -f 03-kafka.yaml

# attendre PostgreSQL et Kafka prêts :
kubectl -n retailpulse wait --for=condition=ready pod -l app.kubernetes.io/name=postgres --timeout=180s
kubectl -n retailpulse wait --for=condition=ready pod -l app.kubernetes.io/name=kafka --timeout=180s

kubectl apply -f 04-kafka-topics-job.yaml
kubectl -n retailpulse wait --for=condition=complete job/kafka-topics-init --timeout=120s

kubectl apply -f 05-collector-job.yaml
kubectl -n retailpulse wait --for=condition=complete job/collector --timeout=300s

kubectl apply -f 06-reference-loader-job.yaml
kubectl -n retailpulse wait --for=condition=complete job/reference-loader --timeout=300s

kubectl apply -f 07-producer.yaml
kubectl apply -f 08-spark.yaml
kubectl apply -f 09-dashboard.yaml
```

## 5. Vérifications (pods, services, PVC, logs)

```bash
kubectl -n retailpulse get pods,svc,pvc,deploy,sts,jobs
kubectl -n retailpulse logs -f deployment/producer --tail=20
kubectl -n retailpulse logs -f deployment/spark --tail=50
kubectl -n retailpulse logs job/collector
```

Accès au tableau de bord :

```bash
# NodePort
minikube service dashboard -n retailpulse   # minikube
# ou : http://<node-ip>:30085
```

Accès PostgreSQL depuis l'hôte (debug) :

```bash
kubectl -n retailpulse port-forward svc/postgres 5432:5432
psql -h localhost -U retail -d retailpulse
```

## 6. Tests de résilience sur le cluster

Voir `docs/tests-et-resilience.md` — notamment :

```bash
# Supprimer un Pod applicatif : le contrôleur (Deployment) le recrée
kubectl -n retailpulse delete pod -l app.kubernetes.io/name=spark

# Interrompre PostgreSQL puis rétablir : observer les erreurs et la reprise
kubectl -n retailpulse scale statefulset postgres --replicas=0
kubectl -n retailpulse scale statefulset postgres --replicas=1
```
