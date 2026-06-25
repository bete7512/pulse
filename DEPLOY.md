# Deploy

pulse ships as a single container image (`cmd/pulsed`, the gRPC server on `:50051`).
The image is built and published by [`.github/workflows/ci.yml`](.github/workflows/ci.yml)
**only after the test job passes**.

## Required GitHub secrets

Set these in **Settings → Secrets and variables → Actions**:

| Secret | Description |
|---|---|
| `DOCKERHUB_USERNAME` | Docker Hub user or org (also the image namespace) |
| `DOCKERHUB_TOKEN` | Docker Hub **access token** (Account Settings → Security), not your password |

## Image & tags

Published to `docker.io/<DOCKERHUB_USERNAME>/pulse`. Tags (via `docker/metadata-action`):

- `latest` — on pushes to the default branch (`main`)
- `sha-<gitsha>` — immutable, every published build
- `X.Y.Z`, `X.Y` — on `v*` git tags (e.g. tagging `v1.2.0`)

Images are multi-arch: **linux/amd64** and **linux/arm64**.

## Configuration

Config is read from the **environment** (no `.env` needed in containers):

| Var | Description |
|---|---|
| `DB_HOST` | Postgres connection string, e.g. `postgres://user:pass@host:5432/pulse?sslmode=disable` |

The server runs schema migrations on startup, then serves gRPC on `:50051`.

## Run

```sh
docker run --rm \
  -e DB_HOST="postgres://user:pass@db:5432/pulse?sslmode=disable" \
  -p 50051:50051 \
  <DOCKERHUB_USERNAME>/pulse:latest
```

## Local image build

```sh
make docker-build DOCKERHUB_USERNAME=<you>   # builds <you>/pulse:dev
```
