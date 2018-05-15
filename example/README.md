# WARNING: This is Alpha software and not intended for use until a stable release.

## Running a single node of M3Aggregator within a Docker container

### Prerequisites

Docker v1.9+

### Build Docker image with HEAD of master:

`docker build -t m3dbnode:latest .`

### Build Docker image with specific git comment:

`docker build --build-arg GITSHA=$(git rev-parse head) -t m3dbnode:latest .`
