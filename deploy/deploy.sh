#!/bin/bash
set -e
set -x

BRANCH=$1 # master
IMAGE=$2

cat deploy.tpl.yaml \
    | sed "s|<BRANCH>|$BRANCH|g" \
    | sed "s|<IMAGE>|$IMAGE|g" \
    | sed "s|<GIT_COMMIT>|$GIT_COMMIT|g" \
    | sed "s|<DB_HOST>|$DB_HOST|g" \
    | sed "s|<DB_PORT>|$DB_PORT|g" \
    | sed "s|<DB_DATABASE>|$DB_DATABASE|g" \
    | sed "s|<DB_USER>|$DB_USER|g" \
    | sed "s|<DB_PASSWORD>|$DB_PASSWORD|g" \
    | sed "s|<PORT>|$PORT|g" \
    | sed "s|<FHIRTERM_BASE>|$FHIRTERM_BASE|g" \
    | sed "s|<BEAT_TIMEOUT>|$BEAT_TIMEOUT|g" \
    > deploy.yaml

cat deploy.yaml

kubectl apply -f deploy.yaml

echo "$IMAGE was deployed as npi"
