#!/bin/bash
set -e
set -x

BRANCH=$1 # master
IMAGE=$2

cat deploy.tpl.yaml \
    | sed -e "s|<BRANCH>|$BRANCH|g" \
    | sed -e "s|<IMAGE>|$IMAGE|g" \
    | sed -e "s|<GIT_COMMIT>|$GIT_COMMIT|g" \
    | sed -e "s|<DB_HOST>|$DB_HOST|g" \
    | sed -e "s|<DB_PORT>|$DB_PORT|g" \
    | sed -e "s|<DB_DATABASE>|$DB_DATABASE|g" \
    | sed -e "s|<DB_USER>|$DB_USER|g" \
    | sed -e "s|<DB_PASSWORD>|$DB_PASSWORD|g" \
    | sed -e "s|<PORT>|$PORT|g" \
    | sed -e "s|<DB_SHARED_BUFFERS>|$DB_SHARED_BUFFERS|g" \
    | sed -e "s|<API_OPS>|$API_OPS|g" \
    > deploy.yaml

cat deploy.yaml

kubectl apply -f deploy.yaml

echo "$IMAGE was deployed as npi"
