#!/bin/bash
set -e
set -x

BRANCH=$1 # master
IMAGE=$2

cat deploy.tpl.yaml \
    | sed 's;<BRANCH>;'"$BRANCH"';g' \
    | sed 's;<IMAGE>;'"$IMAGE"';g' \
    > deploy.yaml

cat deploy.yaml

kubectl apply -f deploy.yaml

echo "$IMAGE was deployed as npi"
