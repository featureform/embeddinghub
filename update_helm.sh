#!/bin/bash
set -e
gsutil cp -r gs://featureform-helm ./
helm package ./charts/featureform -d featureform-helm --app-version $1 --version $1
helm repo index ./featureform-helm
gsutil cp ./featureform-helm/* gs://featureform-helm
