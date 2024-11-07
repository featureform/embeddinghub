#!/bin/bash

#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
# 
#  Copyright 2024 FeatureForm Inc.
# 

set -e
gsutil cp -r gs://featureform-helm ./
sed -i -e "s/0.0.0/$1/g" ./charts/featureform/values.yaml # Sets the default image tag value in the chart
helm package ./charts/featureform -d featureform-helm --app-version $1 --version $1
helm repo index ./featureform-helm
gsutil cp ./featureform-helm/* gs://featureform-helm
