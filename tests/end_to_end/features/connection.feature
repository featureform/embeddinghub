#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

Feature: Connections

  @wip
  Scenario: Failed Connection
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:1234"
    And I create a user
    Then An exception that "contains" "<exception>" should be raised
      """
      Please check if your FEATUREFORM_HOST and FEATUREFORM_CERT environment variables are set correctly or are explicitly set in the client or command line.
      Details: failed to connect to all addresses; last error: UNKNOWN: ipv4:127.0.0.1:1234: Failed to connect to remote host: Connection refused
      """