# Copyright © 2024  Brrr Authors
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, version 3 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

# These are all the pytest tests, with the required database dependencies spun
# up.

{ self
, pkgs
, integrationCommon
, lib
,
}:

pkgs.testers.runNixOSTest {
  inherit (integrationCommon) globalTimeout;
  name = "brrr-integration";
  nodes = {
    inherit (integrationCommon) datastores;
    tester =
      { pkgs, ... }:
      let
        run-integration-test = pkgs.writeShellScriptBin "brrr-test-integration" ''
          set -euo pipefail
          ${self.packages.${pkgs.system}.brrr-venv-test}/bin/pytest ${self.packages.${pkgs.system}.brrr.src}
        '';
      in
      {
        systemd.services.brrr-test-integration =
          {
            serviceConfig = {
              Type = "oneshot";
              Restart = "no";
              RemainAfterExit = "yes";
              ExecStart = lib.getExe run-integration-test;
            };
            environment = integrationCommon.runtimeEnv;
            enable = true;
            wants = [ "multi-user.target" ];
          };
      };
  };

  testScript =
    integrationCommon.testScript
    + ''
      tester.systemctl("start --no-block brrr-test-integration.service")
      tester.wait_for_unit("brrr-test-integration.service")
    '';
}
