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

{ pkgs, self }:

let
  mkTest =
    { name, bin }:
    pkgs.testers.runNixOSTest {
      inherit name;
      globalTimeout = 5 * 60;
      nodes = {
        datastores =
          { config, pkgs, ... }:
          {
            imports = [ self.nixosModules.dynamodb ];
            services.redis.servers.main = {
              enable = true;
              port = 6379;
              openFirewall = true;
              bind = null;
              logLevel = "debug";
              settings.protected-mode = "no";
            };
            services.dynamodb = {
              enable = true;
              openFirewall = true;
            };
          };
        tester =
          { pkgs, ... }:
          {
            systemd.services.brrr-test-integration = {
              serviceConfig = {
                Type = "oneshot";
                Restart = "no";
                RemainAfterExit = "yes";
                ExecStart = bin;
              };
              environment = {
                AWS_DEFAULT_REGION = "us-east-1";
                AWS_REGION = "us-east-1";
                AWS_ENDPOINT_URL = "http://datastores:8000";
                AWS_ACCESS_KEY_ID = "fake";
                AWS_SECRET_ACCESS_KEY = "fake";
                BRRR_TEST_REDIS_URL = "redis://datastores:6379";
              };
              enable = true;
              wants = [ "multi-user.target" ];
            };
          };
      };
      testScript = ''
        datastores.wait_for_unit("default.target")
        tester.wait_for_unit("default.target")
        tester.systemctl("start --no-block brrr-test-integration.service")
        tester.wait_for_unit("brrr-test-integration.service")
      '';
    };
in
{
  brrr-py-test-integration = mkTest {
    name = "brrr-py-test-integration";
    bin = pkgs.lib.getExe (
      pkgs.writeShellScriptBin "brrr-py-test-integration" ''
        set -euo pipefail
        ${self.packages.${pkgs.system}.brrr-venv-test}/bin/pytest ${self.packages.${pkgs.system}.brrr.src}
      ''
    );
  };
  brrr-ts-test-integration = mkTest {
    name = "brrr-ts-test-integration";
    bin = "${self.packages.${pkgs.system}.brrr-ts}/bin/brrr-test-integration";
  };
}
