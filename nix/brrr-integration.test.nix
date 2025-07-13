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

{
  self,
  pkgs,
  dynamodb-module,
}:

pkgs.testers.runNixOSTest {
  name = "brrr-integration";

  nodes.datastores =
    { config, pkgs, ... }:
    {
      imports = [ dynamodb-module ];
      services.redis.servers.main = {
        enable = true;
        port = 6379;
        openFirewall = true;
        bind = null;
        logLevel = "notice";
        settings.protected-mode = "no";
      };
      services.dynamodb = {
        enable = true;
        openFirewall = true;
      };
    };
  nodes.tester =
    {
      lib,
      config,
      pkgs,
      ...
    }:
    let
      test-brrr = pkgs.writeShellApplication {
        name = "test-brrr";
        runtimeInputs = [ self.packages.${pkgs.system}.brrr-venv-test ];
        runtimeEnv = {
          AWS_DEFAULT_REGION = "us-east-1";
          AWS_ENDPOINT_URL = "http://datastores:8000";
          AWS_ACCESS_KEY_ID = "fake";
          AWS_SECRET_ACCESS_KEY = "fake";
          BRRR_TEST_REDIS_URL = "redis://datastores:6379";
        };
        text = ''
          pytest ${self.packages.${pkgs.system}.brrr.src}
        '';
      };
    in
    {
      environment.systemPackages = [ test-brrr ];
    };

  globalTimeout = 5 * 60;

  testScript = ''
    datastores.wait_for_unit("default.target")
    tester.wait_for_unit("default.target")
    tester.wait_until_succeeds("test-brrr")
  '';
}
