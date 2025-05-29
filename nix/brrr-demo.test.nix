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

# Inspired by
# https://blakesmith.me/2024/03/02/running-nixos-tests-with-flakes.html

{ self, pkgs }:

# Distributed test across multiple VMs, so there’s still some room for bugs to
# creep into the actual demo.  Both are nice to have so we should probably add a
# test that replicates the actual demo as closely as possible to catch any
# errors there.
pkgs.testers.runNixOSTest {
  name = "brrr-demo";

  nodes.datastores =
    { config, pkgs, ... }:
    {
      imports = [
        # Not going to export and dogfood this--it’s just local only
        ./dynamodb.module.nix
      ];
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
  nodes.server =
    { config, pkgs, ... }:
    {
      imports = [ self.nixosModules.brrr-demo ];
      networking.firewall.allowedTCPPorts = [ 8080 ];
      services.brrr-demo = {
        enable = true;
        package = self.packages.${pkgs.system}.brrr-demo;
        args = [ "server" ];
        environment = {
          BRRR_DEMO_LISTEN_HOST = "0.0.0.0";
          BRRR_DEMO_REDIS_URL = "redis://datastores:6379";
          AWS_DEFAULT_REGION = "foo";
          AWS_ENDPOINT_URL = "http://datastores:8000";
          AWS_ACCESS_KEY_ID = "foo";
          AWS_SECRET_ACCESS_KEY = "bar";
        };
      };
    };
  nodes.worker =
    { config, pkgs, ... }:
    {
      imports = [ self.nixosModules.brrr-demo ];
      services.brrr-demo = {
        enable = true;
        package = self.packages.${pkgs.system}.brrr-demo;
        args = [ "worker" ];
        environment = {
          BRRR_DEMO_REDIS_URL = "redis://datastores:6379";
          AWS_DEFAULT_REGION = "foo";
          AWS_ENDPOINT_URL = "http://datastores:8000";
          AWS_ACCESS_KEY_ID = "foo";
          AWS_SECRET_ACCESS_KEY = "bar";
        };
      };
    };
  # Separate node entirely just for the actual testing
  nodes.tester =
    { config, pkgs, ... }:
    let
      test-script = pkgs.writeShellApplication {
        name = "test-brrr-demo";
        # 😂
        text = ''
          eval "$(curl --fail -sSL "http://server:8080/hello?greetee=Jim" | jq '. == {status: "ok", result: "Hello, Jim!"}')"
          eval "$(curl --fail -sSL "http://server:8080/fib_and_print?n=6&salt=abcd" | jq '. == {status: "ok", result: 354224848179261915075}')"
        '';
      };
    in
    {
      environment.systemPackages =
        [ test-script ]
        ++ (with pkgs; [
          curl
          jq
        ]);
    };

  globalTimeout = 5 * 60;

  # Chose a big number (100) to ensure debouncing works.
  testScript = ''
    # Start first because it's a dependency
    datastores.wait_for_unit("default.target")
    # Server initializes the stores
    server.wait_for_unit("default.target")
    worker.wait_for_unit("default.target")
    tester.wait_for_unit("default.target")
    server.wait_for_open_port(8080)
    tester.wait_until_succeeds("curl --fail -sSL -X POST 'http://server:8080/hello?greetee=Jim'")
    tester.wait_until_succeeds("curl --fail -sSL -X POST 'http://server:8080/fib_and_print?n=100&salt=abcd'")
    tester.wait_until_succeeds("test-brrr-demo")
  '';
}
