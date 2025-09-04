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

{
  self,
  pkgs,
  dynamodb-module,
}:

# Distributed test across multiple VMs, so there’s still some room for bugs to
# creep into the actual demo.  Both are nice to have so we should probably add a
# test that replicates the actual demo as closely as possible to catch any
# errors there.
let
  mkTest =
    { nodes, name }:
    pkgs.testers.runNixOSTest {
      inherit name;
      nodes = nodes // {
        datastores =
          { config, pkgs, ... }:
          {
            imports = [ dynamodb-module ];
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
        # Separate node entirely just for the actual testing
        tester =
          { config, pkgs, ... }:
          let
            test-script = pkgs.writeShellApplication {
              name = "test-brrr-demo";
              text = ''
                >/dev/stderr echo "Fetching results from previously scheduled tasks..."
                json="$(curl --fail -sSL "http://server:8080/hello?greetee=Jim")"
                val="$(<<<"$json" jq '. == {status: "ok", result: "Hello, Jim!"}')"
                [[ "$val" == true ]]
                json="$(curl --fail -sSL "http://server:8080/fib_and_print?n=78&salt=abcd")"
                val="$(<<<"$json" jq '. == {status: "ok", result: 8944394323791464}')"
                [[ "$val" == true ]]
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
      };

      globalTimeout = 10 * 60;

      # Chose a big number (78) to ensure debouncing works.
      # fib(78) = 8944394323791464 is the largest Fibonacci number under JavaScript's Number.MAX_SAFE_INTEGER
      testScript = ''
        # Start first because it's a dependency
        datastores.wait_for_unit("default.target")
        # Server initializes the stores
        server.wait_for_unit("default.target")
        worker.wait_for_unit("default.target")
        tsworker.wait_for_unit("default.target")
        tester.wait_for_unit("default.target")
        server.wait_for_open_port(8080)
        tester.wait_until_succeeds("curl --fail -sSL -X POST 'http://server:8080/hello?greetee=Jim'")
        tester.wait_until_succeeds("curl --fail -sSL -X POST 'http://server:8080/fib_and_print?n=78&salt=abcd'")
        tester.wait_until_succeeds("test-brrr-demo")
      '';
    };
  module = {
    server =
      { config, pkgs, ... }:
      {
        imports = [ self.nixosModules.brrr-demo ];
        networking.firewall.allowedTCPPorts = [ 8080 ];
        services.brrr-demo = {
          enable = true;
          package = self.packages.${pkgs.system}.brrr-demo;
          args = [ "web_server" ];
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
    worker =
      { config, pkgs, ... }:
      {
        imports = [ self.nixosModules.brrr-demo ];
        services.brrr-demo = {
          enable = true;
          package = self.packages.${pkgs.system}.brrr-demo;
          args = [ "brrr_worker" ];
          environment = {
            BRRR_DEMO_REDIS_URL = "redis://datastores:6379";
            AWS_DEFAULT_REGION = "foo";
            AWS_ENDPOINT_URL = "http://datastores:8000";
            AWS_ACCESS_KEY_ID = "foo";
            AWS_SECRET_ACCESS_KEY = "bar";
          };
        };
      };
    tsworker =
      { config, pkgs, ... }:
      {
        imports = [ self.nixosModules.brrr-demo ];
        services.brrr-demo = {
          enable = true;
          package = self.packages.${pkgs.system}.brrr-demo-ts;
          environment = {
            BRRR_DEMO_REDIS_URL = "redis://datastores:6379";
            AWS_REGION = "foo";
            AWS_ENDPOINT_URL = "http://datastores:8000";
            AWS_ACCESS_KEY_ID = "foo";
            AWS_SECRET_ACCESS_KEY = "bar";
          };
        };
      };
  };
  docker = {
    server =
      { config, pkgs, ... }:
      {
        # Podman (default backend) doesn’t like images built with nix
        # apparently.  Ironic!
        virtualisation.oci-containers.backend = "docker";
        virtualisation.oci-containers.containers.brrr = {
          extraOptions = [ "--network=host" ];
          image = "brrr-demo:latest";
          imageFile = self.packages.${pkgs.system}.docker;
          environment = {
            BRRR_DEMO_LISTEN_HOST = "0.0.0.0";
            BRRR_DEMO_REDIS_URL = "redis://datastores:6379";
            AWS_DEFAULT_REGION = "foo";
            AWS_ENDPOINT_URL = "http://datastores:8000";
            AWS_ACCESS_KEY_ID = "foo";
            AWS_SECRET_ACCESS_KEY = "bar";
          };
          cmd = [ "web_server" ];
        };
        networking.firewall.allowedTCPPorts = [ 8080 ];
      };
    worker =
      { config, pkgs, ... }:
      {
        virtualisation.oci-containers.backend = "docker";
        virtualisation.oci-containers.containers.brrr = {
          extraOptions = [ "--network=host" ];
          image = "brrr-demo:latest";
          imageFile = self.packages.${pkgs.system}.docker;
          cmd = [ "brrr_worker" ];
          environment = {
            BRRR_DEMO_REDIS_URL = "redis://datastores:6379";
            AWS_DEFAULT_REGION = "foo";
            AWS_ENDPOINT_URL = "http://datastores:8000";
            AWS_ACCESS_KEY_ID = "foo";
            AWS_SECRET_ACCESS_KEY = "bar";
          };
        };
      };
    tsworker =
      { config, pkgs, ... }:
      {
        virtualisation.oci-containers.backend = "docker";
        virtualisation.oci-containers.containers.brrr-ts = {
          extraOptions = [ "--network=host" ];
          image = "brrr-demo-ts:latest";
          imageFile = self.packages.${pkgs.system}.docker-ts;
          cmd = [ ];
          environment = {
            BRRR_DEMO_REDIS_URL = "redis://datastores:6379";
            AWS_REGION = "foo";
            AWS_ENDPOINT_URL = "http://datastores:8000";
            AWS_ACCESS_KEY_ID = "foo";
            AWS_SECRET_ACCESS_KEY = "bar";
          };
        };
      };
  };
in
{
  docker-test = mkTest {
    name = "brrr-demo-docker";
    nodes = docker;
  };
  nixos-module-test = mkTest {
    name = "brrr-demo-nixos";
    nodes = module;
  };
}
