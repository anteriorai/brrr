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

{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    systems.url = "systems";
    flake-parts.url = "github:hercules-ci/flake-parts";
    devshell.url = "github:numtide/devshell";
    services-flake.url = "github:juspay/services-flake";
    process-compose-flake.url = "github:Platonic-Systems/process-compose-flake";
    # Heavily inspired by
    # https://pyproject-nix.github.io/uv2nix/usage/hello-world.html
    pyproject-nix = {
      url = "github:pyproject-nix/pyproject.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    uv2nix = {
      url = "github:pyproject-nix/uv2nix";
      inputs.pyproject-nix.follows = "pyproject-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    pyproject-build-systems = {
      url = "github:pyproject-nix/build-system-pkgs";
      inputs.pyproject-nix.follows = "pyproject-nix";
      inputs.uv2nix.follows = "uv2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    package-lock2nix = {
      url = "github:anteriorai/package-lock2nix";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.flake-parts.follows = "flake-parts";
      inputs.treefmt-nix.follows = "treefmt-nix";
    };
    treefmt-nix.url = "github:numtide/treefmt-nix";
  };

  outputs =
    { self, flake-parts, ... }@inputs:
    let
      checkBuildAll = import ./nix/check-build-all.nix;
      # flake.parts module for linux systems
      brrrLinux = {
        perSystem =
          {
            config,
            lib,
            pkgs,
            self',
            ...
          }:
          lib.mkIf pkgs.stdenv.isLinux {
            packages.docker = pkgs.dockerTools.buildLayeredImage {
              name = "brrr-demo";
              tag = "latest";
              config.Entrypoint = [ "${lib.getExe self'.packages.brrr-demo}" ];
            };
          };
      };
      # flake.parts module for any system
      brrrAllSystems = {
        flake = {
          # Expose for reuse.  Name and availability subject to change.
          flakeModules = { inherit checkBuildAll; };
          # A reusable process-compose module (for flake-parts) with either a full

          # demo environment, or just the dependencies if you want to run a server
          # manually.
          processComposeModules = {
            brrr-demo = inputs.services-flake.lib.multiService ./nix/brrr-demo.service.nix;
            dynamodb = import ./nix/dynamodb.service.nix;
            localstack = import ./nix/localstack.service.nix;
            default =
              { pkgs, ... }:
              {
                imports = with self.processComposeModules; [
                  brrr-demo
                  dynamodb
                  # Unused for now but will probably be reintroduced for an SQS demo
                  # soon.
                  localstack
                ];
                services =
                  let
                    demoEnv = {
                      AWS_DEFAULT_REGION = "us-east-1";
                      AWS_ENDPOINT_URL = "http://localhost:8000";
                      AWS_ACCESS_KEY_ID = "000000000000";
                      AWS_SECRET_ACCESS_KEY = "fake";
                    };
                  in
                  {
                    redis.r1.enable = true;
                    dynamodb = {
                      enable = true;
                      args = [ "-disableTelemetry" ];
                    };
                    brrr-demo.worker = {
                      package = self.packages.${pkgs.system}.brrr-demo;
                      args = [ "brrr_worker" ];
                      environment = demoEnv;
                    };
                    brrr-demo.server = {
                      package = self.packages.${pkgs.system}.brrr-demo;
                      args = [ "web_server" ];
                      environment = demoEnv;
                    };
                  };
              };
          };
          # WIP, exporting is best effort.
          nixosModules = {
            brrr-demo = import ./nix/brrr-demo.module.nix;
            dynamodb = import ./nix/dynamodb.module.nix;
          };
        };
        perSystem =
          {
            config,
            self',
            inputs',
            pkgs,
            lib,
            system,
            ...
          }:
          let
            python = pkgs.python313;
            nodejs = pkgs.nodejs_24;
            devPackages = [
              pkgs.process-compose
              pkgs.redis # For the CLI
              self'.packages.uv
              nodejs
            ];
            brrrpy = pkgs.callPackage ./python/package.nix {
              inherit (inputs) pyproject-build-systems pyproject-nix uv2nix;
              inherit python;
            };
            brrrts = pkgs.callPackage ./typescript/package.nix {
              inherit (inputs) package-lock2nix;
              inherit nodejs;
            };
          in
          {
            config = {
              _module.args.pkgs = import inputs.nixpkgs {
                inherit system;
                # dynamodb
                config.allowUnfree = true;
              };
              process-compose.demo = {
                imports = [
                  inputs.services-flake.processComposeModules.default
                  self.processComposeModules.default
                ];
                cli.options.no-server = true;
                services.brrr-demo.server.enable = true;
                services.brrr-demo.worker.enable = true;
              };
              process-compose.deps = {
                imports = [
                  inputs.services-flake.processComposeModules.default
                  self.processComposeModules.default
                ];
                cli.options.no-server = true;
                services.brrr-demo.server.enable = false;
                services.brrr-demo.worker.enable = false;
              };
              treefmt = import ./nix/treefmt.nix;
              packages = {
                inherit python;
                inherit (pkgs) uv;
                inherit (brrrpy) brrr brrr-venv-test;
                inherit (brrrts) brrr-ts;
                default = brrrpy.brrr-venv;
                # Stand-alone brrr_demo.py script
                brrr-demo = pkgs.stdenvNoCC.mkDerivation {
                  name = "brrr-demo.py";
                  dontUnpack = true;
                  installPhase = ''
                    mkdir -p $out/bin
                    cp ${./brrr_demo.py} $out/bin/brrr_demo.py
                  '';
                  buildInputs = [ brrrpy.brrr-venv-test ];
                  # The patch phase will automatically use the python from the venv as
                  # the interpreter for the demo script.
                  meta.mainProgram = "brrr_demo.py";
                };
                # Best-effort package for convenience, zero guarantees, could
                # disappear at any time.
                nix-flake-check-changed = pkgs.callPackage ./nix-flake-check-changed/package.nix { };
              };
              checks =
                {
                  pytestIntegration = pkgs.callPackage ./nix/brrr-integration.test.nix {
                    inherit self;
                    dynamodb-module = self.nixosModules.dynamodb;
                  };
                  typescriptIntegration = pkgs.callPackage ./nix/brrr-typescript-integration.test.nix {
                    inherit self;
                    dynamodb-module = self.nixosModules.dynamodb;
                  };
                }
                // brrrpy.brrr.tests
                // import ./nix/brrr-demo.test.nix {
                  inherit self pkgs;
                  dynamodb-module = self.nixosModules.dynamodb;
                };
              devshells =
                let
                  sharedCommands = [
                    {
                      name = "brrr-demo-deps";
                      category = "demo";
                      help = "Start all dependent services without any brrr workers / server";
                      command = ''
                        nix run .#deps
                      '';
                    }
                  ];
                in
                {
                  default = {
                    packages = devPackages ++ [ self'.packages.python ];
                    motd =
                      ''
                        This is the generic devshell for brrr development.  Use this to fix
                        problems in the Python lockfile and to access generic tooling.

                        Available tools:
                      ''
                      + lib.concatLines (map (x: "  - ${x.pname or x.name}") devPackages)
                      + ''

                        For Python-specific development, use: nix develop .#python
                        For TypeScript-specific development, use: nix develop .#typescript
                      '';
                    env = [
                      {
                        name = "PYTHONPATH";
                        unset = true;
                      }
                      {
                        name = "UV_PYTHON_DOWNLOADS";
                        value = "never";
                      }
                    ];
                  };
                  python = {
                    env = [
                      {
                        name = "REPO_ROOT";
                        eval = "$(git rev-parse --show-toplevel)";
                      }
                      {
                        name = "PYTHONPATH";
                        unset = true;
                      }
                      {
                        name = "UV_PYTHON_DOWNLOADS";
                        value = "never";
                      }
                      {
                        name = "UV_NO_SYNC";
                        value = "1";
                      }
                    ];
                    packages = devPackages ++ [ brrrpy.brrr-venv-editable ];
                    commands = [
                      {
                        name = "brrr-test-unit";
                        category = "test";
                        help = "Tests which don't need dependencies";
                        command = ''
                          pytest -m 'not dependencies' "$@"
                        '';
                      }
                      {
                        name = "brrr-test-all";
                        category = "test";
                        help = "Tests including dependencies, make sure to run brrr-demo-deps";
                        # Lol
                        command = ''
                          (
                            : "''${AWS_DEFAULT_REGION=fake}"
                            export AWS_DEFAULT_REGION
                            : "''${AWS_ENDPOINT_URL=http://localhost:8000}"
                            export AWS_ENDPOINT_URL
                            : "''${AWS_ACCESS_KEY_ID=fake}"
                            export AWS_ACCESS_KEY_ID
                            : "''${AWS_SECRET_ACCESS_KEY=fake}"
                            export AWS_SECRET_ACCESS_KEY
                            : "''${BRRR_TEST_REDIS_URL=redis://localhost:6379}"
                            export BRRR_TEST_REDIS_URL
                            exec pytest "$@"
                          )'';
                      }
                      # Always build aarch64-linux
                      {
                        name = "brrr-build-docker";
                        category = "build";
                        help = "Build and load a Docker image (requires a Nix Linux builder)";
                        command =
                          let
                            drv = self'.packages.docker;
                          in
                          ''
                            (
                              set -o pipefail
                              if nix build --no-link --print-out-paths .#packages.aarch64-linux.docker | xargs -r docker load -i; then
                                echo 'Start a new worker with `docker run <image name>`'
                              fi
                            )
                          '';
                      }
                      {
                        name = "brrr-demo-full";
                        category = "demo";
                        help = "Launch a full demo locally";
                        command = ''
                          nix run .#demo
                        '';
                      }
                    ] ++ sharedCommands;
                  };
                  typescript = {
                    packages = devPackages;
                    commands = [
                      {
                        name = "brrr-test-unit";
                        category = "test";
                        help = "Tests which don't need dependencies";
                        command = ''
                          npm run test
                        '';
                      }
                      {
                        name = "brrr-test-all";
                        category = "test";
                        help = "Tests including dependencies, make sure to run brrr-demo-deps";
                        command = ''
                          (
                            : "''${AWS_DEFAULT_REGION=fake}"
                            export AWS_DEFAULT_REGION
                            : "''${AWS_ENDPOINT_URL=http://localhost:8000}"
                            export AWS_ENDPOINT_URL
                            : "''${AWS_ACCESS_KEY_ID=fake}"
                            export AWS_ACCESS_KEY_ID
                            : "''${AWS_SECRET_ACCESS_KEY=fake}"
                            export AWS_SECRET_ACCESS_KEY
                            : "''${BRRR_TEST_REDIS_URL=redis://localhost:6379}"
                            export BRRR_TEST_REDIS_URL
                            exec npm run test:integration
                          )'';
                      }
                    ] ++ sharedCommands;
                  };
                };
            };
          };
      };
    in
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = import inputs.systems;
      imports = [
        inputs.process-compose-flake.flakeModule
        inputs.devshell.flakeModule
        inputs.treefmt-nix.flakeModule
        brrrLinux
        brrrAllSystems
        checkBuildAll
      ];
    };
}
