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

let
  common = import ./brrr-integration-common.nix { inherit dynamodb-module; };
in

pkgs.testers.runNixOSTest {
  name = "brrr-integration";
  globalTimeout = common.globalTimeout;

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
        runtimeEnv = common.runtimeEnv;
        text = ''
          pytest ${self.packages.${pkgs.system}.brrr.src}
        '';
      };
    in
    {
      environment.systemPackages = [ test-brrr ];
    };

  testScript =
    common.testScript
    + ''
      tester.wait_until_succeeds("test-brrr-python")
    '';
}
