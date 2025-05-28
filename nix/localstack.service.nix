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

# Localstack module for process-compose-flake

{
  config,
  pkgs,
  lib,
  ...
}:
{
  options.services.localstack = with lib.types; {
    enable = lib.mkEnableOption "Enable localstack service";
    package = lib.mkPackageOption pkgs "localstack" { };
    args = lib.mkOption {
      default = [ ];
      type = listOf str;
    };
  };
  config =
    let
      cfg = config.services.localstack;
    in
    lib.mkIf cfg.enable {
      settings.processes.localstack =
        let
          localstack = lib.getExe cfg.package;
        in
        {
          command = ''
            (
              trap "${localstack} stop" EXIT
              ${localstack} start ${lib.escapeShellArgs cfg.args}
            )
          '';
        };
    };
}
