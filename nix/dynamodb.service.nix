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

# Dynamodb module for process-compose-flake

{
  config,
  pkgs,
  lib,
  ...
}:
{
  options.services.dynamodb = with lib.types; {
    enable = lib.mkEnableOption "Enable Dynamodb local service";
    args = lib.mkOption {
      default = [ ];
      type = listOf str;
    };
    dataDir = lib.mkOption {
      default = "data/dynamodb";
      type = str;
    };
  };
  config =
    let
      cfg = config.services.dynamodb;
    in
    lib.mkIf cfg.enable {
      settings.processes.dynamodb.command =
        let
          bin = lib.getExe pkgs.dynamodb-local;
          dir = lib.escapeShellArg cfg.dataDir;
        in
        ''
          mkdir -p ${dir}
          ${bin} -dbPath ${dir} ${lib.escapeShellArgs cfg.args}
        '';
    };
}
