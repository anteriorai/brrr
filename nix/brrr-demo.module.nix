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

# A NixOS module for a brrr demo binary.  N.B.: It does not contain any
# dependencies; this is just the demo script.
#
# Inspired by
# https://blakesmith.me/2024/03/02/running-nixos-tests-with-flakes.html

{
  lib,
  config,
  pkgs,
  ...
}:
{
  options.services.brrr-demo =
    let
      native = import ./brrr-demo.options.nix { inherit lib pkgs; };
      mod1 = {
        options = native;
      };
      mod2 = {
        options.enable = lib.mkEnableOption "brrr-demo";
      };
    in
    lib.mkOption {
      description = "Brrr demo service configuration";
      type = lib.types.submoduleWith {
        modules = [
          mod1
          mod2
        ];
      };
      default = { };
    };

  config =
    let
      cfg = config.services.brrr-demo;
    in
    lib.mkIf cfg.enable {

      systemd.services.brrr-demo = {
        inherit (cfg) environment;
        after = [ "network.target" ];
        wantedBy = [ "multi-user.target" ];
        script = ''
          exec ${lib.getExe cfg.package} ${lib.escapeShellArgs cfg.args}
        '';
        serviceConfig = {
          Type = "simple";
        };
      };
    };
}
