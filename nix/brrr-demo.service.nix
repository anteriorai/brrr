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

# Brrr-demo module for services-flake.  Awkwardly named file because
# services-flake insists on auto-deriving the module name from the filename.
# Ok.

{
  config,
  pkgs,
  name,
  lib,
  ...
}:
{
  # services-flake requires setting the options top-level
  options = import ./brrr-demo.options.nix { inherit lib pkgs; };
  config = {
    outputs.settings.processes.${name} = {
      environment = config.environment;
      command = "${lib.getExe config.package} ${lib.escapeShellArgs config.args}";
    };
  };
}
