# Copyright © Brrr Authors
#
# Licensed under AGPLv3-only. See README for year and details.

# Dynamodb module for NixOS.  No frills just local ephemeral host.

{
  lib,
  pkgs,
  config,
  ...
}:
{
  options.services.dynamodb = {
    enable = lib.mkEnableOption "dynamodb";
    openFirewall = lib.mkOption {
      type = lib.types.bool;
      default = false;
    };
  };
  config =
    let
      cfg = config.services.dynamodb;
    in
    lib.mkIf cfg.enable {
      systemd.services.dynamodb = {
        after = [ "network.target" ];
        wantedBy = [ "multi-user.target" ];
        script = ''
          exec ${lib.getExe pkgs.dynamodb-local} -dbPath /var/lib/dynamodb
        '';
        serviceConfig = {
          Type = "simple";
          User = "dynamodb";
          Group = "dynamodb";
        };
      };
      users.users.dynamodb = {
        group = "dynamodb";
        home = "/var/lib/dynamodb";
        useDefaultShell = true;
        isSystemUser = true;
        createHome = true;
      };
      users.groups.dynamodb = { };
      networking.firewall.allowedTCPPorts = lib.optionals cfg.openFirewall [ 8000 ];
    };
}
