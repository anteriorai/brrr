{
  self,
  pkgs,
  integrationCommon,
}:

pkgs.testers.runNixOSTest {
  inherit (integrationCommon) globalTimeout;
  name = "brrr-typescript-integration";
  nodes = {
    inherit (integrationCommon) datastores;
    tester =
      { pkgs, ... }:
      let
        brrr-test-integration = self.packages.${pkgs.system}.brrr-ts;
      in
      {
        systemd.services.brrr-test-integration = {
          serviceConfig = {
            Type = "oneshot";
            Restart = "no";
            RemainAfterExit = "yes";
            ExecStart = "${brrr-test-integration}/bin/brrr-test-integration";
          };
          environment = integrationCommon.runtimeEnv;
          enable = true;
          wants = [ "multi-user.target" ];
        };
      };
  };

  testScript =
    integrationCommon.testScript
    + ''
      tester.systemctl("start --no-block brrr-test-integration.service")
      tester.wait_for_unit("brrr-test-integration.service")
    '';
}
