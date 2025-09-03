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
      {
        lib,
        config,
        pkgs,
        ...
      }:
      let
        brrr-test-integration = self.packages.${pkgs.system}.brrr-ts.overrideAttrs {
          meta.mainProgram = "brrr-test-integration";
        };
      in
      {
        environment.systemPackages = [ brrr-test-integration ];
      };
  };

  testScript =
    integrationCommon.testScript
    + ''
      tester.wait_until_succeeds("brrr-test-integration")
    '';
}
