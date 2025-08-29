{
  self,
  pkgs,
  integrationCommon,
}:

pkgs.testers.runNixOSTest {
  name = "brrr-typescript-integration";
  globalTimeout = integrationCommon.globalTimeout;

  nodes = {
    inherit (integrationCommon.nodes) datastores;
    tester =
      {
        lib,
        config,
        pkgs,
        ...
      }:
      let
        test-brrr-typescript = pkgs.writeShellApplication {
          name = "test-brrr-typescript";
          runtimeInputs = [
            self.packages.${pkgs.system}.brrr-ts
            pkgs.nodejs_24
          ];
          runtimeEnv = integrationCommon.runtimeEnv;
          text = ''
            cd ${self.packages.${pkgs.system}.brrr-ts}
            NODE_PRESERVE_SYMLINKS=1 npm run test:integration
          '';
        };
      in
      {
        environment.systemPackages = [ test-brrr-typescript ];
      };
  };

  testScript =
    integrationCommon.testScript
    + ''
      tester.wait_until_succeeds("test-brrr-typescript")
    '';
}
