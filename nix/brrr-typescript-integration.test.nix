{
  self,
  pkgs,
  common,
}:

pkgs.testers.runNixOSTest {
  name = "brrr-typescript-integration";
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
        name = "test-brrr-typescript";
        runtimeInputs = [ self.packages.${pkgs.system}.brrr-ts ];
        runtimeEnv = common.runtimeEnv;
        text = ''
          cd ${self.packages.${pkgs.system}.brrr-ts.src}
          npm run test:integration
        '';
      };
    in
    {
      environment.systemPackages = [ test-brrr ];
    };

  testScript =
    common.testScript
    + ''
      tester.wait_until_succeeds("test-brrr-typescript")
    '';
}
