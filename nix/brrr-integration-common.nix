{ dynamodb-module }:

{
  nodes.datastores =
    { config, pkgs, ... }:
    {
      imports = [ dynamodb-module ];
      services.redis.servers.main = {
        enable = true;
        port = 6379;
        openFirewall = true;
        bind = null;
        logLevel = "notice";
        settings.protected-mode = "no";
      };
      services.dynamodb = {
        enable = true;
        openFirewall = true;
      };
    };

  runtimeEnv = {
    AWS_DEFAULT_REGION = "us-east-1";
    AWS_ENDPOINT_URL = "http://datastores:8000";
    AWS_ACCESS_KEY_ID = "fake";
    AWS_SECRET_ACCESS_KEY = "fake";
    BRRR_TEST_REDIS_URL = "redis://datastores:6379";
  };

  testScript = ''
    datastores.wait_for_unit("default.target")
    tester.wait_for_unit("default.target")
  '';

  globalTimeout = 5 * 60;
}
