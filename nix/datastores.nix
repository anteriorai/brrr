{ dynamodb, ... }:
{
  imports = [ dynamodb ];
  services.redis.servers.main = {
    enable = true;
    port = 6379;
    openFirewall = true;
    bind = null;
    logLevel = "debug";
    settings.protected-mode = "no";
  };
  services.dynamodb = {
    enable = true;
    openFirewall = true;
  };
}
