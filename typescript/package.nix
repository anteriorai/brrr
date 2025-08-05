{ package-lock2nix }:

let
  brrr = package-lock2nix.mkNpmWorkspace {
    name = "brrr-ts";
    root = "./.";
  };
in
{
  inherit brrr;
}
