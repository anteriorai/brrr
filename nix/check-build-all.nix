# flake.parts module which adds build-only entries for all your packages and
# devshells to the flake’s checks.  Use this for a more aggressive and
# comprehensive ‘nix flake check.’  By default, nix flake check doesn’t build
# any derivations unless specified as an input to an entry in checks.  With this
# module, a nix flake check automatically ensures every package output on this
# architecture builds successfully, as well as every devShell.
{
  perSystem =
    { lib, self', ... }:
    {
      checks =
        lib.mapAttrs' (name: value: {
          name = "build-${name}";
          inherit value;
        }) self'.packages
        // lib.mapAttrs' (name: value: {
          name = "devshell-${name}";
          inherit value;
        }) self'.devShells;
    };
}
