{
  package-lock2nix,
  callPackage,
  nodejs,
}:

{
  brrr-ts =
    (callPackage package-lock2nix.lib.package-lock2nix {
      # Overriding the node.js version to support running TS natively.
      inherit nodejs;
    }).mkNpmModule
      { src = ./.; };
}
