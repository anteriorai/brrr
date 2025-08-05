{
  package-lock2nix,
  callPackage,
  nodejs,
}:

(callPackage package-lock2nix.lib.package-lock2nix {
  # Overriding the Node.js version to support running TS natively.
  inherit nodejs;
}).mkNpmModule
  { src = ./.; }
