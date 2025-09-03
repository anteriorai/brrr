{
  package-lock2nix,
  callPackage,
  nodejs,
}:

let
  brrr-ts =
    (callPackage package-lock2nix.lib.package-lock2nix {
      # Overriding the Node.js version to support running TS natively.
      inherit nodejs;
    }).mkNpmModule
      {
        src = ./.;
        buildInputs = [ nodejs ];
        postFixup = ''
          substituteInPlace "$out/brrr-test-integration" --subst-var-by brrrTsDir $out
        '';
      };
in
{
  inherit brrr-ts;
}
