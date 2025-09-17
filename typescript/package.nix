{ package-lock2nix, nodejs }:

package-lock2nix.mkNpmModule {
  buildInputs = [ nodejs ];
  src = ./.;
}
