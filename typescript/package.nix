{
  package-lock2nix,
  git,
  moreutils,
  jq,
  nodejs,
  writeShellApplication,
}:

package-lock2nix.mkNpmModule {
  buildInputs = [ nodejs ];
  src = ./.;
  passthru.npm-version-to-git = writeShellApplication {
    name = "npm-version-to-git";
    runtimeInputs = [
      git
      moreutils
      jq
    ];
    text = builtins.readFile ./scripts/npm-version-to-git.sh;
  };
}
