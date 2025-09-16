{
  gnused,
  lib,
  package-lock2nix,
  runCommand,
}:

let
  final = package-lock2nix.mkNpmModule {
    src = ./.;
    nativeBuildInputs = [ gnused ];
    postPatch = ''
      sed -i -e 's!\(PYTHON_DIR = \).*!\1"${../python}";!' -e 's!\(TYPESCRIPT_DIR = \).*!\1"${../typescript}";!' src/index.ts
    '';
    passthru.tests.docsync = runCommand "docsync" { } ''
      ${lib.getExe final}
      touch $out
    '';
  };
in
final
