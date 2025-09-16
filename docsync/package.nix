{ gnused, package-lock2nix }:

package-lock2nix.mkNpmModule {
  src = ./.;
  nativeBuildInputs = [ gnused ];
  postPatch = ''
    sed -i -e 's!\(PYTHON_DIR = \).*!\1"${../python}";!' -e 's!\(TYPESCRIPT_DIR = \).*!\1"${../typescript}";!' src/index.ts
  '';
}
