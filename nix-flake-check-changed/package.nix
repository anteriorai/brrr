{
  writeShellApplication,
  findutils,
  gnused,
}:
writeShellApplication {
  name = "nix-flake-check-changed";
  runtimeInputs = [
    findutils
    gnused
  ];
  text = builtins.readFile ./nix-flake-check-changed.sh;
}
