{ ... }:
{
  projectRootFile = "flake.nix";
  programs.ruff-format.enable = true;
  programs.ruff-check.enable = true;
  programs.nixfmt = {
    enable = true;
    strict = true;
  };
}
