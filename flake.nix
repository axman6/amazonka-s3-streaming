{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    haskell-flake.url = "github:srid/haskell-flake";
    pre-commit-hooks.url = "github:cachix/pre-commit-hooks.nix";
  };

  outputs = inputs@{ self, nixpkgs, flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = nixpkgs.lib.systems.flakeExposed;
      imports = [
        inputs.haskell-flake.flakeModule
        inputs.pre-commit-hooks.flakeModule
      ];

      perSystem = { self', pkgs, config, ... }: {
        haskellProjects.default = {
          # The base package set representing a specific GHC version.
          # By default, this is pkgs.haskellPackages.
          # See https://zero-to-flakes.com/haskell-flake/package-set
          # basePackages = pkgs.haskellPackages;

          # Extra package information. See https://zero-to-flakes.com/haskell-flake/dependency
          #
          # Note that local packages are automatically included in `packages`
          # (defined by `defaults.packages` option).
          #
          # packages = {
          #   aeson.source = "1.5.0.0"; # Hackage version override
          #   shower.source = inputs.shower;
          # };
          # settings = {
          #   aeson = {
          #     check = false;
          #   };
          #   relude = {
          #     haddock = false;
          #     broken = false;
          #   };
          # };

          devShell = {
            enable = true;

            # Programs you want to make available in the shell.
            # Default programs can be disabled by setting to 'null'
            # tools = hp: { fourmolu = hp.fourmolu; ghcid = null; };

            hlsCheck.enable = true;
            mkShellArgs.shellHook = config.pre-commit.installationScript;
          };
        };

        pre-commit.settings.hooks = {
          cabal-fmt.enable = true;
          hlint.enable = true;
          nixpkgs-fmt.enable = true;
        };

        # haskell-flake doesn't set the default package, but you can do it here.
        packages.default = self'.packages.amazonka-s3-streaming;
      };
    };
}
