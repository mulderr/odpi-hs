{ nixpkgs ? import ./nix/nixpkgs.nix {}, compiler ? "ghc921" }:

let
  inherit (nixpkgs) pkgs;

  hlib = pkgs.haskell.lib;
  hpkgs0 = pkgs.haskell.packages.${compiler};

  sources = {
    cabal-doctest = nixpkgs.fetchFromGitHub {
      owner = "haskellari";
      repo = "cabal-doctest";
    };
  };

  hpkgs = hpkgs0.override {
    overrides = self: super: {
      odpi-libdpi = self.callCabal2nix "odpi-libdpi" ./odpi-libdpi {};
      odpi-simple = self.callCabal2nix "odpi-simple" ./odpi-simple { inherit (self) odpi-libdpi; };
      odpi-streaming = self.callCabal2nix "odpi-streaming" ./odpi-streaming { inherit (self) odpi-simple; };

      cabal-doctest = self.callCabal2nix "cabal-doctest" "${sources.cabal-doctest}" {};
    };
  };

in
  hpkgs.shellFor {
    packages = ps: with hpkgs; [
      odpi-simple
      odpi-streaming
    ];
    buildInputs = with pkgs; [
      odpic
      cabal-install
      hlint
      #hpkgs0.haskell-language-server
    ];
  }
