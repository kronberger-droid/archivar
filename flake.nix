{
  description = "archivar – agent-mediated knowledge store";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    # Toolchain on its own input so `nix flake update rust-overlay` gets the
    # newest stable without moving the nixpkgs pin. Same pattern as takt.
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = {
    nixpkgs,
    rust-overlay,
    ...
  }: let
    forAllSystems = nixpkgs.lib.genAttrs ["x86_64-linux" "aarch64-linux"];
    pkgsFor = system:
      import nixpkgs {
        inherit system;
        overlays = [rust-overlay.overlays.default];
      };
    toolchainFor = pkgs:
      pkgs.rust-bin.stable.latest.default.override {
        extensions = ["rust-analyzer" "rust-src"];
      };
  in {
    devShells = forAllSystems (system: let
      pkgs = pkgsFor system;
      # A throwaway cluster inside the repo: socket only, no TCP, owned by
      # whoever runs the shell. `pg-up` creates and starts it, `pg-down` stops
      # it, `rm -rf .pg` forgets everything.
      pg-up = pkgs.writeShellScriptBin "pg-up" ''
        set -eu
        if [ ! -d "$PGDATA" ]; then
          initdb -D "$PGDATA" --auth=trust --no-locale -E UTF8 >/dev/null
        fi
        if ! pg_ctl -D "$PGDATA" status >/dev/null 2>&1; then
          pg_ctl -D "$PGDATA" -l "$PGHOST/log" -w \
            -o "-k $PGHOST -c listen_addresses='''" start >/dev/null
        fi
        createdb archivar 2>/dev/null || true
        echo "postgres up, socket in $PGHOST"
      '';
      pg-down = pkgs.writeShellScriptBin "pg-down" ''
        pg_ctl -D "$PGDATA" stop -m fast
      '';
    in {
      default = pkgs.mkShell {
        nativeBuildInputs = [
          (toolchainFor pkgs)
          pkgs.postgresql_17
          pg-up
          pg-down
        ];
        shellHook = ''
          export PGHOST="$PWD/.pg"
          export PGDATA="$PWD/.pg/data"
          export PGDATABASE=archivar
          # sqlx does not fall back to $USER for the login, so name it.
          export DATABASE_URL="postgres:///archivar?host=$PWD/.pg&user=$USER"
          mkdir -p "$PGHOST"
        '';
      };
    });
  };
}
