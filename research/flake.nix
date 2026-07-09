{
  description = "Entorno de desarrollo aislado para compilar duckdb-ffi";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  };

  outputs = { self, nixpkgs }: 
  let
    system = "x86_64-linux";
    pkgs = nixpkgs.legacyPackages.${system};
  in {
    devShells.${system}.default = pkgs.mkShell {
      buildInputs = [
        pkgs.duckdb
        pkgs.pkg-config
      ];

      # Forzamos al toolchain de C subyacente de GHC a leer desde el /nix/store
      shellHook = ''
        export C_INCLUDE_PATH="${pkgs.duckdb}/include:$C_INCLUDE_PATH"
        export CPLUS_INCLUDE_PATH="${pkgs.duckdb}/include:$CPLUS_INCLUDE_PATH"
        export LIBRARY_PATH="${pkgs.duckdb}/lib:$LIBRARY_PATH"
        
        # Necesario en tiempo de ejecución (Template Haskell o pruebas)
        export LD_LIBRARY_PATH="${pkgs.duckdb}/lib:$LD_LIBRARY_PATH"
        
        echo "Entorno Nix cargado. DuckDB inyectado desde: ${pkgs.duckdb}"
      '';
    };
  };
}
