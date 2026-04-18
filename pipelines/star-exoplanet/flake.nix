{
    description = "Golang jobs builder";

    inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    outputs = { self, nixpkgs }:
    let
        system = "x86_64-linux";
        pkgs = nixpkgs.legacyPackages.${system};

        goJobs = pkgs.buildGoModule {
            pname = "star-exoplanet";
            version = "0.1.0";
            src = ./.;
            vendorHash = "sha256-UwEBoYp9kjTiBS6v1Z+YlmOFt70TWCkj/SsYoc6b/0U=";

            postInstall = ''
                mv $out/bin/pipelines $out/bin/star-exoplanet
            '';
        };

        mkImage = name: binName: pkgs.dockerTools.buildLayeredImage {
            name = "haumea-science/${name}";
            tag = "latest";
            contents = [ pkgs.cacert goJobs ];
            config = {
                Entrypoint = [ "${goJobs}/bin/${name}" ];
                User = "1000:1000";
            };
        };

    in {
        packages.${system} = {
            default = goJobs;
            job = mkImage "star-exoplanet" "star-exoplanet";
        };
    };
}

