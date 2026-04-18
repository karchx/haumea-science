k := kubectl
REGISTRY := 192.168.0.9:32000

.PHONY: deploy-job

deploy-job:
	@echo "Building OCI image for job with Nix..."
	$(eval TAR_PATH := $(shell nix build --no-link --print-out-paths .#job))
	@echo "Loading OCI image into kind cluster..."
	nix run nixpkgs#skopeo -- copy --dest-tls-verify=false docker-archive:$(TAR_PATH) docker://$(REGISTRY)/haumea-science/star-exoplanet:latest
	@echo "Deploying job to Kubernetes cluster..."
	$(k) apply -f job-producer.yaml
